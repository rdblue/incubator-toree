/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License
 */

package org.apache.toree.kernel.interpreter.scala

import java.io.ByteArrayOutputStream
import java.util.concurrent.ExecutionException

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.repl.Main
import org.apache.spark.sql.SparkSession

import org.apache.toree.interpreter._
import org.apache.toree.kernel.api.KernelLike
import org.apache.toree.utils.{MultiOutputStream, TaskManager}
import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IR, OutputStream}
import scala.tools.nsc.util.ClassPath

import org.apache.spark.sql.SQLContext

import org.apache.toree.kernel.protocol.v5.MIMEType
import org.apache.toree.magic.MagicOutput
import vegas.DSL.ExtendedUnitSpecBuilder
import vegas.render.StaticHTMLRenderer


class ScalaInterpreter(private val config:Config = ConfigFactory.load) extends Interpreter with ScalaInterpreterSpecific {
  import ScalaInterpreter._

   private var kernel: KernelLike = _

   protected val logger = LoggerFactory.getLogger(this.getClass.getName)

   protected val _thisClassloader = this.getClass.getClassLoader

   protected val lastResultOut = new ByteArrayOutputStream()

   protected val multiOutputStream: OutputStream = lastResultOut
   private[scala] var taskManager: TaskManager = _

  /** Since the ScalaInterpreter can be started without a kernel, we need to ensure that we can compile things.
      Adding in the default classpaths as needed.
    */
  def appendClassPath(settings: Settings): Settings = {
    settings.classpath.value = buildClasspath(_thisClassloader)
    settings.embeddedDefaults(_runtimeClassloader)
    settings
  }

  protected var settings: Settings = newSettings(List())
  settings = appendClassPath(settings)


  private val maxInterpreterThreads: Int = {
     if(config.hasPath("max_interpreter_threads"))
       config.getInt("max_interpreter_threads")
     else
       TaskManager.DefaultMaximumWorkers
   }

   protected def newTaskManager(): TaskManager =
     new TaskManager(maximumWorkers = maxInterpreterThreads)

  /**
    * This has to be called first to initialize all the settings.
    *
    * @return The newly initialized interpreter
    */
   override def init(kernel: KernelLike): Interpreter = {
     this.kernel = kernel
     val args = interpreterArgs(kernel)
     settings = newSettings(args)
     settings = appendClassPath(settings)

     start()
     bindKernelVariable(kernel)
 
     this
   }

  override def postInit(): Unit = {
    bindSparkSession()
    bindSparkContext()
    bindSqlContext()
    defineImplicits()
  }

   protected[scala] def buildClasspath(classLoader: ClassLoader): String = {

     def toClassLoaderList( classLoader: ClassLoader ): Seq[ClassLoader] = {
       @tailrec
       def toClassLoaderListHelper( aClassLoader: ClassLoader, theList: Seq[ClassLoader]):Seq[ClassLoader] = {
         if( aClassLoader == null )
           return theList

         toClassLoaderListHelper( aClassLoader.getParent, aClassLoader +: theList )
       }
       toClassLoaderListHelper(classLoader, Seq())
     }

     val urls = toClassLoaderList(classLoader).flatMap{
         case cl: java.net.URLClassLoader => cl.getURLs.toList
         case a => List()
     }

     urls.foldLeft("")((l, r) => ClassPath.join(l, r.toString))
   }

   protected def interpreterArgs(kernel: KernelLike): List[String] = {
     import scala.collection.JavaConverters._
     if (kernel == null || kernel.config == null) {
       List()
     }
     else {
       kernel.config.getStringList("interpreter_args").asScala.toList
     }
   }

   private lazy val displayers = new mutable.HashMap[Class[_], Displayer[_]]

   def registerDisplayer[T](objClass: Class[T], displayer: Displayer[T]): Unit = {
     displayers.put(objClass, displayer)
   }

   protected def getDisplayer[T](obj: T): Option[Displayer[_ >: T]] = {
     var objClass: Class[_ >: T] = obj.getClass.asInstanceOf[Class[_ >: T]]
     while (objClass != classOf[Object]) {
       displayers.get(objClass) match {
         case Some(displayer) =>
           return Some(displayer.asInstanceOf[Displayer[_ >: T]])
         case None =>
       }
       objClass = objClass.getSuperclass
     }
     Some(displayers(classOf[Object]).asInstanceOf[Displayer[_ >: T]])
   }

  registerDisplayer(classOf[MagicOutput], new Displayer[MagicOutput] {
    override def display(data: MagicOutput): Map[String, String] = data.asMap
  })

    registerDisplayer(classOf[SparkContext], new Displayer[SparkContext] {
     override def display(sc: SparkContext): Map[String, String] = {
       val appId = sc.applicationId
       val webProxy = sc.hadoopConfiguration.get("yarn.web-proxy.address")
       val masterHost = webProxy.split(":")(0)
       val logFile = sc.getConf.get("spark.log.path")
       val html = s"""
           |<ul>
           |<li><a href="http://$webProxy/proxy/$appId" target="new_tab">Spark UI</a></li>
           |<li><a href="http://$masterHost:8088/cluster/app/$appId" target="new_tab">Hadoop app: $appId</a></li>
           |<li>Local logs are available using %tail_log</li>
           |<li>Local logs are at: $logFile</li>
           |</ul>
         """.stripMargin
       val text = s"""
           |Spark $appId:
           |* http://$webProxy/proxy/$appId
           |* http://$masterHost:8088/cluster/app/$appId
           |
           |Local logs:
           |* $logFile
           |* Also available using %tail_log
         """.stripMargin
       Map(
         MIMEType.PlainText -> text,
         MIMEType.TextHtml -> html
       )
     }
   })

  registerDisplayer(classOf[SparkSession], new Displayer[SparkSession] {
    override def display(spark: SparkSession): Map[String, String] = {
      getDisplayer(spark.sparkContext) match {
        case Some(displayer) =>
          displayer.display(spark.sparkContext)
        case _ =>
          Map(MIMEType.PlainText -> String.valueOf(spark))
      }
    }
  })

  registerDisplayer(classOf[ExtendedUnitSpecBuilder],
     new Displayer[ExtendedUnitSpecBuilder] {
       class StaticHTMLRendererHTTPS(json: String) extends StaticHTMLRenderer(json) {
         override def importsHTML(additionalImports: String*): String = {
           // use HTTPS instead of HTTP
           (JSImports ++ additionalImports).map { s =>
             val newURL = s.replaceFirst("^http:", "https:")
             "<script src=\"" + newURL + "\" charset=\"utf-8\"></script>"
           }.mkString("\n")
         }
       }

       override def display(plot: ExtendedUnitSpecBuilder): Map[String, String] = {
         val plotAsJson = plot.toJson
         Map(
           MIMEType.PlainText -> plotAsJson,
           MIMEType.ApplicationJson -> plotAsJson,
           MIMEType.TextHtml -> new StaticHTMLRendererHTTPS(plotAsJson).frameHTML()
         )
       }
     })

   registerDisplayer(classOf[Object], new Displayer[Object] {
     override def display(obj: Object): Map[String, String] = {
       val objAsString = String.valueOf(obj)
       callToHTML(obj) match {
         case Some(html) =>
           Map(
             MIMEType.PlainText -> objAsString,
             MIMEType.TextHtml -> html
           )
         case None =>
           Map(MIMEType.PlainText -> objAsString)
       }
     }

     private def callToHTML(obj: Any): Option[String] = {
       import scala.reflect.runtime.{universe => ru}
       val toHtmlMethodName = ru.TermName("toHtml")
       val classMirror = ru.runtimeMirror(obj.getClass.getClassLoader)
       val objMirror = classMirror.reflect(obj)
       val toHtmlSym = objMirror.symbol.toType.member(toHtmlMethodName)
       if (toHtmlSym.isMethod) {
         Option(String.valueOf(objMirror.reflectMethod(toHtmlSym.asMethod).apply()))
       } else {
         None
       }
     }
   })

   protected def maxInterpreterThreads(kernel: KernelLike): Int = {
     kernel.config.getInt("max_interpreter_threads")
   }

   protected def bindKernelVariable(kernel: KernelLike): Unit = {
     logger.warn(s"kernel variable: ${kernel}")
//     InterpreterHelper.kernelLike = kernel
//     interpret("import org.apache.toree.kernel.interpreter.scala.InterpreterHelper")
//     interpret("import org.apache.toree.kernel.api.Kernel")
//
//     interpret(s"val kernel = InterpreterHelper.kernelLike.asInstanceOf[org.apache.toree.kernel.api.Kernel]")

     doQuietly {

       bind(
         "kernel", "org.apache.toree.kernel.api.Kernel",
         kernel, List( """@transient implicit""")
       )
     }
   }

   override def interrupt(): Interpreter = {
     require(taskManager != null)

     // Force dumping of current task (begin processing new tasks)
     taskManager.restart()

     this
   }

   override def interpret(code: String, silent: Boolean = false, output: Option[OutputStream]):
    (Results.Result, Either[ExecuteOutput, ExecuteFailure]) = {
     interpretBlock(code, silent)
   }

  def prepareResult(interpreterOutput: String,
                    showType: Boolean = false,
                    noTruncate: Boolean = false
                   ): (Option[AnyRef], Option[String], Option[String]) = {
    if (interpreterOutput.isEmpty) {
      return (None, None, None)
    }

    var lastResult = Option.empty[AnyRef]
    var lastResultAsString = ""
    val definitions = new StringBuilder
    val text = new StringBuilder

    interpreterOutput.split("\n").foreach {
      case NamedResult(name, vtype, value) if read(name).nonEmpty =>
        val result = read(name)

        lastResultAsString = result.map(String.valueOf(_)).getOrElse("")
        lastResult = result

        val defLine = (showType, noTruncate) match {
          case (true, true) =>
            s"$name: $vtype = $lastResultAsString\n"
          case (true, false) =>
            s"$name: $vtype = $value\n"
          case (false, true) =>
            s"$name = $lastResultAsString\n"
          case (false, false) =>
            s"$name = $value\n"
        }

        // suppress interpreter-defined values
        if (!name.matches("res\\d+")) {
          definitions.append(defLine)
        }

      case Definition(defType, name) =>
        lastResultAsString = ""
        definitions.append(s"defined $defType $name\n")

      case Import(name) =>
        // do nothing with the line

      case line if lastResultAsString.contains(line) =>
        // do nothing with the line

      case line =>
        text.append(line).append("\n")
    }

    (lastResult,
     if (definitions.nonEmpty) Some(definitions.toString) else None,
     if (text.nonEmpty) Some(text.toString) else None)
  }

  protected def interpretBlock(code: String, silent: Boolean = false): (Results.Result, Either[ExecuteOutput, ExecuteFailure]) = {
    interpretLine(code, silent)
  }

   protected def interpretLine(line: String, silent: Boolean = false):
     (Results.Result, Either[ExecuteOutput, ExecuteFailure]) =
   {
     logger.trace(s"Interpreting line: $line")

     val futureResult = interpretAddTask(line, silent)

     // Map the old result types to our new types
     val mappedFutureResult = interpretMapToCustomResult(futureResult)

     // Determine whether to provide an error or output
     val futureResultAndExecuteInfo = interpretMapToResultAndOutput(mappedFutureResult)

     // Block indefinitely until our result has arrived
     import scala.concurrent.duration._
     Await.result(futureResultAndExecuteInfo, Duration.Inf)
   }

   protected def interpretMapToCustomResult(future: Future[IR.Result]) = {
     import scala.concurrent.ExecutionContext.Implicits.global
     future map {
       case IR.Success             => Results.Success
       case IR.Error               => Results.Error
       case IR.Incomplete          => Results.Incomplete
     } recover {
       case ex: ExecutionException => Results.Aborted
     }
   }

   protected def display(obj: Any): Map[String, String] = {
     obj match {
       case option: Option[Any] =>
         option match {
           case Some(ref) =>
             display(ref)
           case None =>
             Map()
         }
       case _ =>
         getDisplayer(obj) match {
           case Some(displayer) =>
             try {
               displayer.display(obj)
             } catch {
               case e: Exception =>
                 logger.error("Error calling displayer %s on %s".format(
                     displayer.getClass.getName, String.valueOf(obj)), e)
                 Map(MIMEType.PlainText -> String.valueOf(obj))
             }
           case None =>
             Map(MIMEType.PlainText -> String.valueOf(obj))
         }
     }
   }

   protected def interpretMapToResultAndOutput(future: Future[Results.Result]) = {
     import scala.concurrent.ExecutionContext.Implicits.global

     future map {
       case result @ (Results.Success | Results.Incomplete) =>
         val lastOutput = lastResultOut.toString("UTF-8").trim
         lastResultOut.reset()

         val (obj, defStr, text) = prepareResult(lastOutput)
         defStr.foreach(kernel.display.content(MIMEType.PlainText, _))
         text.foreach(kernel.display.content(MIMEType.PlainText, _))
         val output = display(obj)
         (result, Left(output))

       case Results.Error =>
         val lastOutput = lastResultOut.toString("UTF-8").trim
         lastResultOut.reset()

         val (obj, defStr, text) = prepareResult(lastOutput)
         defStr.foreach(kernel.display.content(MIMEType.PlainText, _))
         val output = interpretConstructExecuteError(text.get)
         (Results.Error, Right(output))

       case Results.Aborted =>
         (Results.Aborted, Right(null))
     }
   }

   def bindSparkContext() = {
     val bindName = "sc"
     doQuietly {
       logger.info(s"Binding SparkContext into interpreter as $bindName")
       interpret(s"""def ${bindName}: ${classOf[SparkContext].getName} = kernel.sparkContext""")

       // NOTE: This is needed because interpreter blows up after adding
       //       dependencies to SparkContext and Interpreter before the
       //       cluster has been used... not exactly sure why this is the case
       // TODO: Investigate why the cluster has to be initialized in the kernel
       //       to avoid the kernel's interpreter blowing up (must be done
       //       inside the interpreter)
       logger.debug("Initializing Spark cluster in interpreter")
     }
   }

  def bindSparkSession(): Unit = {
    val bindName = "spark"
    doQuietly {
      // TODO: This only adds the context to the main interpreter AND
      //       is limited to the Scala interpreter interface
      logger.debug(s"Binding SparkSession into interpreter as $bindName")

      interpret(s"""def ${bindName}: ${classOf[SparkSession].getName} = kernel.sparkSession""")
    }
  }

  def bindSqlContext(): Unit = {
    val bindName = "sqlContext"
    doQuietly {
      logger.debug(s"Binding SQLContext into interpreter as $bindName")
      interpret(
        s"""def ${bindName}: ${classOf[SQLContext].getName} = {
           |  java.lang.System.out.println("WARNING: sqlContext is deprecated. Use spark instead.")
           |  kernel.sqlContext
           |}""".stripMargin)
    }
  }

  def defineImplicits(): Unit = {
    val code =
      """
        |import org.apache.spark.sql.SparkSession
        |import org.apache.spark.sql.SQLContext
        |import org.apache.spark.sql.SQLImplicits
        |
        |object implicits extends SQLImplicits with Serializable {
        |  protected override def _sqlContext: SQLContext = SparkSession.builder.getOrCreate.sqlContext
        |}
        |
        |import implicits._
      """.stripMargin
    doQuietly(interpret(code))
  }

   override def classLoader: ClassLoader = _runtimeClassloader

  /**
    * Returns the language metadata for syntax highlighting
    */
  override def languageInfo = LanguageInfo(
    "scala", "2.11.8",
    fileExtension = Some(".scala"),
    pygmentsLexer = Some("scala"),
    mimeType = Some("text/x-scala"),
    codemirrorMode = Some("text/x-scala"))
}

object ScalaInterpreter {

  val NamedResult = """(\w+):\s+([^=]+)\s+=\s*(.*)""".r
  val Definition = """defined\s+(\w+)\s+(.+)""".r
  val Import = """import\s+([\w\.,\{\}\s]+)""".r

  /**
    * Utility method to ensure that a temporary directory for the REPL exists for testing purposes.
    */
  def ensureTemporaryFolder(): String = {
    val outputDir = Option(System.getProperty("spark.repl.class.outputDir")).getOrElse({

      val execUri = System.getenv("SPARK_EXECUTOR_URI")
      val outputDir: String = Main.outputDir.getAbsolutePath
      System.setProperty("spark.repl.class.outputDir", outputDir)
      if (execUri != null) {
        System.setProperty("spark.executor.uri", execUri)
      }
      outputDir
    })
    outputDir
  }

}
