package org.apache.toree.kernel.interpreter.scala

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

import org.apache.hadoop.fs.FileSystem

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import jupyter.Displayer
import jupyter.Displayers
import jupyter.MIMETypes
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.toree.kernel.protocol.v5.MIMEType
import org.apache.toree.magic.MagicOutput
import org.apache.toree.utils.DisplayHelpers
import vegas.DSL.ExtendedUnitSpecBuilder
import vegas.render.StaticHTMLRenderer

object ScalaDisplayers {

  def ensureLoaded(): Unit = ()

  private def toJava(body: => Map[String, String]): util.Map[String, String] = {
    body.asJava
  }

  Displayers.register(classOf[Array[Row]], new Displayer[Array[Row]] {
    override def display(arr: Array[Row]): util.Map[String, String] = toJava {
      val (text, html) = DisplayHelpers.displayRows(arr)
      Map(MIMEType.PlainText -> text, MIMEType.TextHtml -> html)
    }
  })

  Displayers.register(classOf[MagicOutput], new Displayer[MagicOutput] {
    override def display(data: MagicOutput): util.Map[String, String] = toJava {
      data.asMap
    }
  })

  Displayers.register(classOf[SparkContext], new Displayer[SparkContext] {
    override def display(sc: SparkContext): util.Map[String, String] = toJava {
      val master = sc.getConf.get("spark.master")
      val appId = sc.applicationId
      val logFile = sc.getConf.get("spark.log.path")

      if (master.startsWith("mesos")) {
        val fs = FileSystem.get(sc.hadoopConfiguration).getUri
        val master = fs.getHost.replace("hdfs", "master")
        val sparkUI = sc.uiWebUrl.getOrElse("#")
        val driverPort = sc.getConf.get("spark.driver.port")
        val html =
          s"""
             |<ul>
             |<li><a href="$sparkUI" target="new_tab">Spark UI</a></li>
             |<li><a href="http://$master:5050/#/frameworks/$appId" target="new_tab">Mesos UI: $appId</a></li>
             |<li>Local logs are available using %tail_log</li>
             |<li>Local logs are at: $logFile</li>
             |</ul>
             |""".stripMargin
        val text =
          s"""
             |Spark $appId:
             |* $sparkUI
             |* http://$master:5050/#/frameworks/$appId
             |
              |Local logs:
             |* $logFile
             |* Also available using %tail_log
             |""".stripMargin

        Map(
          MIMEType.PlainText -> text,
          MIMEType.TextHtml -> html
        )
      } else {
        val rmAddress = sc.hadoopConfiguration.get(YarnConfiguration.RM_WEBAPP_ADDRESS)
        val yarnAppUrl = "http://%s/cluster/app/%s".format(rmAddress, appId)
        val webProxy = sc.hadoopConfiguration.get("yarn.web-proxy.address")
        val html =
          s"""
             |<ul>
             |<li><a href="http://$webProxy/proxy/$appId" target="new_tab">Spark UI</a></li>
             |<li><a href="$yarnAppUrl" target="new_tab">Hadoop app: $appId</a></li>
             |<li>Local logs are available using %tail_log</li>
             |<li>Local logs are at: $logFile</li>
             |</ul>
             |""".stripMargin
        val text =
          s"""
             |Spark $appId:
             |* http://$webProxy/proxy/$appId
             |* $yarnAppUrl
             |
             |Local logs:
             |* $logFile
             |* Also available using %tail_log
             |""".stripMargin

        Map(
          MIMEType.PlainText -> text,
          MIMEType.TextHtml -> html
        )
      }
    }
  })

  Displayers.register(classOf[Option[_]], new Displayer[Option[_]] {
    override def display(option: Option[_]): util.Map[String, String] = toJava {
      val result = new mutable.HashMap[String, String]

      option match {
        case Some(wrapped) =>
          Displayers.display(wrapped).asScala.foreach {
            case (mime, text) if mime == MIMETypes.TEXT && text.indexOf("\n") < 0 =>
              // only adds Some() to single-line result text
              result.put(mime, "Some(" + text + ")")
            case (mime, value) =>
              result.put(mime, value)
          }
        case None =>
          result.put(MIMETypes.TEXT, "None")
      }

      result.toMap
    }
  })

  Displayers.register(classOf[SparkSession], new Displayer[SparkSession] {
    override def display(spark: SparkSession): util.Map[String, String] = {
      Displayers.display(spark.sparkContext)
    }
  })

  // Displayer for Vegas
  Displayers.register(classOf[ExtendedUnitSpecBuilder],
    new Displayer[ExtendedUnitSpecBuilder] {
      override def display(plot: ExtendedUnitSpecBuilder): util.Map[String, String] = toJava {
        val plotAsJson = plot.toJson
        Map(
          MIMEType.PlainText -> plotAsJson,
          MIMEType.ApplicationJson -> plotAsJson,
          MIMEType.TextHtml -> new StaticHTMLRenderer(plotAsJson).frameHTML()
        )
      }
    })

  // Set the default displayer to call toHtml if present on Scala objects
  Displayers.registration.setDefault(new Displayer[Object] {
    override def display(obj: Object): util.Map[String, String] = toJava {
      if (obj.getClass.isArray) {
        Map(MIMETypes.TEXT -> obj.asInstanceOf[Array[_]].map(
          elem => Displayers.display(elem).get(MIMETypes.TEXT)
        ).mkString("[", ", ", "]"))
      } else {
        val objAsString = String.valueOf(obj)
        Try(callToHTML(obj)).toOption.flatten match {
          case Some(html) =>
            Map(
              MIMETypes.TEXT -> objAsString,
              MIMETypes.HTML -> html
            )
          case None =>
            Map(MIMETypes.TEXT -> objAsString)
        }
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
}
