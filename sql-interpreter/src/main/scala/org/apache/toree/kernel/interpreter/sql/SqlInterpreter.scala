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
package org.apache.toree.kernel.interpreter.sql

import java.net.URL
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.mutable

import org.apache.toree.interpreter.Results.Result
import org.apache.toree.interpreter._
import org.apache.toree.kernel.api.KernelLike
import org.apache.toree.utils.DisplayHelpers
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.tools.nsc.interpreter.{InputStream, OutputStream}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StructType

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.toree.interpreter.Results.Success

/**
 * Represents an interpreter interface to Spark SQL.
 */
class SqlInterpreter() extends Interpreter {
  private val executor = Executors.newSingleThreadExecutor(
    new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("s3-committer-pool-%d")
        .build)

  private var kernel: KernelLike = _
  private var executionCount = 0
  private var lastVar = Option.empty[String]

  private val queries: mutable.Map[String, DataFrame] =
    new mutable.HashMap[String, DataFrame]()

  override def init(kernel: KernelLike): Interpreter = {
    this.kernel = kernel
    this
  }

  /**
   * Executes the provided code with the option to silence output.
   * @param code The code to execute
   * @param silent Whether or not to execute the code silently (no output)
   * @return The success/failure of the interpretation and the output from the
   *         execution or the failure
   */
  override def interpret(code: String, silent: Boolean, output: Option[OutputStream]):
    (Result, Either[ExecuteOutput, ExecuteFailure]) = {

    val spark = kernel.sparkSession

    val statements = code.split(";").map(_.trim).filter(_.nonEmpty)
    val iter = statements.iterator
    var lastResult: (Result, Either[ExecuteOutput, ExecuteFailure]) =
      (Success, Left(Map.empty[String, String]))
    var failed = false
    while (iter.hasNext && !failed) {
      val sql = iter.next
      lastResult = runStatement(sql, spark)
      lastResult._1 match {
        case Success if lastResult._2.isLeft =>
          // successful if result is success AND output isn't ExecuteFailure
          if (iter.hasNext) {
            // if there is another statement to run, display output from last
            kernel.display.content(lastResult._2.left.get)
          }
        case _ =>
          // stop executing statements
          failed = true
      }
    }

    lastResult
  }

  private def runStatement(sql: String, spark: SparkSession): (Result, Either[ExecuteOutput, ExecuteFailure]) = {
    val varName = nextVar
    this.lastVar = Some(varName)

    val execution: Future[Option[(StructType, Array[Row])]] = Future {
      val resultDF = spark.sql(sql)

      // save the query for later use
      queries.put(varName, resultDF)

      // determine the query type
      val logicalPlan = resultDF.queryExecution.logical
      if (logicalPlan.isInstanceOf[Command]) {
        //resultDF.queryExecution.sparkPlan.execute()
        Some((resultDF.schema, resultDF.take(1001)))
      } else {
        // let Spark format the rows as a string
        Some((resultDF.schema, resultDF.take(1001)))
      }
    }(ExecutionContext.fromExecutorService(executor))

    val converted: Future[(Result, Either[ExecuteOutput, ExecuteFailure])] =
      execution.map {
        case Some((schema, rows)) =>
          val (text, html) = DisplayHelpers.displayRows(
            rows.take(1000),
            Some(schema.map(_.name)),
            isTruncated = rows.length == 1001)
          (Results.Success, Left(Map(
            "text/plain" -> text,
            "text/html" -> html
          )))
        case None =>
          (Results.Success, Left(Map.empty[String, String]))
      }(ExecutionContext.fromExecutorService(executor))

    val sqlFuture = converted.recover {
      case error: Throwable =>
        (Results.Error, Right(ExecuteError(
          error.getClass.getSimpleName,
          error.getMessage,
          error.getStackTrace.map(_.toString).toList)))
    }(ExecutionContext.fromExecutorService(executor))

    Await.result(sqlFuture, Duration.Inf)
  }

  /**
   * Attempt to determine if a multiline block of code is complete
   * @param code The code to determine for completeness
   */
  override def isComplete(code: String): (String, String) = super.isComplete(code)

  /**
   * Starts the interpreter, initializing any internal state.
   * @return A reference to the interpreter
   */
  override def start(): Interpreter = {
    this
  }

  /**
   * Stops the interpreter, removing any previous internal state.
   * @return A reference to the interpreter
   */
  override def stop(): Interpreter = {
    executor.shutdown()
    executor.awaitTermination(1000, TimeUnit.MILLISECONDS)
    this
  }

  /**
   * Returns the class loader used by this interpreter.
   *
   * @return The runtime class loader used by this interpreter
   */
  override def classLoader: ClassLoader = this.getClass.getClassLoader

  override def lastExecutionVariableName: Option[String] = lastVar

  override def read(variableName: String): Option[AnyRef] = queries.get(variableName)

  override def interrupt(): Interpreter = {
    // TODO: interrupt the executor's thread
    this
  }

  override def bind(variableName: String, typeName: String, value: Any, modifiers: List[String]): Unit = {
    val sparkSqlConf = kernel.sparkSession.conf
    value match {
      case s: String =>
        sparkSqlConf.set(variableName, s)
      case i: Int =>
        sparkSqlConf.set(variableName, i)
      case l: Long =>
        sparkSqlConf.set(variableName, l)
      case b: Boolean =>
        sparkSqlConf.set(variableName, b)
      case _ =>
        throw new RuntimeException("Invalid type: " + typeName)
    }
  }

  // Jars are added to Spark by the kernel. Nothing else needs to be done.
  override def addJars(jars: URL*): Unit = ()

  // Results are always returned, not printed
  override def updatePrintStreams(in: InputStream, out: OutputStream, err: OutputStream): Unit = ()

  // Results are always returned, not printed
  override def doQuietly[T](body: => T): T = {
    body
  }

  override def languageInfo = LanguageInfo(
    "sql", "2.11.8",
    fileExtension = Some(".sql"),
    pygmentsLexer = Some("sql"),
    mimeType = Some("text/x-sql"),
    codemirrorMode = Some("text/x-sql"))

  private def nextVar: String = {
    val varName = "df" + executionCount
    executionCount += 1
    return varName
  }
}
