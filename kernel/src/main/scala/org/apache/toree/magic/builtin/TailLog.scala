package org.apache.toree.magic.builtin

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.PrintStream

import scala.collection.mutable

import org.apache.toree.magic.LineMagic
import org.apache.toree.magic.dependencies.IncludeKernel
import org.apache.toree.magic.dependencies.IncludeOutputStream
import org.apache.toree.plugins.annotations.Event
import org.apache.toree.utils.ArgumentParsingSupport

object TailLog {
  val LOG_PATH_PROP = "spark.log.path"
  val int = "(\\d+)".r
}

class TailLog extends LineMagic
    with IncludeKernel with IncludeOutputStream with ArgumentParsingSupport {

  import TailLog._

  private def out = new PrintStream(outputStream)

  @Event(name = "tail_log")
  override def execute(code: String): Unit = {
    val numLines = code.trim match {
      case int(digits) => digits.toInt
      case _ => 100
    }

    Option(kernel.sparkContext) match {
      case Some(sc) =>
        val logFile = new File(sc.getConf.get(LOG_PATH_PROP))
        if (logFile.exists && logFile.canRead) {
          var lines = new mutable.ListBuffer[String]
          val reader = new BufferedReader(new FileReader(logFile))
          var line = reader.readLine()
          var count = 0
          while (line != null) {
            count += 1
            lines.append(line)
            line = reader.readLine()

            // periodically truncate lines to save space
            if (count % numLines == 0) {
              lines = lines.takeRight(numLines)
            }
          }
          lines = lines.takeRight(numLines)

          lines.foreach(printHelp(out, _))

        } else {
          printHelp(out, "Cannot read log path: " + logFile)
        }
      case _ =>
        printHelp(out, "Cannot get log path: sc is not defined")
    }
  }
}
