package org.apache.toree.utils

import scala.collection.mutable

import org.apache.spark.sql.Row

object DisplayHelpers {
  def displayRows(
        rows: Array[Row],
        name: Option[String] = None,
        fields: Option[Seq[String]] = None,
        isTruncated: Boolean = false): (String, String) = {
    if (rows.length < 1) {
      return ("", "")
    }

    val lengths = Array.fill(rows(0).length)(3)
    val cells = rows.map { row =>
      row.toSeq.zipWithIndex.map {
        case (value, pos) =>
          val repr = value match {
            case null => "NULL"
            case binary: Array[Byte] =>
              binary.map("%02X".format(_)).mkString("[", " ", "]")
            case arr: Array[_] =>
              arr.mkString("[", ", ", "]")
            case seq: Seq[_] =>
              seq.mkString("[", ", ", "]")
            case map: Map[_, _] =>
              map.map {
                case (k: Any, v: Any) => s"$k -> $v"
              }.mkString("{", ", ", "}")
            case _ =>
              value.toString
          }
          lengths(pos) = Math.max(lengths(pos), repr.length)
          repr
      }
    }

    fields match {
      case Some(names) =>
        names.zipWithIndex.foreach {
          case (name, pos) =>
            lengths(pos) = Math.max(lengths(pos), name.length)
        }
      case _ =>
    }

    var htmlLines = new mutable.ArrayBuffer[String]()
    htmlLines
    htmlLines += "<table>"

    var lines = new mutable.ArrayBuffer[String]()
    val divider = lengths.map(l => "-" * (l + 2)).mkString("+", "+", "+")
    val format = lengths.map(l => s" %-${l}s ").mkString("|", "|", "|")
    lines += divider

    name match {
      case Some(varName) =>
        htmlLines += s"<tr><th colspan=${lengths.length}>$varName</th></tr>"
      case _ =>
    }

    fields match {
      case Some(names) =>
        htmlLines += names.mkString("<tr><th>", "</th><th>", "</th></tr>")
        lines += String.format(format, names:_*)
        lines += divider
      case _ =>
    }

    cells.foreach { row =>
      htmlLines += row.mkString("<tr><td>", "</td><td>", "</td></tr>")
      lines += String.format(format, row: _*)
    }

    if (isTruncated) {
      val dots = Array.fill(lengths.length)("...")
      htmlLines += dots.mkString("<tr><td>", "</td><td>", "</td></tr>")
      lines += String.format(format, dots: _*)
    }

    htmlLines += "</table>"
    lines += divider

    name match {
      case Some(varName) =>
        lines += s"  available as $varName"
      case _ =>
    }

    (lines.mkString("\n"), htmlLines.mkString("\n"))
  }
}
