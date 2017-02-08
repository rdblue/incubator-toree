package org.apache.toree.interpreter

trait Displayer[T] {
  def display(obj: T): Map[String, String]
}
