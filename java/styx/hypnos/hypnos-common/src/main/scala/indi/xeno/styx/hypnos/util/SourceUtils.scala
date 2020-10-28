package indi.xeno.styx.hypnos.util

import scala.io.Source

object SourceUtils {

  def closeSource[T](fn: Source => T, src: Source): T = {
    val t = fn(src)
    src.close()
    t
  }
}
