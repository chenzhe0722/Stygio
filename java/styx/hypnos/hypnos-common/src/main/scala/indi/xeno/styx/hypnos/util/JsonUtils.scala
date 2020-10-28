package indi.xeno.styx.hypnos.util

import org.json4s.native.Serialization.{formats, read}
import org.json4s.{FileInput, Formats, JsonInput, NoTypeHints, StreamInput}

import java.io.{File, InputStream, Reader}

object JsonUtils {

  private val FORMATS: Formats = formats(NoTypeHints)

  def deserialize[T](str: String)(implicit mf: Manifest[T]): T = {
    read[T](str)(FORMATS, mf)
  }

  def deserialize[T](reader: Reader)(implicit mf: Manifest[T]): T = {
    read[T](reader)(FORMATS, mf)
  }

  def deserialize[T](is: InputStream)(implicit mf: Manifest[T]): T = {
    read[T](StreamInput(is))(FORMATS, mf)
  }

  def deserialize[T](file: File)(implicit mf: Manifest[T]): T = {
    read[T](FileInput(file))(FORMATS, mf)
  }

  def deserialize[T](input: JsonInput)(implicit mf: Manifest[T]): T = {
    read[T](input)(FORMATS, mf)
  }
}
