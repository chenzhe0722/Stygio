package indi.xeno.styx.thanatos.exec

import indi.xeno.styx.charon.util.BytesUtils.{doubleToBytes, floatToBytes, intToBytes, longToBytes, shortToBytes}
import org.apache.spark.sql.Row

import java.nio.ByteOrder
import java.nio.charset.Charset

private[exec] object ValueExtractor {

  def shortValue(row: Row, endian: ByteOrder): Array[Byte] = {
    shortToBytes(row.getShort(1), endian)
  }

  def intValue(row: Row, endian: ByteOrder): Array[Byte] = {
    intToBytes(row.getInt(1), endian)
  }

  def longValue(row: Row, endian: ByteOrder): Array[Byte] = {
    longToBytes(row.getLong(1), endian)
  }

  def floatValue(row: Row, endian: ByteOrder): Array[Byte] = {
    floatToBytes(row.getFloat(1), endian)
  }

  def doubleValue(row: Row, endian: ByteOrder): Array[Byte] = {
    doubleToBytes(row.getDouble(1), endian)
  }

  def strValue(row: Row, cs: Charset): Array[Byte] = {
    row.getString(1).getBytes(cs)
  }

  def binValue(row: Row): Array[Byte] = {
    row.getSeq[Byte](1).toArray
  }

  def strToStrValue(
      row: Row,
      cs: Charset
  ): Iterator[(Array[Byte], Array[Byte])] = {
    row
      .getMap[String, String](1)
      .iterator
      .map(t => (t._1.getBytes(cs), t._2.getBytes(cs)))
  }
}
