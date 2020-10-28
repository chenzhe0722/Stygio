package indi.xeno.styx.thanatos.exec

import indi.xeno.styx.aiakos.RedisConnection
import indi.xeno.styx.charon.common.FutureBatch
import io.lettuce.core.codec.ByteArrayCodec.INSTANCE

import java.nio.ByteOrder
import java.nio.ByteOrder.{BIG_ENDIAN, LITTLE_ENDIAN}
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets.{US_ASCII, UTF_8}
import scala.collection.JavaConverters._

private[exec] case class Conf(
    uri: Iterable[String],
    cluster: Boolean,
    ioThreadPoolSize: Option[Int],
    computationThreadPoolSize: Option[Int],
    flushSize: Int,
    keyAscii: Option[Boolean],
    valueAscii: Option[Boolean],
    littleEndian: Option[Boolean]
) {

  def connect(): RedisConnection[Array[Byte], Array[Byte]] = {
    val coll = uri.asJavaCollection
    (ioThreadPoolSize, computationThreadPoolSize) match {
      case (Some(io), Some(comp)) =>
        new RedisConnection(coll, cluster, io, comp, INSTANCE, false)
      case (None, None) =>
        new RedisConnection(coll, cluster, INSTANCE, false)
      case _ => throw new IllegalArgumentException()
    }
  }

  def batch(): FutureBatch = {
    new FutureBatch(flushSize)
  }

  def endian(): ByteOrder = {
    if (littleEndian.getOrElse(true)) LITTLE_ENDIAN else BIG_ENDIAN
  }

  def keyCharset(): Charset = {
    if (keyAscii.getOrElse(false)) US_ASCII else UTF_8
  }

  def valueCharset(): Charset = {
    if (valueAscii.getOrElse(false)) US_ASCII else UTF_8
  }
}
