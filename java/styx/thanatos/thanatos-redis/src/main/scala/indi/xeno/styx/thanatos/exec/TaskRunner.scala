package indi.xeno.styx.thanatos.exec

import indi.xeno.styx.aiakos.RedisConnection
import indi.xeno.styx.charon.util.HttpUtils.httpGet
import indi.xeno.styx.charon.util.IOUtils.closeInput
import indi.xeno.styx.charon.util.ResourceUtils.readResource
import indi.xeno.styx.erebos.util.YamlUtils.readYaml
import indi.xeno.styx.thanatos.exec.RedisOperator.{hsetOp, setOp}
import indi.xeno.styx.thanatos.exec.ValueExtractor.{binValue, doubleValue, floatValue, intValue, longValue, shortValue, strToStrValue, strValue}
import indi.xeno.styx.thanatos.util.SourceUtils.closeSource
import org.apache.hadoop.fs.FileContext.getFileContext
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BinaryType, DataType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType}

import java.io.InputStream
import java.net.http.HttpResponse.BodyHandlers.ofInputStream
import scala.io.Source.fromFile

object TaskRunner {

  private type Conn = RedisConnection[Array[Byte], Array[Byte]]

  def local(uri: String, ty: DataType): Iterator[Row] => Unit = {
    val text = closeSource(_.mkString, fromFile(uri))
    process(() => readYaml(text, classOf[Conf]), ty)
  }

  def hdfs(uri: String, ty: DataType): Iterator[Row] => Unit = {
    process(() => closeInput(readConf, getFileContext.open(new Path(uri))), ty)
  }

  def http(uri: String, ty: DataType): Iterator[Row] => Unit = {
    val getConf = () =>
      httpGet(uri, ofInputStream())
        .thenApply[Conf](response => closeInput(readConf, response.body()))
        .get()
    process(getConf, ty)
  }

  def resource(uri: String, ty: DataType): Iterator[Row] => Unit = {
    process(() => closeInput(readConf, readResource(uri)), ty)
  }

  private def readConf(is: InputStream): Conf = {
    readYaml(is, classOf[Conf])
  }

  private def process(getConf: () => Conf, ty: DataType)(
      it: Iterator[Row]
  ): Unit = {
    val conf = getConf()
    val cs = conf.keyCharset()
    val conn = conf.connect()
    val cmd = conn.getCmd
    val op = ty match {
      case ShortType   => setOp(cmd, cs, shortValue(_, conf.endian())) _
      case IntegerType => setOp(cmd, cs, intValue(_, conf.endian())) _
      case LongType    => setOp(cmd, cs, longValue(_, conf.endian())) _
      case FloatType   => setOp(cmd, cs, floatValue(_, conf.endian())) _
      case DoubleType  => setOp(cmd, cs, doubleValue(_, conf.endian())) _
      case StringType  => setOp(cmd, cs, strValue(_, conf.valueCharset())) _
      case BinaryType  => setOp(cmd, cs, binValue) _
      case MapType(StringType, StringType, _) =>
        hsetOp(cmd, cs, strToStrValue(_, conf.valueCharset())) _
      case _ => throw new IllegalArgumentException()
    }
    it.foldLeft(conf.batch())((agg, r) => {
      agg.add(op(r).toCompletableFuture, () => cmd.flushCommands())
    }).all(() => cmd.flushCommands())
      .thenCompose(_ => conn.shutdown())
      .get()
  }
}
