package indi.xeno.styx.thanatos

import indi.xeno.styx.charon.util.HttpUtils.httpGet
import indi.xeno.styx.erebos.app.CommandApp.run
import indi.xeno.styx.erebos.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.thanatos.app.SparkApp
import indi.xeno.styx.thanatos.exec.TaskRunner.{hdfs, http, local, resource}
import indi.xeno.styx.thanatos.util.SourceUtils.closeSource
import org.apache.spark.sql.functions.col

import java.net.http.HttpResponse.BodyHandlers.ofString
import scala.collection.JavaConverters._
import scala.io.Source.{fromFile, fromResource}

object RedisApp {

  def main(args: Array[String]): Unit = {
    run(new RedisApp(), args)
  }
}

private class RedisApp() extends SparkApp() {

  override final protected def initCommand(opts: CommandOptions): Unit = {
    opts
      .mapArg("f", "conf", "Redis configuration URI", "type=path", true)
      .mapArg("s", "sql", "Sql script URI", "type=path", true)
      .mapArg("c", "column", "Column name", "key=value", true)
  }

  override final protected def run(args: CommandArguments): Unit = {
    val (key, value) = args.getMapArg("c").asScala.head
    val sql = args.getMapArg("s").asScala.head match {
      case ("local", uri) => closeSource(_.mkString, fromFile(uri))
      case ("hdfs", uri)  => spark.read.textFile(uri).reduce(_ + _)
      case ("http", uri) =>
        httpGet(uri, ofString()).thenApply[String](_.body).get()
      case (_, uri) => closeSource(_.mkString, fromResource(uri))
    }
    val table = spark.sql(sql).select(col(key), col(value))
    val ty = table.schema.last.dataType
    val task = args.getMapArg("f").asScala.head match {
      case ("local", uri) => local(uri, ty)
      case ("hdfs", uri)  => hdfs(uri, ty)
      case ("http", uri)  => http(uri, ty)
      case (_, uri)       => resource(uri, ty)
    }
    table.foreachPartition(task)
  }
}
