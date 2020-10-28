package indi.xeno.styx.thanatos

import indi.xeno.styx.charon.util.HttpUtils.httpGet
import indi.xeno.styx.erebos.app.CommandApp.run
import indi.xeno.styx.erebos.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.thanatos.app.SparkApp
import indi.xeno.styx.thanatos.util.SourceUtils.closeSource

import java.net.http.HttpResponse.BodyHandlers.ofString
import scala.collection.JavaConverters._
import scala.io.Source.{fromFile, fromResource}

object SqlApp {

  def main(args: Array[String]): Unit = {
    run(new SqlApp(), args)
  }
}

private class SqlApp() extends SparkApp() {

  override final protected def initCommand(opts: CommandOptions): Unit = {
    opts.mapArg("s", "sql", "SQL script URI", "type=path", true)
  }

  override final protected def run(args: CommandArguments): Unit = {
    val sql = args.getMapArg("s").asScala.head match {
      case ("local", uri) => closeSource(_.mkString, fromFile(uri))
      case ("hdfs", uri)  => spark.read.textFile(uri).reduce(_ + _)
      case ("http", uri) =>
        httpGet(uri, ofString()).thenApply[String](_.body).get()
      case (_, uri) => closeSource(_.mkString, fromResource(uri))
    }
    spark.sql(sql)
  }
}
