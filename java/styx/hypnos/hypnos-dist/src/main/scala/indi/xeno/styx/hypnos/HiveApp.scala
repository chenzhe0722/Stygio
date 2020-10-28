package indi.xeno.styx.hypnos

import indi.xeno.styx.charon.util.HttpUtils.httpGet
import indi.xeno.styx.hypnos.app.FlinkApp.run
import indi.xeno.styx.hypnos.app.{CommandArguments, CommandOptions, FlinkApp}
import indi.xeno.styx.hypnos.util.SourceUtils.closeSource

import java.net.http.HttpResponse.BodyHandlers.ofString
import scala.io.Source.{fromFile, fromResource}

object HiveApp {

  def main(args: Array[String]): Unit = {
    run(new HiveApp(), args)
  }
}

private class HiveApp() extends FlinkApp() {

  protected final def initCommand(opts: CommandOptions): Unit = {
    opts
      .mapArg("s", "sql", "Flink sql script uri", "uri", true)
  }

  protected final def run(args: CommandArguments): Unit = {
    val sql = args.getMapArg("s").head match {
      case ("local", uri) => closeSource(_.mkString, fromFile(uri))
      case ("http", uri) =>
        httpGet(uri, ofString()).thenApply[String](_.body).get()
      case (_, uri) => closeSource(_.mkString, fromResource(uri))
    }
  }
}
