package indi.xeno.styx.thanatos

import indi.xeno.styx.charon.util.ResourceUtils.read
import indi.xeno.styx.erebos.app.CommandApp.run
import indi.xeno.styx.erebos.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.thanatos.app.SparkApp

import scala.io.Source.fromInputStream

object SqlApp {

  def main(args: Array[String]): Unit = {
    run(new SqlApp(), args)
  }
}

private class SqlApp() extends SparkApp() {

  override final protected def initCommand(opts: CommandOptions): Unit = {
    opts.addSingleArgOption("f", "file", "SQL script", "path", true)
      .addNoArgOption("d", "hdfs", "Use SQL script in HDFS")
  }

  override final protected def run(args: CommandArguments): Unit = {
    val file = args.getSingleArg("f").orElseThrow()
    val sql = if (args.hasArg("d")) {
      spark.read.textFile(file).reduce(_ + _)
    } else {
      fromInputStream(read(file)).mkString
    }
    spark.sql(sql)
  }
}
