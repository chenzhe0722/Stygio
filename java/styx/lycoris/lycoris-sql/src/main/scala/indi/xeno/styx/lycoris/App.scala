package indi.xeno.styx.lycoris

import java.lang.String.join
import java.lang.System.lineSeparator

import indi.xeno.styx.charon.app.CommandApp.run
import indi.xeno.styx.charon.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.lycoris.app.SparkApp
import org.apache.spark.sql.functions.col

import scala.collection.JavaConverters._

object App {

  def main(args: Array[String]): Unit = {
    run(new App(), args)
  }
}

private class App extends SparkApp {

  override final protected def initCommand(opts: CommandOptions): CommandOptions = {
    opts.addSingleArgOption("f", "file", "SQL file on hdfs", "path", true)
      .addSingleArgOption("t", "table", "Table name in hive", "name", true)
      .addListArgOption("p", "partition", "Partition name", "name", false)
  }

  override final protected def run(args: CommandArguments): Unit = {
    val sql = spark.readStream
      .textFile(args.getSingleArg("f").orElseThrow()).collect()
    val writer = spark.sql(join(lineSeparator(), sql: _*))
      .writeTo(args.getSingleArg("t").orElseThrow())
    val partition = args.getListArg("p").asScala
    if (partition.isEmpty) {
      writer.createOrReplace()
    } else {
      writer
        .partitionedBy(col(partition.head), partition.view.drop(1).map(col): _*)
        .createOrReplace()
    }
  }
}
