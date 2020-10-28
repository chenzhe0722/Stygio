package indi.xeno.styx.thanatos

import indi.xeno.styx.erebos.app.CommandApp.run
import indi.xeno.styx.erebos.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.thanatos.app.SparkApp
import indi.xeno.styx.thanatos.cf.ItemCf
import indi.xeno.styx.thanatos.sql.Partition.{getPartition, wherePartition, withPartition}

import scala.collection.JavaConverters._

object ItemCfApp {

  def main(args: Array[String]): Unit = {
    run(new ItemCfApp(), args)
  }
}

private class ItemCfApp() extends SparkApp() {

  override final protected def initCommand(opts: CommandOptions): Unit = {
    opts
      .singleArg("r", "src", "Source data", "table", true)
      .singleArg("d", "dst", "Destination storage", "table", true)
      .singleArg("i", "item", "Item column", "column", true)
      .singleArg("u", "user", "User column", "column", true)
      .singleArg("s", "score", "Score column", "column", true)
      .singleArg("n", "norm", "Norm column", "column", true)
      .singleArg("y", "similarity", "Similarity column", "column", true)
      .singleArg("x", "suffix", "Column suffix", "column", true)
      .listArg(
        "p",
        "partition",
        "Partition columns and values",
        "column:value[,column:value...]",
        false
      )
  }

  override final protected def run(args: CommandArguments): Unit = {
    val cf = new ItemCf(
      args.getSingleArg("i").orElseThrow(),
      args.getSingleArg("u").orElseThrow(),
      args.getSingleArg("s").orElseThrow(),
      args.getSingleArg("n").orElseThrow(),
      args.getSingleArg("y").orElseThrow(),
      args.getSingleArg("x").orElseThrow()
    )
    val dst = args.getSingleArg("d").orElseThrow()
    val src = spark.table(args.getSingleArg("r").orElseThrow())
    if (args.hasArg("p")) {
      args
        .getListArg("p")
        .asScala
        .view
        .map(getPartition)
        .foreach(pts =>
          withPartition(cf.run(wherePartition(src, pts)), pts)
            .writeTo(dst)
            .overwritePartitions()
        )
    } else {
      cf.run(src)
        .writeTo(dst)
        .createOrReplace()
    }
  }
}
