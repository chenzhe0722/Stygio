package indi.xeno.styx.thanatos

import indi.xeno.styx.charon.util.StrUtils.COMMA
import indi.xeno.styx.erebos.app.CommandApp.run
import indi.xeno.styx.erebos.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.thanatos.app.SparkApp
import indi.xeno.styx.thanatos.sample.{Feature, Joiner}
import indi.xeno.styx.thanatos.sample.Aggregation.get
import indi.xeno.styx.thanatos.sql.Partition
import indi.xeno.styx.thanatos.sql.Partition.{getPartition, withPartition}
import org.apache.spark.sql.DataFrame

import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer, Map => MutMap}
import scala.collection.{Map => GenMap}

object SampleApp {

  def main(args: Array[String]): Unit = {
    run(new SampleApp(), args)
  }
}

private class SampleApp() extends SparkApp() {

  override final protected def initCommand(opts: CommandOptions): Unit = {
    opts
      .singleArg("r", "record", "Label record table", "table", true)
      .singleArg("l", "label", "Label column", "column", true)
      .mapArg(
        "f",
        "feat",
        "Feature table and join columns",
        "table=column[,column...]",
        true
      )
      .listArg(
        "p",
        "partition",
        "Partition columns and values",
        "column:value[,column:value...]",
        false
      )
      .singleArg("m", "limit", "Limit on each partition", "number", true)
      .mapArg("a", "agg", "Aggregation columns and types", "column=type", true)
      .singleArg("i", "index", "Index column", "column", true)
      .singleArg("x", "suffix", "Suffix column", "column", true)
      .singleArg("w", "write", "Writing table", "table", true)
      .noArg("u", "udf", "Use user-defined aggregation function")
  }

  override final protected def run(args: CommandArguments): Unit = {
    val agg = get(
      args.getMapArg("a").asScala,
      args.getSingleArg("l").orElseThrow(),
      args.hasArg("u")
    )
    val index = args.getSingleArg("i").orElseThrow()
    val suffix = args.getSingleArg("x").orElseThrow()
    val limit = args.getSingleArg("m").orElseThrow().toLong
    val write = args.getSingleArg("w").orElseThrow()

    val label = spark.table(args.getSingleArg("r").orElseThrow())
    val feat = args.getMapArg("f").asScala.map(info => (info._1, spark.table(info._1)))
    val partitions =
      if (args.hasArg("p")) {
        args.getListArg("p").asScala.view.map(getPartition)
      } else {
        Seq(Array.empty)
      }

    if (args.hasArg("p")) {
      args
        .getListArg("p")
        .asScala
        .foreach(pt => {
          val partitions = getPartition(pt)
          val label = read(record, partitions)
          val feats = getFeat(feat, partitions)
          val df = Joiner(label, feats, agg, index, suffix, limit)
          withPartition(df, partitions)
            .writeTo(write)
            .overwritePartitions()
        })
    } else {

    }
  }

  private def getFeat(
      feat: GenMap[String, String],
      partitions: Traversable[Partition]
  ): Seq[Feature] = {
    val info = MutMap.empty[Set[String], Buffer[DataFrame]]
    feat.foreach(ft =>
      info.getOrElseUpdate(
        ft._2.split(COMMA).toSet,
        Buffer.empty
      ) += read(ft._1, partitions)
    )
    info.view.map(t => Feature(t._2, t._1.toSeq)).toSeq
  }
}
