package indi.xeno.styx.thanatos

import indi.xeno.styx.charon.util.StrUtils.{COLON, COMMA}
import indi.xeno.styx.erebos.app.CommandApp.run
import indi.xeno.styx.erebos.app.{CommandArguments, CommandOptions}
import indi.xeno.styx.thanatos.app.SparkApp
import indi.xeno.styx.thanatos.fn.agg.MapAggregator
import indi.xeno.styx.thanatos.sample.{Aggregation, Feature, Joiner}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, first, lit}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes.{ByteType, createArrayType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}

import scala.collection.JavaConverters._
import scala.collection.{Map => GenMap}
import scala.collection.mutable.{Buffer, Map => MutMap}

object SampleApp {

  def main(args: Array[String]): Unit = {
    run(new SampleApp(), args)
  }

  private def getAgg(
    arg: GenMap[String, String],
    label: String,
    udf: Boolean,
  ): Seq[Aggregation] = {
    arg.view.map(getSingleAgg(udf)).toBuffer +=
      Aggregation(label, IntegerType, first(_, true))
  }

  private def getSingleAgg(udf: Boolean)(arg: (String, String)): Aggregation = {
    val buildSingle = buildSingleAgg(udf, arg._1)
    val buildArray = buildArrayAgg(udf, arg._1)
    arg._2 match {
      case "byte" => buildSingle(ByteType, MapAggregator.BYTE_MAP_AGG)
      case "short" => buildSingle(ShortType, MapAggregator.SHORT_MAP_AGG)
      case "int" => buildSingle(IntegerType, MapAggregator.INT_MAP_AGG)
      case "long" => buildSingle(LongType, MapAggregator.LONG_MAP_AGG)
      case "float" => buildSingle(FloatType, MapAggregator.FLOAT_MAP_AGG)
      case "double" => buildSingle(DoubleType, MapAggregator.DOUBLE_MAP_AGG)
      case "string" => buildSingle(StringType, MapAggregator.STRING_MAP_AGG)
      case "byte_array" => buildArray(ByteType, MapAggregator.BYTE_ARRAY_MAP_AGG)
      case "short_array" => buildArray(ShortType, MapAggregator.SHORT_ARRAY_MAP_AGG)
      case "int_array" => buildArray(IntegerType, MapAggregator.INT_ARRAY_MAP_AGG)
      case "long_array" => buildArray(LongType, MapAggregator.LONG_ARRAY_MAP_AGG)
      case "float_array" => buildArray(FloatType, MapAggregator.FLOAT_ARRAY_MAP_AGG)
      case "double_array" => buildArray(DoubleType, MapAggregator.DOUBLE_ARRAY_MAP_AGG)
      case "string_array" => buildArray(ShortType, MapAggregator.SHORT_ARRAY_MAP_AGG)
      case _ => throw new IllegalArgumentException()
    }
  }

  private def buildSingleAgg(
    udf: Boolean,
    name: String,
  ): (DataType, UserDefinedFunction) => Aggregation = {
    (ty, agg) => Aggregation(name, ty, if (udf) agg(_) else MapAggregator.mapAgg)
  }

  private def buildArrayAgg(
    udf: Boolean,
    name: String,
  ): (DataType, UserDefinedFunction) => Aggregation = {
    (ty, agg) => Aggregation(
      name,
      createArrayType(ty, false),
      if (udf) agg(_) else MapAggregator.mapAgg)
  }

  private def getPartition(part: String): (String, String) = {
    val i = part.indexOf(COLON)
    if (i < 0) {
      (part.substring(0, i), part.substring(i + 1))
    } else {
      throw new IllegalArgumentException()
    }
  }
  
  private def wherePartition(
    df: DataFrame,
    partition: (String, String),
  ): DataFrame = {
    df.where(col(partition._1) === lit(partition._2))
  }

  private def withPartition(
    df: DataFrame,
    partition: (String, String),
  ): DataFrame = {
    df.withColumn(partition._1, lit(partition._2))
  }
}

private class SampleApp() extends SparkApp() {

  override final protected def initCommand(opts: CommandOptions): Unit = {
    opts.addSingleArgOption("r", "record", "Label record table", "table", true)
      .addSingleArgOption("l", "label", "Label column", "column", true)
      .addMapArgOption("f", "feat", "Feature table and join columns", "table=column[,column...]", true)
      .addListArgOption("p", "partition", "Partition columns and values", "column:value[,column:value...]", false)
      .addMapArgOption("a", "agg", "Aggregation columns and types", "column=type", true)
      .addSingleArgOption("i", "index", "Index column", "column", true)
      .addSingleArgOption("x", "suffix", "Suffix column", "column", true)
      .addSingleArgOption("m", "limit", "Limitation of record processed by one partition", "number", true)
      .addSingleArgOption("w", "write", "Writing table", "table", true)
      .addNoArgOption("u", "udf", "Use user-defined aggregation function")
  }

  override final protected def run(args: CommandArguments): Unit = {
    val record = args.getSingleArg("r").orElseThrow()
    val feat = args.getMapArg("f").asScala
    val agg = SampleApp.getAgg(
      args.getMapArg("a").asScala,
      args.getSingleArg("l").orElseThrow(),
      args.hasArg("u"))
    val index = args.getSingleArg("i").orElseThrow()
    val suffix = args.getSingleArg("x").orElseThrow()
    val limit = args.getSingleArg("m").orElseThrow().toLong
    val write = args.getSingleArg("w").orElseThrow()
    if (args.hasArg("p")) {
      args.getListArg("p").asScala.foreach(pt => {
        val partitions = pt.split(COMMA).map(SampleApp.getPartition)
        val df = Joiner(
          read(record, partitions),
          getFeat(feat, partitions),
          agg, index, suffix, limit)
        partitions.foldLeft(df)(SampleApp.withPartition)
          .writeTo(write)
          .overwritePartitions()
      })
    } else {
      Joiner(
        read(record, Seq.empty),
        getFeat(feat, Seq.empty),
        agg, index, suffix, limit,
      ).writeTo(write).createOrReplace()
    }
  }

  private def getFeat(
    feat: GenMap[String, String],
    partitions: Seq[(String, String)],
  ): Seq[Feature] = {
    val info = MutMap.empty[Set[String], Buffer[DataFrame]]
    feat.foreach(ft =>
      info.getOrElseUpdate(
        ft._2.split(COMMA).toSet,
        Buffer.empty,
      ) += read(ft._1, partitions))
    info.view.map(t => Feature(t._2, t._1.toSeq)).toSeq
  }

  private def read(
    name: String,
    partitions: Seq[(String, String)],
  ): DataFrame = {
    val df = spark.table(name)
    partitions
      .view
      .filter(partition => df.columns.contains(partition._1))
      .foldLeft(df)(SampleApp.wherePartition)
  }
}
