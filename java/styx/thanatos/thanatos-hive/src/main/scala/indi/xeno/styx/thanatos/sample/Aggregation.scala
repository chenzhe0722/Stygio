package indi.xeno.styx.thanatos.sample

import indi.xeno.styx.thanatos.fn.agg.MapAggregator.{BYTE_ARRAY_MAP_AGG, BYTE_MAP_AGG, DOUBLE_ARRAY_MAP_AGG, DOUBLE_MAP_AGG, FLOAT_ARRAY_MAP_AGG, FLOAT_MAP_AGG, INT_ARRAY_MAP_AGG, INT_MAP_AGG, LONG_ARRAY_MAP_AGG, LONG_MAP_AGG, SHORT_ARRAY_MAP_AGG, SHORT_MAP_AGG, STRING_ARRAY_MAP_AGG, STRING_MAP_AGG, mapAgg}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, first, lit}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes.{ByteType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, createArrayType}
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.{Map => GenMap}

object Aggregation {

  def apply(
      df: DataFrame,
      index: String,
      aggregation: Seq[Aggregation]
  ): DataFrame = {
    df.select(aggregation.map(_.select(df.columns)) :+ col(index): _*)
  }

  def get(
      arg: GenMap[String, String],
      label: String,
      udf: Boolean
  ): Seq[Aggregation] = {
    arg.map(build(_, udf)).toBuffer +=
      new Aggregation(label, IntegerType, first(_, true))
  }

  private def build(arg: (String, String), udf: Boolean): Aggregation = {
    val atomic = buildAtomic(arg._1, _, _, udf)
    val array = buildArray(arg._1, _, _, udf)
    arg._2 match {
      case "byte"         => atomic(ByteType, BYTE_MAP_AGG)
      case "short"        => atomic(ShortType, SHORT_MAP_AGG)
      case "int"          => atomic(IntegerType, INT_MAP_AGG)
      case "long"         => atomic(LongType, LONG_MAP_AGG)
      case "float"        => atomic(FloatType, FLOAT_MAP_AGG)
      case "double"       => atomic(DoubleType, DOUBLE_MAP_AGG)
      case "string"       => atomic(StringType, STRING_MAP_AGG)
      case "byte_array"   => array(ByteType, BYTE_ARRAY_MAP_AGG)
      case "short_array"  => array(ShortType, SHORT_ARRAY_MAP_AGG)
      case "int_array"    => array(IntegerType, INT_ARRAY_MAP_AGG)
      case "long_array"   => array(LongType, LONG_ARRAY_MAP_AGG)
      case "float_array"  => array(FloatType, FLOAT_ARRAY_MAP_AGG)
      case "double_array" => array(DoubleType, DOUBLE_ARRAY_MAP_AGG)
      case "string_array" => array(StringType, STRING_ARRAY_MAP_AGG)
      case _              => throw new IllegalArgumentException()
    }
  }

  private def buildAtomic(
      name: String,
      ty: DataType,
      fn: UserDefinedFunction,
      udf: Boolean
  ): Aggregation = {
    new Aggregation(name, ty, if (udf) fn(_) else mapAgg _)
  }

  private def buildArray(
      name: String,
      ty: DataType,
      fn: UserDefinedFunction,
      udf: Boolean
  ): Aggregation = {
    new Aggregation(
      name,
      createArrayType(ty, false),
      if (udf) fn(_) else mapAgg _
    )
  }
}

private class Aggregation private (
    val name: String,
    private val ty: DataType,
    val agg: Column
) {

  private def this(name: String, ty: DataType, fn: Column => Column) {
    this(name, ty, fn(col(name)))
  }

  def select(columns: Seq[String]): Column = {
    if (columns.contains(name)) {
      col(name)
    } else {
      lit(None).as(name).cast(ty)
    }
  }
}
