package indi.xeno.styx.thanatos.fn.agg

import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedFunction}
import org.apache.spark.sql.functions.{collect_list, explode, map_entries, map_from_entries, udaf}
import org.apache.spark.sql.{Column, Encoder}

import scala.collection.mutable.{Map => MutMap}
import scala.collection.{Map => GenMap}

object MapAggregator {

  val BYTE_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Byte]]))

  val SHORT_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Short]]))

  val INT_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Int]]))

  val LONG_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Long]]))

  val FLOAT_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Float]]))

  val DOUBLE_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Double]]))

  val STRING_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, String]]))

  val BYTE_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[Byte]]]))

  val SHORT_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[Short]]]))

  val INT_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[Int]]]))

  val LONG_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[Long]]]))

  val FLOAT_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[Float]]]))

  val DOUBLE_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[Double]]]))

  val STRING_ARRAY_MAP_AGG: UserDefinedFunction =
    udaf(new MapAggregator(kryo[MutMap[String, Array[String]]]))

  def mapAgg(col: Column): Column = {
    map_from_entries(collect_list(explode(map_entries(col))))
  }
}

class MapAggregator[T](
    override final val bufferEncoder: Encoder[MutMap[String, T]]
) extends Aggregator[
      Option[GenMap[String, T]],
      MutMap[String, T],
      MutMap[String, T]
    ]() {

  override final def zero: MutMap[String, T] = MutMap()

  override final def reduce(
      buf: MutMap[String, T],
      elem: Option[GenMap[String, T]]
  ): MutMap[String, T] = {
    elem.foreach(buf ++= _)
    buf
  }

  override final def merge(
      first: MutMap[String, T],
      second: MutMap[String, T]
  ): MutMap[String, T] = {
    first ++= second
  }

  override final def finish(
      reduction: MutMap[String, T]
  ): MutMap[String, T] = {
    reduction
  }

  override final val outputEncoder: Encoder[MutMap[String, T]] = bufferEncoder
}
