package indi.xeno.styx.thanatos.sample

import indi.xeno.styx.thanatos.fn.Common.cnt
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, ceil, coalesce, col, explode, floor, lit, monotonically_increasing_id, rand, udf}

import scala.Array.range

object Joiner {

  def apply(
    label: DataFrame,
    feats: Seq[Feature],
    aggregations: Seq[Aggregation],
    index: String,
    suffix: String,
    limit: Long,
  ): DataFrame = {
    val indexer = label.withColumn(index, monotonically_increasing_id())
    val init =
      expand(select(indexer, aggregations, Seq(index)), aggregations, index)
    feats.view
      .map(join(indexer, _, aggregations, index, suffix, limit))
      .foldLeft(init)(_ union _)
      .groupBy(col(index))
      .agg(col(index), aggregations.view.map(_.aggregate()): _*)
  }

  private def join(
    label: DataFrame,
    feat: Feature,
    aggregations: Seq[Aggregation],
    index: String,
    suffix: String,
    limit: Long,
  ): DataFrame = {
    val indexer = label.select((feat.joinCols.view :+ index).map(col): _*)
    val counter = broadcast(indexer
      .groupBy(feat.joinCols.view.map(col): _*)
      .agg(cnt().as(suffix), feat.joinCols.view.map(col): _*)
      .where(col(suffix) > lit(limit))
      .withColumn(suffix, ceil(col(suffix) / lit(limit))))
    val randLabel = indexer
      .join(counter, feat.joinCols, "left")
      .withColumn(suffix, floor(rand() * coalesce(col(suffix), lit(1L))))
    feat.data.view
      .map(select(_, aggregations, feat.joinCols))
      .map(_.join(counter, feat.joinCols, "left"))
      .map(_.withColumn(suffix, explode(RANGE_UDF(coalesce(col(suffix), lit(1L))))))
      .map(_.join(randLabel, feat.joinCols :+ suffix))
      .map(expand(_, aggregations, index))
      .reduce(_ union _)
  }

  private val RANGE_UDF = udf(range(0, _: Int)).asNonNullable()

  private def select(
    df: DataFrame,
    aggregations: Seq[Aggregation],
    other: Seq[String],
  ): DataFrame = {
    val agg = aggregations.view
      .map(_.name)
      .filter(df.columns.contains)
    df.select((agg ++ other.view).distinct.map(col): _*)
  }

  private def expand(
    df: DataFrame,
    aggregations: Seq[Aggregation],
    index: String,
  ): DataFrame = {
    val select = aggregations.view
      .map(agg =>
        if (df.columns.contains(agg.name)) {
          col(agg.name)
        } else {
          lit(None).as(agg.name).cast(agg.ty)
        })
    df.select(select :+ col(index): _*)
  }
}
