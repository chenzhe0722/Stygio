package indi.xeno.styx.thanatos.sample

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, ceil, coalesce, col, count, explode, floor, lit, monotonically_increasing_id, rand, udf}

import scala.Array.{empty, range}

object Joiner {

  def apply(
      label: DataFrame,
      feats: Seq[Feature],
      aggregations: Seq[Aggregation],
      index: String,
      suffix: String,
      limit: Long
  ): DataFrame = {
    val indexer = label.withColumn(index, monotonically_increasing_id())
    val init =
      expand(select(indexer, aggregations, Seq(index)), aggregations, index)
    val agg = aggregations.map(_.aggregate())
    feats
      .map(join(indexer, _, aggregations, index, suffix, limit))
      .foldLeft(init)(_ union _)
      .groupBy(col(index))
      .agg(agg.head, agg.drop(1): _*)
  }

  private val RANGE_UDF = udf(range(0, _: Int)).asNonNullable()

  private def select(
      df: DataFrame,
      aggregations: Seq[Aggregation],
      other: Seq[String]
  ): DataFrame = {
    val agg = aggregations
      .map(_.name)
      .filter(df.columns.contains)
    df.select((agg ++ other).distinct.map(col): _*)
  }

  private def expand(
      df: DataFrame,
      aggregations: Seq[Aggregation],
      index: String
  ): DataFrame = {
    val select = aggregations
      .map(agg =>
        if (df.columns.contains(agg.name)) {
          col(agg.name)
        } else {
          lit(None).as(agg.name).cast(agg.ty)
        }
      )
    df.select(select :+ col(index): _*)
  }
}

class Joiner(
    private val index: String,
    private val suffix: String,
    private val limit: Long,
    private val aggregation: Seq[Aggregation]
) {

  def run(record: DataFrame, feat: Seq[Feature]): DataFrame = {
    val label = new Label(record, index, aggregation)
    feat.map(join(label, _))
  }

  private def join(label: Label, feat: Feature): DataFrame = {
    val joinCols = feat.joinCols.map(col)
    val counter = broadcast(
      label.data
        .groupBy(joinCols: _*)
        .agg(count(lit(1)).as(suffix), empty: _*)
        .where(col(suffix) > lit(limit))
        .withColumn(suffix, ceil(col(suffix) / lit(limit)))
    )
    val labelJoiner = label.data
      .join(counter, feat.joinCols, "left")
      .withColumn(suffix, coalesce(floor(rand() * col(suffix)), lit(0L)))
    feat.data.view
      .map(_.select(joinCols: _*))
      .map(_.join(counter, feat.joinCols, "left"))
      .map(
        _.withColumn(
          suffix,
          explode(Joiner.RANGE_UDF(coalesce(col(suffix), lit(1L))))
        )
      )
      .map(_.join(randLabel, feat.joinCols :+ suffix))
      .map(expand(_, aggregations, index))
      .reduce(_ union _)
  }
}
