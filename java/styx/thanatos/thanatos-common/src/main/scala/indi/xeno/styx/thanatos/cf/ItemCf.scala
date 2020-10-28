package indi.xeno.styx.thanatos.cf

import indi.xeno.styx.charon.util.StrUtils.UNDERLINE
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col, lit, pow, sqrt, sum, when}

import java.lang.String.join
import scala.Array.empty

class ItemCf private (
    private val item: String,
    private val user: String,
    private val score: String,
    private val norm: String,
    private val similarity: String,
    private val itemSuffix: String,
    private val scoreSuffix: String,
    private val normSuffix: String
) {

  def this(
      item: String,
      user: String,
      score: String,
      norm: String,
      similarity: String,
      suffix: String
  ) {
    this(
      item,
      user,
      score,
      norm,
      similarity,
      join(UNDERLINE, item, suffix),
      join(UNDERLINE, score, suffix),
      join(UNDERLINE, norm, suffix)
    )
  }

  def run(src: DataFrame): DataFrame = {
    val data = src.select(item, user, score)
    val agg = data
      .groupBy(item)
      .agg(sum(pow(score, 2d)).as(norm), empty: _*)
      .withColumn(norm, sqrt(norm))
      .withColumn(norm, when(col(norm) =!= lit(0d), col(norm)))
    val aggCross = agg
      .withColumnRenamed(item, itemSuffix)
      .withColumnRenamed(norm, normSuffix)
    val crossNorm = agg
      .join(aggCross, agg(item) < aggCross(itemSuffix))
      .withColumn(norm, col(norm) * col(normSuffix))
      .drop(normSuffix)
    val crossDot = data
      .withColumnRenamed(item, itemSuffix)
      .withColumnRenamed(score, scoreSuffix)
      .join(data, Seq(user))
      .where(col(item) < col(itemSuffix))
      .withColumn(score, col(score) * col(scoreSuffix))
      .groupBy(item, itemSuffix)
      .agg(sum(score).as(score), empty: _*)
    crossNorm
      .join(crossDot, Seq(item, itemSuffix), "left")
      .withColumn(similarity, coalesce(col(score) / col(norm), lit(0d)))
      .drop(score, norm)
  }
}
