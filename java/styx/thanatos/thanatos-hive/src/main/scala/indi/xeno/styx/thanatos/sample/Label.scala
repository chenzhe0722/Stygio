package indi.xeno.styx.thanatos.sample

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.monotonically_increasing_id

private[sample] class Label private (
    private val index: String,
    private val aggregation: Seq[Aggregation],
    val data: DataFrame
) {

  def this(
      src: DataFrame,
      index: String,
      aggregation: Seq[Aggregation]
  ) {
    this(
      index,
      aggregation,
      src.withColumn(index, monotonically_increasing_id())
    )
  }

  def agg(): DataFrame = {
    Aggregation(data, index, aggregation)
  }
}
