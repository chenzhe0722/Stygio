package indi.xeno.styx.thanatos.sql

import indi.xeno.styx.charon.util.StrUtils.{COLON, COMMA}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

object Partition {

  def wherePartition(
      df: DataFrame,
      partitions: Traversable[Partition]
  ): DataFrame = {
    partitions
      .filter(p => df.columns.contains(p.col))
      .foldLeft(df)((d, p) => d.where(col(p.col) === lit(p.value)))
  }

  def withPartition(
      df: DataFrame,
      partitions: Traversable[Partition]
  ): DataFrame = {
    partitions.foldLeft(df)((d, p) => d.withColumn(p.col, lit(p.value)))
  }

  def getPartition(text: String): Array[Partition] = {
    text.split(COMMA).map(getSinglePartition)
  }

  private def getSinglePartition(text: String): Partition = {
    val i = text.indexOf(COLON)
    if (i < 0) {
      Partition(text.substring(0, i), text.substring(i + 1))
    } else {
      throw new IllegalArgumentException()
    }
  }
}

case class Partition(col: String, value: String) {}
