package indi.xeno.styx.thanatos.fn.time

import indi.xeno.styx.charon.util.TimeUtils.floorYear
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf, when, year}

import java.sql.Timestamp

object Year {

  def floorIntervalYear(time: String, step: Int, offset: Int): Column = {
    val yearCol = year(col(time))
    val stepCol = lit(step)
    val offsetCol = lit(offset)
    val rem = yearCol % lit(step)
    val base = yearCol - rem + offsetCol
    when(rem >= offsetCol, base).otherwise(base - stepCol)
  }

  val FLOOR_INTERVAL_YEAR_TIMESTAMP: UserDefinedFunction =
    udf(floorYearTimestamp _)

  private def floorYearTimestamp(
      timestamp: Option[Timestamp],
      step: Int,
      offset: Int
  ): Option[Int] = {
    timestamp.map(t => floorYear(t.toLocalDateTime.getYear, step, offset))
  }
}
