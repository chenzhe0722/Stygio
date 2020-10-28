package indi.xeno.styx.thanatos.fn.time

import indi.xeno.styx.charon.util.TimeUtils.floorMonth
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, month, udf}

import java.sql.Timestamp

object Month {

  def floorSixMonth(time: String): Column = {
    floorMonthCol(time, 6)
  }

  def floorThreeMonth(time: String): Column = {
    floorMonthCol(time, 3)
  }

  private def floorMonthCol(time: String, step: Int): Column = {
    val monthCol = month(col(time))
    monthCol - ((monthCol - lit(1)) % lit(step))
  }

  val FLOOR_SIX_MONTH_TIMESTAMP: UserDefinedFunction =
    udf(floorMonthTimestamp(_, 6))

  val FLOOR_THREE_MONTH_TIMESTAMP: UserDefinedFunction =
    udf(floorMonthTimestamp(_, 3))

  private def floorMonthTimestamp(
      timestamp: Option[Timestamp],
      step: Int
  ): Option[Int] = {
    timestamp.map(t => floorMonth(t.toLocalDateTime.getMonthValue, step))
  }
}
