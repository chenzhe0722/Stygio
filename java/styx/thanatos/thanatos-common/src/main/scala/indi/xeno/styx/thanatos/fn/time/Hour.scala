package indi.xeno.styx.thanatos.fn.time

import indi.xeno.styx.charon.util.TimeUtils.floorTime
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, hour, lit, udf}

import java.sql.Timestamp

object Hour {

  def floorTwelveHour(time: String): Column = {
    floorHourCol(time, 12)
  }

  def floorEightHour(time: String): Column = {
    floorHourCol(time, 8)
  }

  def floorSixHour(time: String): Column = {
    floorHourCol(time, 6)
  }

  def floorFourHour(time: String): Column = {
    floorHourCol(time, 4)
  }

  def floorThreeHour(time: String): Column = {
    floorHourCol(time, 3)
  }

  def floorTwoHour(time: String): Column = {
    floorHourCol(time, 2)
  }

  private def floorHourCol(time: String, step: Int): Column = {
    val hourCol = hour(col(time))
    hourCol - (hourCol % lit(step))
  }

  val FLOOR_TWELVE_HOUR_TIMESTAMP: UserDefinedFunction =
    udf(floorHourTimestamp(_, 12))

  val FLOOR_EIGHT_HOUR_TIMESTAMP: UserDefinedFunction =
    udf(floorHourTimestamp(_, 8))

  val FLOOR_SIX_HOUR_TIMESTAMP: UserDefinedFunction =
    udf(floorHourTimestamp(_, 6))

  val FLOOR_FOUR_HOUR_TIMESTAMP: UserDefinedFunction =
    udf(floorHourTimestamp(_, 4))

  val FLOOR_THREE_HOUR_TIMESTAMP: UserDefinedFunction =
    udf(floorHourTimestamp(_, 3))

  val FLOOR_TWO_HOUR_TIMESTAMP: UserDefinedFunction =
    udf(floorHourTimestamp(_, 2))

  private def floorHourTimestamp(
      timestamp: Option[Timestamp],
      step: Int
  ): Option[Int] = {
    timestamp.map(t => floorTime(t.toLocalDateTime.getHour, step))
  }
}
