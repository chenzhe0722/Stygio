package indi.xeno.styx.thanatos.fn.time

import indi.xeno.styx.charon.util.TimeUtils.floorTime
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, minute, udf}

import java.sql.Timestamp

object Minute {

  def floorThirtyMinute(time: String): Column = {
    floorMinuteCol(time, 30)
  }

  def floorTwentyMinute(time: String): Column = {
    floorMinuteCol(time, 20)
  }

  def floorFifteenMinute(time: String): Column = {
    floorMinuteCol(time, 15)
  }

  def floorTenMinute(time: String): Column = {
    floorMinuteCol(time, 10)
  }

  def floorFiveMinute(time: String): Column = {
    floorMinuteCol(time, 5)
  }

  private def floorMinuteCol(time: String, step: Int): Column = {
    val minuteCol = minute(col(time))
    minuteCol - (minuteCol % lit(step))
  }

  val FLOOR_THIRTY_MINUTE_TIMESTAMP: UserDefinedFunction =
    udf(floorMinuteTimestamp(_, 30))

  val FLOOR_TWENTY_MINUTE_TIMESTAMP: UserDefinedFunction =
    udf(floorMinuteTimestamp(_, 20))

  val FLOOR_FIFTEEN_MINUTE_TIMESTAMP: UserDefinedFunction =
    udf(floorMinuteTimestamp(_, 15))

  val FLOOR_TEN_MINUTE_TIMESTAMP: UserDefinedFunction =
    udf(floorMinuteTimestamp(_, 10))

  val FLOOR_FIVE_MINUTE_TIMESTAMP: UserDefinedFunction =
    udf(floorMinuteTimestamp(_, 5))

  private def floorMinuteTimestamp(
    timestamp: Option[Timestamp],
    step: Int,
  ): Option[Int] = {
    timestamp.map(t => floorTime(t.toLocalDateTime.getMinute, step))
  }
}
