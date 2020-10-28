package indi.xeno.styx.thanatos.fn

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{count, lit}

object Common {

  def one(): Column = lit(1)

  def cnt(): Column = count(one())
}
