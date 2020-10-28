package indi.xeno.styx.thanatos.sample

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataType

case class Aggregation(name: String, ty: DataType, fn: Column => Column) {

  def aggregate(): Column = {
    fn(col(name))
  }
}
