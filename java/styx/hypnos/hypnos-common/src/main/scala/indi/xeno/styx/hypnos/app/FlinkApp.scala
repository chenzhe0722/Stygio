package indi.xeno.styx.hypnos.app

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.getExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment.create

object FlinkApp {

  private val ENV = getExecutionEnvironment

  private val TABLE_ENV = create(ENV)
}

abstract class FlinkApp() {

  protected val env: StreamExecutionEnvironment = FlinkApp.ENV

  protected val tbl_env: StreamTableEnvironment = FlinkApp.TABLE_ENV
}
