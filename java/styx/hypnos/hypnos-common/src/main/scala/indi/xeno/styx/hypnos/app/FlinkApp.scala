package indi.xeno.styx.hypnos.app

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment.getExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment.create

object FlinkApp {

  private val ENV = getExecutionEnvironment

  private val TABLE_ENV = create(ENV)

  def run(app: FlinkApp, args: Array[String]): Unit = {
    val opts = new CommandOptions()
    app.initCommand(opts)
    app.run(opts.parse(args))
  }
}

abstract class FlinkApp() {

  protected final val env: StreamExecutionEnvironment = FlinkApp.ENV

  protected final val tblEnv: StreamTableEnvironment = FlinkApp.TABLE_ENV

  protected def initCommand(opts: CommandOptions): Unit

  protected def run(args: CommandArguments): Unit
}
