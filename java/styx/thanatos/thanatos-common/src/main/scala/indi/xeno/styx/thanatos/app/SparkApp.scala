package indi.xeno.styx.thanatos.app

import indi.xeno.styx.erebos.app.CommandApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.builder

private object SparkApp {

  private val SPARK = builder().enableHiveSupport().getOrCreate()
}

abstract class SparkApp() extends CommandApp() {

  protected final val spark: SparkSession = SparkApp.SPARK
}
