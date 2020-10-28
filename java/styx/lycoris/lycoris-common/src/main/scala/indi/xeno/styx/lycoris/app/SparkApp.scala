package indi.xeno.styx.lycoris.app

import indi.xeno.styx.charon.app.CommandApp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession.builder

private object SparkApp {

  private val spark: SparkSession = builder().enableHiveSupport().getOrCreate()
}

abstract class SparkApp extends CommandApp {

  protected val spark: SparkSession = SparkApp.spark
}
