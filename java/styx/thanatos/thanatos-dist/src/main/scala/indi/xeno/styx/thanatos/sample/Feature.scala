package indi.xeno.styx.thanatos.sample

import org.apache.spark.sql.DataFrame

case class Feature(data: Seq[DataFrame], joinCols: Seq[String])
