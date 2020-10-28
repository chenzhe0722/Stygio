package indi.xeno.styx.thanatos.sample

import org.apache.spark.sql.DataFrame

class Feature(
    val joinCols: Seq[String],
    val data: Seq[DataFrame]
) {


}
