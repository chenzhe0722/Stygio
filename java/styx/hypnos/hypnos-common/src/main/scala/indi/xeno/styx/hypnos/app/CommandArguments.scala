package indi.xeno.styx.hypnos.app

import org.apache.commons.cli.CommandLine

import java.util.Objects.nonNull
import scala.collection.JavaConverters._

class CommandArguments private[app] (private val cli: CommandLine) {

  def hasArg(opt: String): Boolean = {
    cli.hasOption(opt)
  }

  def getSingleArg(opt: String): Option[String] = {
    Some(cli.getOptionValue(opt)).filter(nonNull)
  }

  def getListArg(opt: String): Seq[String] = {
    cli.getOptionValues(opt)
  }

  def getMapArg(opt: String): Map[String, String] = {
    val props = cli.getOptionProperties(opt)
    props
      .stringPropertyNames()
      .asScala
      .view
      .map(k => (k, props.getProperty(k)))
      .toMap
  }
}
