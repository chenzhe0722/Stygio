package indi.xeno.styx.hypnos.app

import org.apache.commons.cli.Option.UNLIMITED_VALUES
import org.apache.commons.cli.{DefaultParser, Options, Option => CliOption}

object CommandOptions {

  val VALUE_SEP: Char = '='

  private def arg(
      opt: String,
      longOpt: String,
      desc: String,
      argName: String,
      required: Boolean
  ): CliOption = {
    val option = new CliOption(opt, longOpt, true, desc)
    option.setRequired(required)
    option.setArgName(argName)
    option
  }
}

class CommandOptions private (private val options: Options) {

  private[app] def this() {
    this(new Options())
  }

  def noArg(opt: String, longOpt: String, desc: String): CommandOptions = {
    val option = new CliOption(opt, longOpt, false, desc)
    options.addOption(option)
    this
  }

  def singleArg(
      opt: String,
      longOpt: String,
      desc: String,
      argName: String,
      required: Boolean
  ): CommandOptions = {
    val option = CommandOptions.arg(opt, longOpt, desc, argName, required)
    options.addOption(option)
    this
  }

  def listArg(
      opt: String,
      longOpt: String,
      desc: String,
      argName: String,
      required: Boolean
  ): CommandOptions = {
    val option = CommandOptions.arg(opt, longOpt, desc, argName, required)
    option.setArgs(UNLIMITED_VALUES)
    options.addOption(option)
    this
  }

  def mapArg(
      opt: String,
      longOpt: String,
      desc: String,
      argName: String,
      required: Boolean
  ): CommandOptions = {
    val option = CommandOptions.arg(opt, longOpt, desc, argName, required)
    option.setArgs(UNLIMITED_VALUES)
    option.setValueSeparator(CommandOptions.VALUE_SEP)
    options.addOption(option)
    this
  }

  private[app] def parse(args: Array[String]): CommandArguments = {
    val parser = new DefaultParser()
    new CommandArguments(parser.parse(options, args))
  }
}
