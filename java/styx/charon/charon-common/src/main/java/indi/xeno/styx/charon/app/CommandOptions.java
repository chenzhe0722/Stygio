package indi.xeno.styx.charon.app;

import indi.xeno.styx.charon.exception.UnhandledException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

import static org.apache.commons.cli.Option.UNLIMITED_VALUES;

public class CommandOptions {

  private final Options options;

  static final char COMMAND_OPTION_VALUE_SEP = '=';

  CommandOptions() {
    options = new Options();
  }

  public CommandOptions addNoArgOption(String opt, String longOpt, String desc) {
    Option option = new Option(opt, longOpt, false, desc);
    options.addOption(option);
    return this;
  }

  public CommandOptions addSingleArgOption(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = new Option(opt, longOpt, true, desc);
    option.setRequired(required);
    option.setArgName(argName);
    options.addOption(option);
    return this;
  }

  public CommandOptions addListArgOption(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = new Option(opt, longOpt, true, desc);
    option.setRequired(required);
    option.setArgName(argName);
    option.setArgs(UNLIMITED_VALUES);
    options.addOption(option);
    return this;
  }

  public CommandOptions addMapArgOption(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = new Option(opt, longOpt, true, desc);
    option.setRequired(required);
    option.setArgName(argName);
    option.setArgs(UNLIMITED_VALUES);
    option.setValueSeparator(COMMAND_OPTION_VALUE_SEP);
    options.addOption(option);
    return this;
  }

  CommandArguments parse(String[] args) {
    Parser parser = new BasicParser();
    try {
      return new CommandArguments(parser.parse(options, args));
    } catch (ParseException ex) {
      throw new UnhandledException(ex);
    }
  }
}
