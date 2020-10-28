package indi.xeno.styx.erebos.app;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;

import static org.apache.commons.cli.Option.UNLIMITED_VALUES;

public class CommandOptions {

  private final Options options;

  public static final char VALUE_SEP = '=';

  CommandOptions() {
    options = new Options();
  }

  public CommandOptions noArg(String opt, String longOpt, String desc) {
    Option option = new Option(opt, longOpt, false, desc);
    options.addOption(option);
    return this;
  }

  public CommandOptions singleArg(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = arg(opt, longOpt, desc, argName, required);
    options.addOption(option);
    return this;
  }

  public CommandOptions listArg(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = arg(opt, longOpt, desc, argName, required);
    option.setArgs(UNLIMITED_VALUES);
    options.addOption(option);
    return this;
  }

  public CommandOptions mapArg(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = arg(opt, longOpt, desc, argName, required);
    option.setArgs(UNLIMITED_VALUES);
    option.setValueSeparator(VALUE_SEP);
    options.addOption(option);
    return this;
  }

  private static Option arg(
      String opt, String longOpt, String desc, String argName, boolean required) {
    Option option = new Option(opt, longOpt, true, desc);
    option.setRequired(required);
    option.setArgName(argName);
    return option;
  }

  CommandArguments parse(String[] args) {
    Parser parser = new BasicParser();
    try {
      return new CommandArguments(parser.parse(options, args));
    } catch (ParseException ex) {
      throw new IllegalArgumentException(ex);
    }
  }
}
