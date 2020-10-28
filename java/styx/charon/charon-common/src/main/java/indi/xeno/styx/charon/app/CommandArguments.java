package indi.xeno.styx.charon.app;

import org.apache.commons.cli.CommandLine;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class CommandArguments {

  private final CommandLine cli;

  CommandArguments(CommandLine cli) {
    this.cli = cli;
  }

  public boolean hasOption(String opt) {
    return cli.hasOption(opt);
  }

  public Optional<String> getSingleArg(String opt) {
    return ofNullable(cli.getOptionValue(opt));
  }

  public List<String> getListArg(String opt) {
    return asList(cli.getOptionValues(opt));
  }

  public Map<String, String> getMapArg(String opt) {
    Properties props = cli.getOptionProperties(opt);
    return props.stringPropertyNames().stream().collect(toMap(identity(), props::getProperty));
  }
}
