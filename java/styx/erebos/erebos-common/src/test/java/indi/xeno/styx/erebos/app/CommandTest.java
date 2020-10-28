package indi.xeno.styx.erebos.app;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommandTest {

  private static final CommandOptions options = new CommandOptions();

  @BeforeAll
  static void init() {
    options
        .addSingleArgOption("s", "single", "Single argument test", "str", true)
        .addListArgOption("l", "list", "List arguments test", "str", true)
        .addMapArgOption("m", "map", "Map arguments test", "str", true);
  }

  @Test
  void test() {
    String[] cli = {"-s", "test", "-l", "a", "-l", "b", "-m", "ma=a", "-m", "mb=b"};
    CommandArguments arguments = options.parse(cli);
    assertEquals(arguments.getSingleArg("s").orElseThrow(), "test");
    assertEquals(arguments.getListArg("l").size(), 2);
    assertEquals(arguments.getMapArg("m").get("mb"), "b");
  }
}
