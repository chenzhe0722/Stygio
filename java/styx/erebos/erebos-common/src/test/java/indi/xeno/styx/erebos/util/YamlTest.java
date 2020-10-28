package indi.xeno.styx.erebos.util;

import indi.xeno.styx.erebos.domain.Person;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static indi.xeno.styx.charon.util.ResourceUtils.readResource;
import static indi.xeno.styx.erebos.util.YamlUtils.readYaml;
import static org.junit.jupiter.api.Assertions.assertEquals;

class YamlTest {

  @Test
  void test() throws IOException {
    Person person = readYaml(readResource("person.yml"), Person.class);
    assertEquals(person.getEmail().size(), 2);
    assertEquals(person.getAddress().getCity(), "New York");
  }
}
