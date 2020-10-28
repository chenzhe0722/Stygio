package indi.xeno.styx.erebos.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;

public class Person {

  private final String name;

  private final Integer age;

  private final Address address;

  private final Collection<String> email;

  public Person(
      @JsonProperty("name") String name,
      @JsonProperty("age") Integer age,
      @JsonProperty("address") Address address,
      @JsonProperty("email") Collection<String> email) {
    this.name = name;
    this.age = age;
    this.address = address;
    this.email = email;
  }

  public String getName() {
    return name;
  }

  public Integer getAge() {
    return age;
  }

  public Address getAddress() {
    return address;
  }

  public Collection<String> getEmail() {
    return email;
  }
}
