package indi.xeno.styx.erebos.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Address {

  private final String country;

  private final String city;

  public Address(@JsonProperty("country") String country, @JsonProperty("city") String city) {
    this.country = country;
    this.city = city;
  }

  public String getCountry() {
    return country;
  }

  public String getCity() {
    return city;
  }
}
