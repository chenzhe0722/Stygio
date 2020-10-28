package indi.xeno.styx.charon.util;

import java.io.InputStream;

public abstract class ResourceUtils {

  private ResourceUtils() {}

  public static InputStream read(String resource) {
    return ResourceUtils.class.getClassLoader().getResourceAsStream(resource);
  }
}
