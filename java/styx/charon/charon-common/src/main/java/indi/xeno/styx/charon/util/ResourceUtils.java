package indi.xeno.styx.charon.util;

import java.io.BufferedInputStream;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

public abstract class ResourceUtils {

  private ResourceUtils() {}

  public static InputStream readResource(String resource) {
    InputStream is = ResourceUtils.class.getClassLoader().getResourceAsStream(resource);
    return new BufferedInputStream(requireNonNull(is));
  }
}
