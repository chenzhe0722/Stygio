package indi.xeno.styx.charon.util;

import indi.xeno.styx.charon.fn.IOFn;

import java.io.IOException;
import java.io.InputStream;

public abstract class IOUtils {

  private IOUtils() {}

  public static <T> T closeInput(IOFn<InputStream, T> fn, InputStream is) throws IOException {
    T t = fn.apply(is);
    is.close();
    return t;
  }
}
