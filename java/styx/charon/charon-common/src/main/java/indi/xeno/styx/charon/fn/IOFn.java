package indi.xeno.styx.charon.fn;

import java.io.IOException;
import java.util.function.Function;

@FunctionalInterface
public interface IOFn<T, R> {

  R apply(T t) throws IOException;

  default <V> IOFn<T, V> andThen(Function<? super R, ? extends V> after) {
    return t -> after.apply(apply(t));
  }
}
