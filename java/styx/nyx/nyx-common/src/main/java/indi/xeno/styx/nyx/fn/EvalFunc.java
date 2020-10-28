package indi.xeno.styx.nyx.fn;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

@FunctionalInterface
public interface EvalFunc<T> {

  T apply(DeferredObject[] args) throws HiveException;

  default <V> EvalFunc<V> andThen(Function<? super T, ? extends V> after) {
    requireNonNull(after);
    return args -> after.apply(apply(args));
  }
}
