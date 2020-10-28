package indi.xeno.styx.charon.common;

import java.util.concurrent.CompletableFuture;

import static java.util.Arrays.fill;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class FutureBatch {

  private static final CompletableFuture<Void> EMPTY_FUTURE = completedFuture(null);

  private final CompletableFuture<?>[] futures;

  private int offset;

  public FutureBatch(int batch) {
    if (batch <= 1) {
      throw new IllegalArgumentException();
    }
    futures = new CompletableFuture<?>[batch];
    fill(futures, EMPTY_FUTURE);
    offset = 0;
  }

  public FutureBatch add(CompletableFuture<?> future, Runnable fn) {
    futures[offset] = future;
    offset += 1;
    if (offset == futures.length) {
      all(fn);
    }
    return this;
  }

  public CompletableFuture<Void> all(Runnable fn) {
    fn.run();
    CompletableFuture<Void> all = allOf(futures);
    fill(futures, EMPTY_FUTURE);
    futures[0] = all;
    offset = 1;
    return all;
  }
}
