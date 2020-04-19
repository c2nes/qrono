package com.brewtab.queue.server;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public interface IOScheduler {
  // TODO: We're allowing this to block indefinitely. Is this okay...?
  <V> CompletableFuture<V> schedule(Callable<V> operation, Parameters parameters);

  // Scheduling hints
  class Parameters {
    // None at the moment
  }
}
