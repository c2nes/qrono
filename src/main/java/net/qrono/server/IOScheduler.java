package net.qrono.server;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public interface IOScheduler {
  // TODO: We're allowing this to block indefinitely. Is this okay...?
  <V> CompletableFuture<V> schedule(Parameters parameters, Callable<V> operation);

  // Scheduling hints
  class Parameters {
    // None at the moment
  }
}
