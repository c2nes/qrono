package com.brewtab.queue.server;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Uninterruptibles;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticIOWorkerPool extends AbstractIdleService implements IOScheduler {
  private static final Logger log = LoggerFactory.getLogger(StaticIOWorkerPool.class);
  private final SynchronousQueue<Request<?>> channel = new SynchronousQueue<>(true);
  private final int numWriters;

  private ExecutorService executor;

  public StaticIOWorkerPool(int numWriters) {
    this.numWriters = numWriters;
  }

  @Override
  public <V> CompletableFuture<V> schedule(Parameters parameters, Callable<V> operation) {
    var request = new Request<>(operation);
    Uninterruptibles.putUninterruptibly(channel, request);
    return request.future;
  }

  private void processOne() throws Exception {
    var request = channel.take();
    var start = Instant.now();
    // TODO: We're letting exceptions bubble up and we're not completing the future
    request.execute();
    var duration = Duration.between(start, Instant.now());
    log.debug("IO operation completed; duration={}ms", duration.toMillis());
  }

  private void process() {
    while (isRunning()) {
      try {
        processOne();
      } catch (InterruptedException e) {
        if (isRunning()) {
          log.warn("Ignoring InterruptedException while running");
        }
      } catch (Exception e) {
        // TODO: Need to do something here...maybe just kill the process?
        log.error("Unhandled exception while executing IO", e);
      }
    }
  }

  @Override
  protected void startUp() throws Exception {
    executor = Executors.newFixedThreadPool(numWriters);
    for (int i = 0; i < numWriters; i++) {
      executor.submit(this::process);
    }
  }

  @Override
  protected void shutDown() throws Exception {
    executor.shutdownNow();
    while (executor.isTerminated()) {
      executor.awaitTermination(5, TimeUnit.SECONDS);
      if (!executor.isTerminated()) {
        log.info("Waiting for in-progress IO operations to complete...");
      }
    }
  }

  private static class Request<V> {
    private final CompletableFuture<V> future = new CompletableFuture<>();
    private final Callable<V> operation;

    private Request(Callable<V> operation) {
      this.operation = operation;
    }

    void execute() throws Exception {
      future.complete(operation.call());
    }
  }
}
