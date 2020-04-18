package com.brewtab.queue.server;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterPoolSegmentFreezer extends AbstractIdleService implements SegmentFreezer {
  private static final Logger log = LoggerFactory.getLogger(WriterPoolSegmentFreezer.class);
  private final SynchronousQueue<FreezeRequest> channel = new SynchronousQueue<>(true);
  private final int numWriters;

  private ExecutorService executor;

  public WriterPoolSegmentFreezer(int numWriters) {
    this.numWriters = numWriters;
  }

  @Override
  public void freeze(WritableSegment segment) throws InterruptedException {
    Preconditions.checkState(isRunning(), "not running");
    var request = new FreezeRequest(segment);
    channel.put(request);
  }

  @Override
  public void freezeUninterruptibly(WritableSegment segment) {
    Preconditions.checkState(isRunning(), "not running");
    var request = new FreezeRequest(segment);
    Uninterruptibles.putUninterruptibly(channel, request);
  }

  private void processOne() throws InterruptedException, IOException {
    var request = channel.take();
    var start = Instant.now();
    request.segment.freeze();
    var duration = Duration.between(start, Instant.now());
    log.debug("Segment freeze completed; duration={}ms", duration.toMillis());
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
        log.error("Unhandled exception while freezing segment", e);
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
        log.info("Waiting for in-progress segment freezes to complete...");
      }
    }
  }

  private static class FreezeRequest {
    private final WritableSegment segment;

    private FreezeRequest(WritableSegment segment) {
      this.segment = segment;
    }
  }
}
