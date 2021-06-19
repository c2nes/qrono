package net.qrono.server;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueueTest {
  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  @Ignore
  @Test
  public void benchmarkEnqueue() throws IOException {
    var path = dir.getRoot().toPath();
    var segmentWriter = new StandardSegmentWriter(path);
    var workerPool = new ExecutorTaskScheduler(newSingleThreadExecutor());
    var clock = Clock.systemUTC();
    IdGenerator idGenerator = new AtomicLong()::incrementAndGet;
    var segmentFlushScheduler = new SegmentFlushScheduler(100 * 1024 * 1024);
    var data = new QueueData(path, workerPool, segmentWriter, segmentFlushScheduler);
    data.startAsync().awaitRunning();
    var workingSet = new DiskBackedWorkingSet(path, 1 << 30, workerPool);
    workingSet.startAsync().awaitRunning();

    var queue = new Queue(data, idGenerator, clock, workingSet, workerPool);
    queue.startAsync().awaitRunning();

    var value = Unpooled.copiedBuffer("Hello, world!", CharsetUtil.UTF_8);

    for (var n : List.of(5_000_000, 10_000_000, 10_000_000)) {
      var start = Instant.now();
      int outstandingLimit = 100_000;
      var futures = new ArrayDeque<CompletableFuture<?>>(outstandingLimit);
      for (int i = 0; i < n; i++) {
        futures.add(queue.enqueueAsync(value, null));

        while (true) {
          var future = futures.peek();
          if (future == null || !future.isDone()) {
            break;
          }
          future.join();
          futures.poll();
        }

        if (futures.size() >= outstandingLimit) {
          futures.poll().join();
        }
      }
      futures.forEach(CompletableFuture::join);

      var duration = Duration.between(start, Instant.now());
      System.out.println(1e3 * n / duration.toMillis());
      System.out.println(queue.getQueueInfo());
    }
  }
}