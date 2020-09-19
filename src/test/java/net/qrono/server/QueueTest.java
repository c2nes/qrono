package net.qrono.server;

import static java.lang.System.currentTimeMillis;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
    var workerPool = new StaticIOWorkerPool(1);
    workerPool.startAsync().awaitRunning();
    var clock = Clock.systemUTC();
    var idGenerator = new StandardIdGenerator(currentTimeMillis(), 0);
    var data = new QueueData(path, workerPool, segmentWriter);
    data.startAsync().awaitRunning();
    var workingSet = new DiskBackedWorkingSet(path, 1 << 30);
    workingSet.startAsync().awaitRunning();

    var queue = new Queue(data, idGenerator, clock, workingSet);
    queue.startAsync().awaitRunning();

    var value = ByteString.copyFromUtf8("Hello, world!");

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