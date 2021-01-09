package net.qrono.server.grpc;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.util.Timestamps;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import net.qrono.Api.DequeueRequest;
import net.qrono.Api.EnqueueRequest;
import net.qrono.Api.EnqueueResponse;
import net.qrono.Api.Item;
import net.qrono.Api.ReleaseRequest;
import net.qrono.Api.RequeueRequest;
import net.qrono.Api.RequeueResponse;
import net.qrono.QueueServerGrpc;
import net.qrono.server.ExecutorIOScheduler;
import net.qrono.server.IdGenerator;
import net.qrono.server.InMemoryWorkingSet;
import net.qrono.server.QueueFactory;
import net.qrono.server.QueueManager;
import net.qrono.server.SegmentFlushScheduler;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore("benchmark")
public class QueueServerServiceBenchmark {
  private static final Logger log = LoggerFactory.getLogger(QueueServerServiceBenchmark.class);

  @Rule
  public GrpcServerRule serverRule = new GrpcServerRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Ignore("live server required")
  @Test
  public void testEnqueue_liveServer() throws Exception {
    var channel = NettyChannelBuilder.forAddress("localhost", 9090)
        .usePlaintext()
        .build();

    try {
      testEnqueue_liveServer(channel);
    } finally {
      channel.shutdown().awaitTermination(5, TimeUnit.MINUTES);
    }
  }

  private void testEnqueue_liveServer(ManagedChannel channel) throws Exception {
    var executor = Executors.newCachedThreadPool();

    var concurrency = 10;
    var sync = new CyclicBarrier(concurrency + 1);
    var limit = new AtomicLong(3_000_000);

    var baseTime = Instant.now()
        .plus(Duration.ofDays(7))
        .toEpochMilli();

    // Cycle through N deadlines. This should be a worst case scenario for segment merging
    // as all segments will interleave.

    var deadlines = new CycleIterator<>(
        LongStream.range(baseTime, baseTime + 100)
            .mapToObj(Timestamps::fromMillis)
            .collect(ImmutableList.toImmutableList()));

    for (int i = 0; i < concurrency; i++) {
      executor.submit(() -> {
        var client = QueueServerGrpc.newStub(channel);
        var pipelineLength = 64;
        var completionQueue = new ArrayDeque<CompletableFuture<EnqueueResponse>>(pipelineLength);
        var responseQueue = new ArrayDeque<CompletableFuture<EnqueueResponse>>(pipelineLength);
        var requestStream = client
            .enqueueStream(new StreamObserver<>() {
              @Override
              public void onNext(EnqueueResponse value) {
                synchronized (completionQueue) {
                  completionQueue.removeFirst().complete(value);
                }
              }

              @Override
              public void onError(Throwable t) {
                synchronized (completionQueue) {
                  completionQueue.removeFirst().completeExceptionally(t);
                }
              }

              @Override
              public void onCompleted() {
                // noop
              }
            });

        try {
          sync.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          return;
        }

        log.info("Starting...");

        long n;
        long mine = 0;
        var rand = new Random();
        while ((n = limit.getAndDecrement()) > 0) {
          try {
            while (responseQueue.size() >= pipelineLength) {
              responseQueue.removeFirst().join();
            }

            var future = new CompletableFuture<EnqueueResponse>();
            synchronized (completionQueue) {
              completionQueue.addLast(future);
              responseQueue.addLast(future);
            }
            requestStream.onNext(EnqueueRequest.newBuilder()
                .setQueue("test-queue-4")
                //.setDeadline(deadlines.next())
                //.setDeadline(Timestamps.fromMillis(baseTime + rand.nextInt(1_000_000_000)))
                .setValue(ByteString.copyFromUtf8("yolo " + n))
                .build());
            mine++;
          } catch (StatusRuntimeException e) {
            log.error("Enqueue error", e);
            break;
          }
        }
        requestStream.onCompleted();
        // Wait for remaining responses
        while (!responseQueue.isEmpty()) {
          try {
            responseQueue.removeFirst().join();
          } catch (Exception e) {
            log.error("Enqueue error", e);
            break;
          }
        }
        log.info("Done! count={}", mine);
      });
    }
    sync.await();
    var start = Instant.now();
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);
    var duration = Duration.between(start, Instant.now());
    System.out.println(duration);
  }

  @Test
  public void testEnqueueThroughput() throws InterruptedException, IOException {
    IdGenerator idGenerator = new AtomicLong()::incrementAndGet;
    Path directory = temporaryFolder.getRoot().toPath();
    var ioScheduler = new ExecutorIOScheduler(newSingleThreadExecutor());
    var workingSet = new InMemoryWorkingSet();
    var segmentFlushScheduler = new SegmentFlushScheduler(100 * 1024 * 1024);
    var queueFactory = new QueueFactory(
        directory,
        idGenerator,
        ioScheduler,
        workingSet,
        segmentFlushScheduler);
    var queueManager = new QueueManager(directory, queueFactory);
    queueManager.startAsync().awaitRunning();
    QueueServerService service = new QueueServerService(queueManager);

    var value = ByteString.copyFromUtf8(Strings.repeat("0", 256));
    var n = 1_000_000;
    Instant start = Instant.now();
    for (int i = 0; i < n; i++) {
      EnqueueResponse resp = service.enqueue(EnqueueRequest.newBuilder()
          .setQueue("test-queue")
          .setValue(value)
          .build());
      // System.out.println(resp.getDeadline() + " / " + resp.getId());
    }

    Instant end = Instant.now();
    Duration duration = Duration.between(start, end);
    System.out.println(duration.toMillis());
    var seconds = 1e-9 * duration.toNanos();
    System.out.println((long) (n / seconds));
  }

  @Test
  public void testEnqueueDequeueRelease() throws IOException, InterruptedException {
    IdGenerator idGenerator = new AtomicLong()::incrementAndGet;
    Path directory = temporaryFolder.getRoot().toPath();
    var ioScheduler = new ExecutorIOScheduler(newSingleThreadExecutor());
    var workingSet = new InMemoryWorkingSet();
    var segmentFlushScheduler = new SegmentFlushScheduler(100 * 1024 * 1024);
    var queueFactory = new QueueFactory(
        directory,
        idGenerator,
        ioScheduler,
        workingSet,
        segmentFlushScheduler);
    var queueManager = new QueueManager(directory, queueFactory);
    queueManager.startAsync().awaitRunning();
    QueueServerService service = new QueueServerService(queueManager);

    var value = ByteString.copyFromUtf8("Hello, world!");
    EnqueueResponse resp = service.enqueue(EnqueueRequest.newBuilder()
        .setQueue("test-queue")
        .setValue(value)
        .build());
    System.out.println("Enqueue: " + resp);

    Item item = service.dequeue(DequeueRequest.newBuilder()
        .setQueue("test-queue")
        .build());
    System.out.println("Dequeue: " + item);

    Thread.sleep(1000);

    RequeueResponse requeue = service.requeue(RequeueRequest.newBuilder()
        .setQueue("test-queue")
        .setId(item.getId())
        .build());
    System.out.println("Requeue: " + requeue);

    item = service.dequeue(DequeueRequest.newBuilder()
        .setQueue("test-queue")
        .build());
    System.out.println("Dequeue: " + item);

    Empty release = service.release(ReleaseRequest.newBuilder()
        .setQueue("test-queue")
        .setId(item.getId())
        .build());
    System.out.println("Release: " + release);

    // Double release should be rejected
    try {
      service.release(ReleaseRequest.newBuilder()
          .setQueue("test-queue")
          .setId(item.getId())
          .build());
      fail("FAILED_PRECONDITION expected");
    } catch (StatusRuntimeException e) {
      assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }
  }

  @Test
  public void testEnqueueDequeueReleaseMany() throws IOException {
    IdGenerator idGenerator = new AtomicLong()::incrementAndGet;
    Path directory = temporaryFolder.getRoot().toPath();
    var ioScheduler = new ExecutorIOScheduler(newSingleThreadExecutor());
    var workingSet = new InMemoryWorkingSet();
    var segmentFlushScheduler = new SegmentFlushScheduler(100 * 1024 * 1024);
    var queueFactory = new QueueFactory(
        directory,
        idGenerator,
        ioScheduler,
        workingSet,
        segmentFlushScheduler);
    var queueManager = new QueueManager(directory, queueFactory);
    queueManager.startAsync().awaitRunning();
    QueueServerService service = new QueueServerService(queueManager);
    var queueName = "test-queue-" + System.currentTimeMillis();

    var start = Instant.now();
    var n = 1_000_000;
    for (int i = 0; i < n; i++) {
      var value = ByteString.copyFromUtf8("Hello, world! " + i);
      service.enqueue(EnqueueRequest.newBuilder()
          .setQueue(queueName)
          .setValue(value)
          .build());
    }

    var dequeued = 0;
    try {
      while (true) {
        Item item = service.dequeue(DequeueRequest.newBuilder()
            .setQueue(queueName)
            .build());

        dequeued++;
        service.release(ReleaseRequest.newBuilder()
            .setQueue(queueName)
            .setId(item.getId())
            .build());
      }
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() != Code.NOT_FOUND) {
        throw e;
      }
    }

    System.out.println("Dequeue: " + dequeued);
    var end = Instant.now();
    var duration = Duration.between(start, end);
    System.out.println(duration.toMillis());
    var seconds = 1e-9 * duration.toNanos();
    System.out.println((long) (n / seconds));
  }

  static class CycleIterator<E> implements Iterator<E> {
    private final ImmutableList<E> values;
    private final AtomicInteger idx = new AtomicInteger();

    CycleIterator(ImmutableList<E> values) {
      this.values = values;
    }

    @Override
    public boolean hasNext() {
      return !values.isEmpty();
    }

    @Override
    public E next() {
      return values.get(idx.getAndUpdate(i -> (i + 1) % values.size()));
    }
  }
}