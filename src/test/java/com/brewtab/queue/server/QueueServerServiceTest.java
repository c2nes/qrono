package com.brewtab.queue.server;

import com.brewtab.queue.Api.DequeueRequest;
import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.EnqueueResponse;
import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.ReleaseRequest;
import com.brewtab.queue.Api.RequeueRequest;
import com.brewtab.queue.Api.RequeueResponse;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.testing.GrpcServerRule;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class QueueServerServiceTest {
  @Rule
  public GrpcServerRule serverRule = new GrpcServerRule();

  @Test
  public void testEnqueueThroughput() throws InterruptedException, IOException {
    IdGeneratorImpl idGenerator = new IdGeneratorImpl(System.currentTimeMillis());
    Path directory = Path.of("/tmp/queue-server-test");
    var ioScheduler = new StaticIOWorkerPool(1);
    ioScheduler.startAsync().awaitRunning();
    var queueFactory = new QueueFactory(directory, idGenerator, ioScheduler);
    QueueServerService service = new QueueServerService(queueFactory);

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
    IdGeneratorImpl idGenerator = new IdGeneratorImpl(System.currentTimeMillis());
    Path directory = Path.of("/tmp/queue-server-test");
    var ioScheduler = new StaticIOWorkerPool(1);
    ioScheduler.startAsync().awaitRunning();
    var queueFactory = new QueueFactory(directory, idGenerator, ioScheduler);
    QueueServerService service = new QueueServerService(queueFactory);

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
    service.release(ReleaseRequest.newBuilder()
        .setQueue("test-queue")
        .setId(item.getId())
        .build());
  }

  @Test
  public void testEnqueueDequeueReleaseMany() throws IOException, InterruptedException {
    IdGeneratorImpl idGenerator = new IdGeneratorImpl(System.currentTimeMillis());
    Path directory = Path.of("/tmp/queue-server-test");
    var ioScheduler = new StaticIOWorkerPool(1);
    ioScheduler.startAsync().awaitRunning();
    var queueFactory = new QueueFactory(directory, idGenerator, ioScheduler);
    QueueServerService service = new QueueServerService(queueFactory);
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
}