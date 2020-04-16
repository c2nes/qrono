package com.brewtab.queue.server;

import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.EnqueueResponse;
import com.brewtab.queue.QueueServerGrpc;
import com.brewtab.queue.QueueServerGrpc.QueueServerBlockingStub;
import com.brewtab.queue.QueueServerGrpc.QueueServerStub;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcServerRule;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TransferQueue;
import org.junit.Rule;
import org.junit.Test;

public class QueueServerServiceTest {
  @Rule
  public GrpcServerRule serverRule = new GrpcServerRule();

  @Test
  public void test() throws InterruptedException {
    IdGeneratorImpl idGenerator = new IdGeneratorImpl(System.currentTimeMillis());
    Path directory = Path.of("/tmp/queue-server-test");
    QueueServerService service = new QueueServerService(idGenerator, directory);
    serverRule.getServiceRegistry().addService(service);

    QueueServerBlockingStub stub = QueueServerGrpc.newBlockingStub(serverRule.getChannel());
    QueueServerStub asyncStub = QueueServerGrpc.newStub(serverRule.getChannel());

    var respQueue = new SynchronousQueue<EnqueueResponse>();
    var stream = asyncStub.enqueueStream(new StreamObserver<>() {
      @Override
      public void onNext(EnqueueResponse resp) {
        Uninterruptibles.putUninterruptibly(respQueue, resp);
      }

      @Override
      public void onError(Throwable t) {
        t.printStackTrace(System.err);
      }

      @Override
      public void onCompleted() {
        System.out.println("done");
      }
    });

    Instant start = Instant.now();
    for (int i = 0; i < 1_000_000; i++) {
      stream.onNext(EnqueueRequest.newBuilder()
          .setQueue("test-queue-1")
          .setValue(ByteString.copyFromUtf8(String.format("Hello world %9d-", i)
              + Strings.repeat("0", 256)))
          .build());
      EnqueueResponse resp = respQueue.take();
      // System.out.println(resp.getDeadline() + " / " + resp.getId());
    }
    stream.onCompleted();
    Instant end = Instant.now();
    System.out.println(Duration.between(start, end).toMillis());
  }
}