package com.brewtab.queue.server;

import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.EnqueueResponse;
import com.brewtab.queue.QueueServerGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Client {
  private static final Logger log = LoggerFactory.getLogger(Client.class);

  public static void main(String[] args) throws Exception {
    var channel = NettyChannelBuilder.forAddress("localhost", 9090)
        .usePlaintext()
        .build();
    try {
      run(channel);
    } finally {
      channel.shutdown().awaitTermination(5, TimeUnit.MINUTES);
    }
  }

  private static void run(ManagedChannel channel) throws Exception {
    var executor = Executors.newCachedThreadPool();

    var concurrency = 10;
    var sync = new CyclicBarrier(concurrency + 1);
    var limit = new AtomicLong(3_000_000);
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
                .setQueue("test-queue-1")
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
}
