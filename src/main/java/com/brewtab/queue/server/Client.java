package com.brewtab.queue.server;

import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.EnqueueResponse;
import com.brewtab.queue.QueueServerGrpc;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
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
                .setQueue("test-queue-3")
                //.setDeadline(deadlines.next())
                .setDeadline(Timestamps.fromMillis(baseTime + rand.nextInt(1_000_000_000)))
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
