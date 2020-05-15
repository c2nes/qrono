package com.brewtab.queue.server.grpc;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static com.google.protobuf.util.Timestamps.toMillis;

import com.brewtab.queue.Api.CompactQueueRequest;
import com.brewtab.queue.Api.DequeueRequest;
import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.EnqueueResponse;
import com.brewtab.queue.Api.GetQueueInfoRequest;
import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.QueueInfo;
import com.brewtab.queue.Api.ReleaseRequest;
import com.brewtab.queue.Api.RequeueRequest;
import com.brewtab.queue.Api.RequeueResponse;
import com.brewtab.queue.Api.Stats;
import com.brewtab.queue.QueueServerGrpc;
import com.brewtab.queue.server.Queue;
import com.brewtab.queue.server.QueueService;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueServerService extends QueueServerGrpc.QueueServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(QueueServerService.class);
  private final QueueService svc;

  public QueueServerService(QueueService svc) {
    this.svc = svc;
  }

  private synchronized Queue getQueue(String queueName) {
    Queue queue = svc.getQueue(queueName);
    if (queue == null) {
      throw Status.NOT_FOUND
          .withDescription("no such queue, " + queueName)
          .asRuntimeException();
    }
    return queue;
  }

  @Override
  public void enqueue(EnqueueRequest request, StreamObserver<EnqueueResponse> responseObserver) {
    process(responseObserver, () -> enqueue(request));
  }

  @VisibleForTesting
  EnqueueResponse enqueue(EnqueueRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = svc.getOrCreateQueue(queueName);
    var item = queue.enqueue(
        request.getValue(),
        request.hasDeadline() ? ImmutableTimestamp.of(toMillis(request.getDeadline())) : null);

    return EnqueueResponse.newBuilder()
        .setId(item.id())
        .setDeadline(fromMillis(item.deadline().millis()))
        .build();
  }

  @Override
  public StreamObserver<EnqueueRequest> enqueueStream(
      StreamObserver<EnqueueResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(EnqueueRequest request) {
        try {
          responseObserver.onNext(enqueue(request));
        } catch (IOException e) {
          logger.error("Unhandled exception", e);
          responseObserver.onError(e);
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.error("Unhandled exception", t);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void dequeue(DequeueRequest request, StreamObserver<Item> responseObserver) {
    process(responseObserver, () -> dequeue(request));
  }

  @VisibleForTesting
  Item dequeue(DequeueRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = getQueue(queueName);

    var item = queue.dequeue();
    if (item == null) {
      throw Status.NOT_FOUND
          .withDescription("no items ready")
          .asRuntimeException();
    }

    // Convert to API model
    return Item.newBuilder()
        .setDeadline(fromMillis(item.deadline().millis()))
        .setId(item.id())
        .setStats(Stats.newBuilder()
            .setDequeueCount(item.stats().dequeueCount())
            .setEnqueueTime(fromMillis(item.stats().enqueueTime().millis()))
            .setRequeueTime(fromMillis(item.stats().requeueTime().millis()))
            .build())
        .setValue(item.value())
        .build();
  }

  @Override
  public void release(ReleaseRequest request, StreamObserver<Empty> responseObserver) {
    process(responseObserver, () -> release(request));
  }

  @VisibleForTesting
  Empty release(ReleaseRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = getQueue(queueName);
    queue.release(request.getId());
    return Empty.getDefaultInstance();
  }

  @Override
  public void requeue(RequeueRequest request, StreamObserver<RequeueResponse> responseObserver) {
    process(responseObserver, () -> requeue(request));
  }

  @VisibleForTesting
  RequeueResponse requeue(RequeueRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = getQueue(queueName);
    var deadline = request.hasDeadline()
        ? ImmutableTimestamp.of(toMillis(request.getDeadline()))
        : null;
    // TODO: Return updated deadline
    var newDeadlineMillis = queue.requeue(request.getId(), deadline).millis();
    return RequeueResponse.newBuilder().build();
  }

  @Override
  public void getQueueInfo(
      GetQueueInfoRequest request,
      StreamObserver<QueueInfo> responseObserver
  ) {
    process(responseObserver, () -> getQueueInfo(request));
  }

  @VisibleForTesting
  QueueInfo getQueueInfo(GetQueueInfoRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = getQueue(queueName);
    var info = queue.getQueueInfo();
    return QueueInfo.newBuilder()
        .setName(queueName)
        .setPending(info.pendingCount())
        .setDequeued(info.dequeuedCount())
        .build();
  }

  @Override
  public void compactQueue(CompactQueueRequest request, StreamObserver<Empty> responseObserver) {
    process(responseObserver, () -> compactQueue(request));
  }

  Empty compactQueue(CompactQueueRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = getQueue(queueName);
    queue.runTestCompaction();
    return Empty.getDefaultInstance();
  }

  private static <R> void process(StreamObserver<R> observer, Callable<R> operation) {
    try {
      observer.onNext(operation.call());
      observer.onCompleted();
    } catch (StatusException | StatusRuntimeException e) {
      observer.onError(e);
    } catch (Exception e) {
      logger.error("Unhandled error", e);
      observer.onError(e);
    }
  }
}
