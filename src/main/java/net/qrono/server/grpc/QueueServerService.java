package net.qrono.server.grpc;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static com.google.protobuf.util.Timestamps.toMillis;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.concurrent.Callable;
import net.qrono.Api.CompactQueueRequest;
import net.qrono.Api.DequeueRequest;
import net.qrono.Api.EnqueueRequest;
import net.qrono.Api.EnqueueResponse;
import net.qrono.Api.GetQueueInfoRequest;
import net.qrono.Api.Item;
import net.qrono.Api.PeekRequest;
import net.qrono.Api.QueueInfo;
import net.qrono.Api.ReleaseRequest;
import net.qrono.Api.RequeueRequest;
import net.qrono.Api.RequeueResponse;
import net.qrono.Api.Stats;
import net.qrono.QueueServerGrpc;
import net.qrono.server.Queue;
import net.qrono.server.QueueManager;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.exceptions.QueueNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueServerService extends QueueServerGrpc.QueueServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(QueueServerService.class);
  private final QueueManager manager;

  public QueueServerService(QueueManager manager) {
    this.manager = manager;
  }

  @Override
  public void enqueue(EnqueueRequest request, StreamObserver<EnqueueResponse> responseObserver) {
    process(responseObserver, () -> enqueue(request));
  }

  @VisibleForTesting
  EnqueueResponse enqueue(EnqueueRequest request) throws IOException {
    String queueName = request.getQueue();

    var deadline = request.hasDeadline()
        ? ImmutableTimestamp.of(toMillis(request.getDeadline()))
        : null;

    var item = manager.withQueue(
        queueName,
        queue -> queue.enqueueAsync(request.getValue(), deadline));

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

    var item = manager.withExistingQueue(queueName, Queue::dequeueAsync);
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

    try {
      manager.withExistingQueue(queueName, queue -> queue.releaseAsync(request.getId()));
    } catch (IllegalStateException e) {
      throw Status.FAILED_PRECONDITION
          .withDescription(e.getMessage())
          .asRuntimeException();
    }

    return Empty.getDefaultInstance();
  }

  @Override
  public void requeue(RequeueRequest request, StreamObserver<RequeueResponse> responseObserver) {
    process(responseObserver, () -> requeue(request));
  }

  @VisibleForTesting
  RequeueResponse requeue(RequeueRequest request) throws IOException {
    String queueName = request.getQueue();

    var deadline = request.hasDeadline()
        ? ImmutableTimestamp.of(toMillis(request.getDeadline()))
        : null;

    try {
      var newDeadlineMillis = manager.withExistingQueue(queueName,
          queue -> queue.requeueAsync(request.getId(), deadline)).millis();
      return RequeueResponse.newBuilder()
          .setDeadline(fromMillis(newDeadlineMillis))
          .build();
    } catch (IllegalStateException e) {
      throw Status.FAILED_PRECONDITION
          .withDescription(e.getMessage())
          .asRuntimeException();
    }
  }

  @Override
  public void peek(PeekRequest request, StreamObserver<Item> responseObserver) {
    process(responseObserver, () -> peek(request));
  }

  @VisibleForTesting
  Item peek(PeekRequest request) throws IOException {
    String queueName = request.getQueue();

    var item = manager.withExistingQueue(queueName, Queue::peekAsync);
    if (item == null) {
      throw Status.NOT_FOUND
          .withDescription("no items pending")
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
  public void getQueueInfo(
      GetQueueInfoRequest request,
      StreamObserver<QueueInfo> responseObserver
  ) {
    process(responseObserver, () -> getQueueInfo(request));
  }

  @VisibleForTesting
  QueueInfo getQueueInfo(GetQueueInfoRequest request) throws IOException {
    String queueName = request.getQueue();
    var info = manager.withExistingQueue(queueName, Queue::getQueueInfoAsync);
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
    throw Status.UNIMPLEMENTED.asRuntimeException();
  }

  private static <R> void process(StreamObserver<R> observer, Callable<R> operation) {
    try {
      observer.onNext(operation.call());
      observer.onCompleted();
    } catch (QueueNotFoundException e) {
      observer.onError(Status.NOT_FOUND
          .withDescription(e.getMessage())
          .asRuntimeException());
    } catch (StatusException | StatusRuntimeException e) {
      observer.onError(e);
    } catch (Exception e) {
      logger.error("Unhandled error", e);
      observer.onError(e);
    }
  }
}
