package com.brewtab.queue.server;

import com.brewtab.queue.Api.DequeueRequest;
import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.EnqueueResponse;
import com.brewtab.queue.Api.Item;
import com.brewtab.queue.QueueServerGrpc;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.util.Timestamps;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueServerService extends QueueServerGrpc.QueueServerImplBase {
  private static final Logger logger = LoggerFactory.getLogger(QueueServerService.class);
  private final Map<String, Queue> queues = new HashMap<>();
  private final IdGenerator idGenerator;
  private final Path directory;

  public QueueServerService(IdGenerator idGenerator, Path directory) {
    this.idGenerator = idGenerator;
    this.directory = directory;
  }

  @Override
  public void enqueue(EnqueueRequest request, StreamObserver<EnqueueResponse> responseObserver) {
    try {
      responseObserver.onNext(enqueue(request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      logger.error("Unhandled exception", e);
      responseObserver.onError(e);
    }
  }

  @VisibleForTesting
  EnqueueResponse enqueue(EnqueueRequest request) throws IOException {
    String queueName = request.getQueue();
    if (queueName.equals("short-circuit")) {
      return EnqueueResponse.newBuilder()
          .setId(0)
          .setDeadline(Timestamps.fromMillis(System.currentTimeMillis()))
          .build();
    }
    Queue queue = queues.get(queueName);
    if (queue == null) {
      queue = new Queue(queueName, idGenerator, directory.resolve(queueName));
      queues.put(queueName, queue);
    }

    Item item = queue.enqueue(request);
    return EnqueueResponse.newBuilder()
        .setId(item.getId())
        .setDeadline(item.getDeadline())
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
    try {
      responseObserver.onNext(dequeue(request));
      responseObserver.onCompleted();
    } catch (Exception e) {
      responseObserver.onError(e);
    }
  }

  private Item dequeue(DequeueRequest request) throws IOException {
    String queueName = request.getQueue();
    Queue queue = queues.get(queueName);
    if (queue == null) {
      throw Status.NOT_FOUND
          .withDescription("no such queue, " + queueName)
          .asRuntimeException();
    }

    return queue.dequeue();
  }
}
