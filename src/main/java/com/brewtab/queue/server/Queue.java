package com.brewtab.queue.server;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static com.google.protobuf.util.Timestamps.toMillis;

import com.brewtab.queue.Api;
import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.GetQueueInfoRequest;
import com.brewtab.queue.Api.QueueInfo;
import com.brewtab.queue.Api.ReleaseRequest;
import com.brewtab.queue.Api.RequeueRequest;
import com.brewtab.queue.Api.RequeueResponse;
import com.brewtab.queue.Api.Stats;
import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.brewtab.queue.server.data.Timestamp;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Queue {
  private static final Logger logger = LoggerFactory.getLogger(Queue.class);

  private final QueueData data;
  private final IdGenerator idGenerator;
  private final Clock clock;

  private final Map<Long, Item> dequeued = new HashMap<>();

  public Queue(QueueData queueData, IdGenerator idGenerator, Clock clock) {
    this.data = queueData;
    this.idGenerator = idGenerator;
    this.clock = clock;
  }

  public synchronized QueueLoadSummary load() throws IOException {
    return data.load();
  }

  public synchronized Api.Item enqueue(EnqueueRequest request) throws IOException {
    var enqueueTime = clock.millis();
    var deadline = request.hasDeadline() ? toMillis(request.getDeadline()) : enqueueTime;

    // Invariant: Entries must be written in ID order.
    //
    // Solution: This method is synchronized so ID generation and entry writing
    //  are performed atomically.

    long id = idGenerator.generateId();

    var requestedItem = ImmutableItem.builder()
        .deadline(ImmutableTimestamp.of(deadline))
        .id(id)
        .stats(ImmutableItem.Stats.builder()
            .dequeueCount(0)
            .enqueueTime(ImmutableTimestamp.of(enqueueTime))
            .requeueTime(Timestamp.ZERO)
            .build())
        .value(request.getValue())
        .build();

    var entry = data.write(Entry.newPendingEntry(requestedItem));

    // data.write() may adjust the item deadline
    var item = entry.item();

    // Help out the linter
    assert item != null : "pending entries have non-null items";

    // Build API response object
    return Api.Item.newBuilder()
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

  public synchronized Api.Item dequeue() throws IOException {
    var key = data.peek();
    if (key == null) {
      return null;
    }

    long now = clock.millis();
    long deadline = key.deadline().millis();
    if (now < deadline) {
      // Deadline is in the future.
      return null;
    }

    var entry = data.next();
    var item = entry.item();
    if (item == null) {
      throw new UnsupportedOperationException("tombstone dequeue handling not implemented");
    }

    // Increment dequeue count
    item = ImmutableItem.builder()
        .from(item)
        .stats(ImmutableItem.Stats.builder()
            .from(item.stats())
            .dequeueCount(item.stats().dequeueCount() + 1)
            .build())
        .build();

    dequeued.put(item.id(), item);

    // Convert to API model
    return Api.Item.newBuilder()
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

  public synchronized void release(ReleaseRequest request) throws IOException {
    var released = dequeued.remove(request.getId());
    if (released == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    data.write(Entry.newTombstoneEntry(released));
  }

  public synchronized RequeueResponse requeue(RequeueRequest request) throws IOException {
    var item = dequeued.remove(request.getId());

    if (item == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    var tombstone = Entry.newTombstoneEntry(item);
    var requeueTime = clock.millis();
    var deadline = request.hasDeadline() ? toMillis(request.getDeadline()) : requeueTime;

    item = ImmutableItem.builder()
        .from(item)
        .deadline(ImmutableTimestamp.of(deadline))
        .stats(ImmutableItem.Stats.builder()
            .from(item.stats())
            .requeueTime(ImmutableTimestamp.of(requeueTime))
            .build())
        .build();

    // TODO: Commit these atomically
    data.write(Entry.newPendingEntry(item));
    data.write(tombstone);

    return RequeueResponse.newBuilder().build();
  }

  public synchronized QueueInfo getQueueInfo(GetQueueInfoRequest request) {
    SegmentMetadata meta = data.getMetadata();

    // Queue is totally empty
    if (meta == null) {
      return QueueInfo.getDefaultInstance();
    }

    // Our definition of pending excludes dequeued items, but at the data layer an
    // item is pending until it is released / requeued.
    long totalSize = meta.pendingCount() - meta.tombstoneCount();
    long dequeuedSize = dequeued.size();
    long pendingSize = totalSize - dequeuedSize;
    return QueueInfo.newBuilder()
        .setPending(pendingSize)
        .setDequeued(dequeuedSize)
        .build();
  }

  public void runTestCompaction() throws IOException {
    data.runTestCompaction();
  }
}
