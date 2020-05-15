package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableQueueInfo;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.QueueInfo;
import com.brewtab.queue.server.data.Timestamp;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.time.Clock;
import javax.annotation.Nullable;

public class Queue {
  private final QueueData data;
  private final IdGenerator idGenerator;
  private final Clock clock;

  private final WorkingSet dequeued;

  public Queue(QueueData queueData, IdGenerator idGenerator, Clock clock, WorkingSet dequeued) {
    this.data = queueData;
    this.idGenerator = idGenerator;
    this.clock = clock;
    this.dequeued = dequeued;
  }

  public synchronized QueueLoadSummary load() throws IOException {
    return data.load();
  }

  public synchronized Item enqueue(ByteString value, @Nullable Timestamp deadline)
      throws IOException {
    var enqueueTime = ImmutableTimestamp.of(clock.millis());
    if (deadline == null) {
      deadline = enqueueTime;
    }

    // Invariant: Entries must be written in ID order.
    //
    // Solution: This method is synchronized so ID generation and entry writing
    //  are performed atomically.

    long id = idGenerator.generateId();

    var requestedItem = ImmutableItem.builder()
        .deadline(deadline)
        .id(id)
        .stats(ImmutableItem.Stats.builder()
            .dequeueCount(0)
            .enqueueTime(enqueueTime)
            .requeueTime(Timestamp.ZERO)
            .build())
        .value(value)
        .build();

    var entry = data.write(Entry.newPendingEntry(requestedItem));

    // data.write() may adjust the item deadline
    var item = entry.item();

    // Help out the linter
    assert item != null : "pending entries have non-null items";

    // Build API response object
    return item;
  }

  public synchronized Item dequeue() throws IOException {
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

    dequeued.add(item);

    return item;
  }

  public synchronized void release(long id) throws IOException {
    // TODO: This doesn't validate that the item is for this queue!
    var released = dequeued.removeForRelease(id);
    if (released == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    data.write(Entry.newTombstoneEntry(released));
  }

  public synchronized Timestamp requeue(long id, @Nullable Timestamp deadline) throws IOException {
    // TODO: This doesn't validate that the item is for this queue!
    var item = dequeued.removeForRequeue(id);
    if (item == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    var success = false;

    try {
      var tombstone = Entry.newTombstoneEntry(item);
      var requeueTime = ImmutableTimestamp.of(clock.millis());

      if (deadline == null) {
        deadline = requeueTime;
      }

      var newItem = ImmutableItem.builder()
          .from(item)
          .deadline(deadline)
          .stats(ImmutableItem.Stats.builder()
              .from(item.stats())
              .requeueTime(requeueTime)
              .build())
          .build();

      // TODO: Commit these atomically
      var entry = data.write(Entry.newPendingEntry(newItem));
      data.write(tombstone);
      success = true;

      return entry.key().deadline();
    } finally {
      if (!success) {
        dequeued.add(item);
      }
    }
  }

  public synchronized QueueInfo getQueueInfo() {
    long totalSize = data.getQueueSize();
    long dequeuedSize = dequeued.size();
    long pendingSize = totalSize - dequeuedSize;
    return ImmutableQueueInfo.builder()
        .pendingCount(pendingSize)
        .dequeuedCount(dequeuedSize)
        .build();
  }

  public void runTestCompaction() throws IOException {
    data.runTestCompaction();
  }
}
