package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableQueueInfo;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.QueueInfo;
import com.brewtab.queue.server.data.Timestamp;
import com.google.common.base.Verify;
import com.google.protobuf.ByteString;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.IOException;
import java.time.Clock;
import javax.annotation.Nullable;

public class Queue {
  private final QueueData data;
  private final IdGenerator idGenerator;
  private final Clock clock;

  private final WorkingSet workingSet;
  private final LongSet dequeuedIds = new LongOpenHashSet();

  public Queue(QueueData queueData, IdGenerator idGenerator, Clock clock, WorkingSet workingSet) {
    this.data = queueData;
    this.idGenerator = idGenerator;
    this.clock = clock;
    this.workingSet = workingSet;
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

    workingSet.add(item);
    dequeuedIds.add(item.id());

    return item;
  }

  public synchronized void release(long id) throws IOException {
    if (!dequeuedIds.remove(id)) {
      // TODO: Consider using a more specific exception here
      throw new IllegalStateException("item not dequeued");
    }

    var released = workingSet.removeForRelease(id);
    Verify.verifyNotNull(released, "item missing from working set");
    var success = false;

    try {
      data.write(Entry.newTombstoneEntry(released));
      success = true;
    } finally {
      if (!success) {
        // TODO: We don't have the item...what should we do?
        // workingSet.add(item);
        dequeuedIds.add(id);
      }
    }
  }

  public synchronized Timestamp requeue(long id, @Nullable Timestamp deadline) throws IOException {
    if (!dequeuedIds.remove(id)) {
      throw new IllegalStateException("item not dequeued");
    }

    var item = workingSet.removeForRequeue(id);
    Verify.verifyNotNull(item, "item missing from working set");
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
        workingSet.add(item);
        dequeuedIds.add(id);
      }
    }
  }

  public synchronized QueueInfo getQueueInfo() {
    long totalSize = data.getQueueSize();
    long dequeuedSize = dequeuedIds.size();
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
