package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.itemKey;

import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.ReleaseRequest;
import com.brewtab.queue.Api.RequeueRequest;
import com.brewtab.queue.Api.RequeueResponse;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Stats;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
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

  public synchronized Item enqueue(EnqueueRequest request) throws IOException {
    Timestamp enqueueTime = Timestamps.fromMillis(clock.millis());
    Timestamp deadline = request.hasDeadline() ? request.getDeadline() : enqueueTime;

    // Invariant: Entries must be written in ID order.
    //
    // Solution: This method is synchronized so ID generation and entry writing
    //  are performed atomically.

    long id = idGenerator.generateId();

    Item item = Item.newBuilder()
        .setId(id)
        .setDeadline(deadline)
        .setValue(request.getValue())
        .setStats(Stats.newBuilder().setEnqueueTime(enqueueTime))
        .build();

    Entry entry = Entry.newBuilder()
        .setPending(item)
        .build();

    return data.write(entry).getPending();
  }

  public synchronized Item dequeue() throws IOException {
    Key key = data.peek();
    if (key == null) {
      return null;
    }

    long now = clock.millis();
    long deadline = Timestamps.toMillis(key.getDeadline());
    if (now < deadline) {
      // Deadline is in the future.
      return null;
    }

    Entry entry = data.next();
    if (!entry.hasPending()) {
      throw new UnsupportedOperationException("tombstone dequeue handling not implemented");
    }
    Item item = entry.getPending();
    // Increment dequeue count
    item = item.toBuilder()
        .setStats(item.getStats().toBuilder()
            .setDequeueCount(item.getStats().getDequeueCount() + 1))
        .build();
    dequeued.put(item.getId(), item);
    return item;
  }

  public synchronized void release(ReleaseRequest request) throws IOException {
    Item released = dequeued.remove(request.getId());
    if (released == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    data.write(Entry.newBuilder()
        .setTombstone(itemKey(released))
        .build());
  }

  public synchronized RequeueResponse requeue(RequeueRequest request) throws IOException {
    Item item = dequeued.remove(request.getId());

    if (item == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    Key originalKey = itemKey(item);
    Timestamp requeueTime = Timestamps.fromMillis(clock.millis());
    Timestamp deadline = request.hasDeadline() ? request.getDeadline() : requeueTime;
    Stats stats = item.getStats();
    item = item.toBuilder()
        .setStats(stats.toBuilder().setRequeueTime(requeueTime))
        .setDeadline(deadline)
        .build();

    // TODO: Commit these atomically
    data.write(Entry.newBuilder()
        .setPending(item)
        .build());
    data.write(Entry.newBuilder()
        .setTombstone(originalKey)
        .build());

    return RequeueResponse.newBuilder().build();
  }
}
