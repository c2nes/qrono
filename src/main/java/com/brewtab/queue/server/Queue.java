package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.itemKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Queue {
  private static final Logger logger = LoggerFactory.getLogger(Queue.class);
  private final String name;
  private final IdGenerator idGenerator;
  private final Path directory;
  private final SegmentFreezer segmentFreezer;
  private final Clock clock;

  private final AtomicLong segmentCounter = new AtomicLong();
  private WritableSegment currentSegment;
  private final MergedSegmentView<Segment> immutableSegments = new MergedSegmentView<>();
  private final Map<Long, Item> dequeued = new HashMap<>();

  public Queue(String name, IdGenerator idGenerator, Path directory,
      SegmentFreezer segmentFreezer) throws IOException {
    this(name, idGenerator, directory, segmentFreezer, Clock.systemUTC());
  }

  public Queue(String name, IdGenerator idGenerator, Path directory,
      SegmentFreezer segmentFreezer, Clock clock)
      throws IOException {
    this.name = name;
    this.idGenerator = idGenerator;
    this.directory = directory;
    this.segmentFreezer = segmentFreezer;
    this.clock = clock;

    Files.createDirectories(directory);
    currentSegment = nextWritableSegment();
  }

  private WritableSegment nextWritableSegment() throws IOException {
    var name = String.format("%010d", segmentCounter.getAndIncrement());
    return new ReplaceWhenFrozenDecorator(new WritableSegmentImpl(directory, name));
  }

  private Path getDirectory() {
    return directory;
  }

  public synchronized Item enqueue(EnqueueRequest request) throws IOException {
    Timestamp enqueueTime = Timestamps.fromMillis(clock.millis());
    Timestamp deadline = request.hasDeadline() ? request.getDeadline() : enqueueTime;
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

    currentSegment.add(entry);
    flushCurrentSegment();
    return item;
  }

  private void flushCurrentSegment() throws IOException {
    // TODO: Fix this...
    if (currentSegment.size() > 128 * 1024) {
      var start = Instant.now();
      currentSegment.close();
      immutableSegments.addSegment(currentSegment);
      segmentFreezer.freezeUninterruptibly(currentSegment);
      currentSegment = nextWritableSegment();
      System.out.println("xfrSegmentWriter: " + Duration.between(start, Instant.now()));
    }
  }

  private Item dequeueFrom(Key key, Segment segment) throws IOException {
    long now = clock.millis();
    long deadline = Timestamps.toMillis(key.getDeadline());
    if (now < deadline) {
      // Deadline is in the future.
      return null;
    }
    Entry entry = segment.next();
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

  public synchronized Item dequeue() throws IOException {
    var currentSegmentKey = currentSegment.peek();
    var immutableSegmentsKey = immutableSegments.peek();

    if (currentSegmentKey == null && immutableSegmentsKey == null) {
      // Queue is empty
      return null;
    }

    if (currentSegmentKey == null) {
      return dequeueFrom(immutableSegmentsKey, immutableSegments);
    }

    if (immutableSegmentsKey == null) {
      return dequeueFrom(currentSegmentKey, currentSegment);
    }

    if (entryKeyComparator().compare(currentSegmentKey, immutableSegmentsKey) < 0) {
      return dequeueFrom(currentSegmentKey, currentSegment);
    } else {
      return dequeueFrom(immutableSegmentsKey, immutableSegments);
    }
  }

  public synchronized void release(ReleaseRequest request) throws IOException {
    Item released = dequeued.remove(request.getId());
    if (released == null) {
      throw Status.FAILED_PRECONDITION
          .withDescription("item not dequeued")
          .asRuntimeException();
    }

    currentSegment.add(Entry.newBuilder()
        .setTombstone(itemKey(released))
        .build());

    flushCurrentSegment();
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
    currentSegment.add(Entry.newBuilder()
        .setPending(item)
        .build());
    currentSegment.add(Entry.newBuilder()
        .setTombstone(originalKey)
        .build());

    flushCurrentSegment();
    return RequeueResponse.newBuilder().build();
  }
}
