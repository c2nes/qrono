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
import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Queue {
  private static final Logger logger = LoggerFactory.getLogger(Queue.class);
  private final String name;
  private final IdGenerator idGenerator;
  private final Path directory;
  private final Clock clock;

  private final AtomicLong segmentCounter = new AtomicLong();
  private WritableSegment currentSegment;
  private MergedSegmentView<Segment> immutableSegments = new MergedSegmentView<>();
  private Map<Long, Item> dequeued = new HashMap<>();

  // In-memory segment ->
  // Frozen segment ->
  // Immutable segment

  // TODO: Redo this properly
  private final SynchronousQueue<WritableSegment> segmentWriterTransfer = new SynchronousQueue<>();
  private final Thread segmentWriterThread = new Thread(() -> {
    try {
      while (true) {
        WritableSegment segment = segmentWriterTransfer.take();
        Instant start = Instant.now();
        // TODO: Replace segment with frozen copy?
        segment.freeze();
        System.out.println("SegmentWrite:" + Duration.between(start, Instant.now()));
      }
    } catch (InterruptedException e) {
      logger.info("Shutting down");
    } catch (Exception e) {
      // TODO: Implement error recovery
      logger.error("Failed to write segment!", e);
    }
  });

  public Queue(String name, IdGenerator idGenerator, Path directory) throws IOException {
    this(name, idGenerator, directory, Clock.systemUTC());
  }

  public Queue(String name, IdGenerator idGenerator, Path directory, Clock clock)
      throws IOException {
    this.name = name;
    this.idGenerator = idGenerator;
    this.directory = directory;
    this.clock = clock;

    Files.createDirectories(directory);
    currentSegment = nextWritableSegment();
    segmentWriterThread.start();
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
      Uninterruptibles.putUninterruptibly(segmentWriterTransfer, currentSegment);
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

    // Enqueue deadlines must be in the future?
    // Deadline can never be _before_ the oldest dequeued item?

    // Segments have Entries and Tombstones

    // How do we know if a WAL entry is within an on-disk segment? Keep WALs and Segments 1-1.
    //
    // If we assign monotonic IDs to segments then we just need to keep track of the
    // last segment ID in the WAL?
    //
    // Each segment can have its own WAL.
    //
    // *.log
    // *.idx

    // In-Memory Segments writes to its WAL
    // When full (atomically),
    // - Close old segment WAL
    // - Rename old segment WAL -> "0000000001.log_ro"
    // - Open new in-memory segment with new WAL
    // Write old segment IDX.
    // Remove old segment WAL.
    // Segment is frozen.

    // When freezing segment,
    // - Some items may have been dequeued
    // - Keep track of these IDs and skip over them after swapping the in-memory
    //   segment for the on-disk copy. The segment writer can keep track of the
    //   file offset for each item. The on-disk segment can then be opened to
    //   the appropriate offset.

    // On startup,
    //
    // Re-execute rewrites of any "*.log_ro" files
    // - Read file into memory
    // - Freeze and write segment
    // - Remove *.log_ro file
    //
    // Find "*.log" file (there should be at most one)
    // - Handle corruption at end
    // - Otherwise handle same as log_ro (i.e. freeze and remove)
    //
    // Start new segment.

    // Should you ever see a rogue tombstone (without its corresponding entry?)
    // Tombstones are added to mark a pending item as deleted.
    // When a working item is requeued a tombstone is added for the old copy.
    // When a working item is released a tombstone is added for the item.
    // When a tombstone is _added_ the corresponding entry has already been seen.
    // We only need to read tombstones when,
    // - Loading a WAL
    // - Merging segments
    //
    // Its possible we would see rogue tombstones when merging segments. For
    // example, if we have segments A, B and C and A contains entry "a" and B
    // contains tombstone "a", and we merge B and C then we will see the rogue
    // tombstone for "a".
    //
    // A "dequeue" operation should never see a tombstone though, since we
    // must always be reading from a merged view of _all_ segments.

    // When freezing a segment,
    // - Write separate segments for pending and tombstones
    // -
  }

  public void release(ReleaseRequest request) throws IOException {
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

  public RequeueResponse requeue(RequeueRequest request) throws IOException {
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
