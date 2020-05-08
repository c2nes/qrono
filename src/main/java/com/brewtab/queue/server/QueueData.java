package com.brewtab.queue.server;

import static java.util.Objects.requireNonNull;

import com.brewtab.queue.server.IOScheduler.Parameters;
import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.util.DataSize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueData implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(QueueData.class);

  private final Path directory;
  private final IOScheduler ioScheduler;
  private final SegmentWriter segmentWriter;

  private final AtomicLong segmentCounter = new AtomicLong();
  private final MergedSegmentReader immutableSegments = new MergedSegmentReader();

  private WritableSegment currentSegment = null;

  // Last key dequeued by next()
  private volatile Key last = Key.ZERO;

  public QueueData(Path directory, IOScheduler ioScheduler, SegmentWriter segmentWriter) {
    this.directory = directory;
    this.ioScheduler = ioScheduler;
    this.segmentWriter = segmentWriter;
  }

  // TODO: Refactor loading process
  // TODO: Load with Guava Service startUp
  public synchronized QueueLoadSummary load() throws IOException {
    // TODO: Use some sort of file locking to prevent multiple processes
    //  from accessing the queue data simultaneously?
    Files.createDirectories(directory);

    // Load any segment logs and write out segments
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();

      if (SegmentFiles.isClosedLogPath(path)) {
        logger.debug("Writing segment from log file {}", path);
        var segmentName = SegmentName.fromPath(path);
        List<Entry> entries = StandardWriteAheadLog.read(path);
        segmentWriter.write(segmentName, new InMemorySegmentReader(entries));
        Files.delete(path);
      }

      if (SegmentFiles.isLogPath(path)) {
        logger.debug("Writing segment from log file {}", path);
        var segmentName = SegmentName.fromPath(path);
        // TODO: Use lax read method here
        List<Entry> entries = StandardWriteAheadLog.read(path);
        segmentWriter.write(segmentName, new InMemorySegmentReader(entries));
        Files.delete(path);
      }
    }

    long maxSegmentId = -1;
    long maxId = 0;
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();
      if (SegmentFiles.isIndexPath(path)) {
        var segment = ImmutableSegment.open(path);
        logger.debug("Opening segment {}; meta={}", path, segment.metadata());
        immutableSegments.addSegment(segment, Key.ZERO);
        if (segment.metadata().maxId() > maxId) {
          maxId = segment.metadata().maxId();
        }

        var segmentName = SegmentName.fromPath(path);
        var segmentId = segmentName.id();
        if (segmentId > maxSegmentId) {
          maxSegmentId = segmentId;
        }
      }
    }

    segmentCounter.set(maxSegmentId + 1);

    // Initialize next segment
    currentSegment = nextWritableSegment();

    return new QueueLoadSummary(maxId);
  }

  private void runCompaction(List<Path> paths) {
    // Merge paths -> new path
    // How do we name the paths to know what replaces what?
    // 0-000 -> 1-000
    // 0-001 -> 1-001
    // ...
    // 1-*   -> 2-*
    // Level 0 = Written from in-memory segment
    // Level 1 = Written from level 0 segments
    // Level N = Written from level n-1 segments
    //
    // 0-000000000001.p-idx
    // 0-000000000002.p-idx
    // 0-000000000003.p-idx
    // 0-000000000004.p-idx
    // 0-000000000005.p-idx
    //
    // 1-000000000005.p-idx = Merged version of all level 0 segments <= 5
    //
    // 1-000000000010.p-idx = Merged version of all level 0, 5 < segments <= 10 ?
    //
    //
    // .p-idx.tmp -> .p-idx
    //
    // Any new level zero segments will be > 5
    // There will never be new level 0 segments <= 5
    //
    // Start compaction at level N,
    //  1) Identify all input files (level N-1). Find MaxID.
    //  2) Write N-MaxID.idx
    //  3) Swap in-process. How do we know where to skip to?
    //
    // Well, we need to keep track of the last returned key "K".
    // Any keys k_1 < K in written segments have already been returned (right? if k_1 < K and )
    //
    // Recovery,
    //  1) Remove tmp files
    //  2) Remove overlapping segment files in lower levels
    //  3) Compact log file (if exists)
    //

    // We can pass a reference to the watermark to the segment writer and ask it to track
    // the location of the first key after this value?

    // Unrelated, is it okay that we reuse IDs when requeueing items?
  }

  public void runTestCompaction() throws IOException {
    // Force flush current segment so it is included in the compaction.
    forceFlushCurrentSegment().join();

    // TODO: Some of this is redundant now
    var pendingMergePaths = new HashSet<Path>();
    var pendingMergeNames = new HashSet<SegmentName>();
    var pendingMerge = new MergedSegmentReader();
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();
      if (SegmentFiles.isIndexPath(path)) {
        pendingMergePaths.add(path);
        pendingMergeNames.add(SegmentName.fromPath(path));
        pendingMerge.addSegment(ImmutableSegment.open(path), Key.ZERO);
      }
    }

    var start = Instant.now();
    var mergeName = new SegmentName(
        pendingMergeNames.stream()
            .mapToInt(SegmentName::level)
            .max()
            .orElse(0) + 1,
        pendingMergeNames.stream()
            .mapToLong(SegmentName::id)
            .max()
            .orElse(0));
    var mergedPath = SegmentFiles.getIndexPath(directory, mergeName);
    var mergedSegment = segmentWriter.write(
        mergeName,
        pendingMerge,
        () -> last);
    var writeDuration = Duration.between(start, Instant.now());

    // Synchronized to ensure "last" doesn't change while we replace the segments.
    synchronized (this) {
      // At this point we would like to swap the new segment for the input segments.
      // We need to do this swapping within the merged segment view.
      immutableSegments.replaceSegments(pendingMerge.getSegments(), mergedSegment, last);
    }

    var seekDuration = Duration.between(start, Instant.now()).minus(writeDuration);
    logger.info("Merged {} segments; entries={}, size={}, writeDuration={}, "
            + "switches={}, seekDuration={}",
        pendingMerge.getSegments().size(),
        mergedSegment.size(),
        DataSize.fromBytes(Files.size(mergedPath)),
        Duration.between(start, Instant.now()),
        pendingMerge.getHeadSwitchDebugCount(),
        seekDuration);

    // Delete
    for (Path path : pendingMergePaths) {
      Files.delete(path);
    }
  }

  private WritableSegment nextWritableSegment() throws IOException {
    var name = new SegmentName(0, segmentCounter.getAndIncrement());
    var wal = StandardWriteAheadLog.create(directory, name);
    return new StandardWritableSegment(name, wal);
  }

  private Entry adjustEntryDeadline(Entry entry) {
    // If item is non-null then this is a pending entry
    var item = entry.item();
    if (item != null && entry.key().compareTo(last) < 0) {
      var newDeadline = last.deadline();
      var newItem = ImmutableItem.builder()
          .from(item)
          .deadline(newDeadline)
          .build();
      var newEntry = Entry.newPendingEntry(newItem);

      // If the item still compares less after adjusting the deadline then the item
      // ID must have gone backwards (which should never happen).
      // TODO: This could happen with a requeue, right? How should we handle it?
      //  Advance the deadline by 1ms? Assign new IDs when requeueing?
      Verify.verify(last.compareTo(newEntry.key()) < 0,
          "Pending item ID went backwards! %s < %s",
          newDeadline, last.deadline());

      return newEntry;
    }

    return entry;
  }

  public Entry write(Entry entry) throws IOException {
    synchronized (this) {
      entry = adjustEntryDeadline(entry);
      currentSegment.add(entry);
    }

    checkFlushCurrentSegment();
    return entry;
  }

  private void checkFlushCurrentSegment() throws IOException {
    Segment frozen;

    synchronized (this) {
      // TODO: This should consider memory usage
      if (currentSegment.size() < 128 * 1024) {
        // Do not freeze or flush
        return;
      }

      frozen = freezeAndReplaceCurrentSegment();
    }

    if (frozen != null) {
      flush(frozen);
    }
  }

  @VisibleForTesting
  synchronized Segment freezeAndReplaceCurrentSegment() throws IOException {
    // Freeze segment to close WAL and prevent future writes
    var frozen = currentSegment.freeze();
    immutableSegments.addSegment(frozen, last);
    currentSegment = nextWritableSegment();
    return frozen;
  }

  @VisibleForTesting
  CompletableFuture<Void> forceFlushCurrentSegment() throws IOException {
    return flush(freezeAndReplaceCurrentSegment());
  }

  private CompletableFuture<Void> flush(Segment frozen) {
    var start = Instant.now();

    // Schedule the write operation to happen asynchronously
    CompletableFuture<Void> future = ioScheduler.schedule(new Parameters(), () -> {
      // TODO: Handle exceptional completion of the future
      Segment segment = writeAndDeleteLog(frozen);
      synchronized (QueueData.this) {
        immutableSegments.replaceSegments(Collections.singleton(frozen), segment, last);
      }
      return null;
    });

    logger.debug("Scheduled in-memory segment for compaction; waitTime={}",
        Duration.between(start, Instant.now()));

    return future;
  }

  private Segment writeAndDeleteLog(Segment source) throws IOException {
    var frozenSegment = segmentWriter.write(source.name(), source.newReader(), () -> last);
    StandardWriteAheadLog.delete(directory, source.name());
    return frozenSegment;
  }

  public synchronized Key peek() {
    return headReader().peek();
  }

  public synchronized Entry next() throws IOException {
    var entry = headReader().next();
    if (entry != null) {
      last = entry.key();
    }
    return entry;
  }

  private SegmentReader headReader() {
    var currentSegmentKey = currentSegment.peek();
    if (currentSegmentKey == null) {
      return immutableSegments;
    }

    var immutableSegmentsKey = immutableSegments.peek();
    if (immutableSegmentsKey == null || currentSegmentKey.compareTo(immutableSegmentsKey) < 0) {
      return currentSegment;
    }

    return immutableSegments;
  }

  public synchronized long getQueueSize() {
    return immutableSegments.getSegments().stream()
        .map(Segment::metadata)
        .mapToLong(m -> m.pendingCount() - m.tombstoneCount())
        .sum()
        + currentSegment.pendingCount()
        - currentSegment.tombstoneCount();
  }

  @Override
  public synchronized void close() throws IOException {
    writeAndDeleteLog(currentSegment.freeze());
    currentSegment.close();
    immutableSegments.close();
  }
}
