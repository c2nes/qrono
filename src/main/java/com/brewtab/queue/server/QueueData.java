package com.brewtab.queue.server;

import static java.util.Objects.requireNonNull;

import com.brewtab.queue.server.IOScheduler.Parameters;
import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.brewtab.queue.server.util.DataSize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueData implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(QueueData.class);

  private final Path directory;
  private final IOScheduler ioScheduler;
  private final SegmentWriter segmentWriter;

  private final AtomicLong segmentCounter = new AtomicLong();

  // TODO: Make this ... better?
  // Updated together
  private final Map<SegmentName, Segment> immutableSegmentsMap = new HashMap<>();
  private final MergedSegmentReader immutableSegments = new MergedSegmentReader();

  private WritableSegment currentSegment = null;

  // Last key dequeued by next()
  private Entry.Key last = null;

  public QueueData(Path directory, IOScheduler ioScheduler, SegmentWriter segmentWriter) {
    this.directory = directory;
    this.ioScheduler = ioScheduler;
    this.segmentWriter = segmentWriter;
  }

  // TODO: Refactor loading process
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
        if (!entries.isEmpty()) {
          segmentWriter.write(segmentName, new InMemorySegmentReader(entries));
        }
        Files.delete(path);
      }
    }

    long maxSegmentId = -1;
    long maxId = 0;
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();
      if (SegmentFiles.isIndexPath(path)) {
        var segment = ImmutableSegment.open(path);
        logger.debug("Opening segment {}; meta={}", path, segment.getMetadata());
        immutableSegmentsMap.put(SegmentName.fromPath(path), segment);
        immutableSegments.addSegment(segment);
        if (segment.getMetadata().maxId() > maxId) {
          maxId = segment.getMetadata().maxId();
        }

        var segmentName = SegmentName.fromPath(path);
        var segmentId = segmentName.id();
        if (segmentId > maxSegmentId) {
          maxSegmentId = segmentId;
        }
      }
    }

    segmentCounter.set(maxSegmentId + 1);

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
    CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
    synchronized (this) {
      if (currentSegment != null && currentSegment.size() > 0) {
        future = flushCurrentSegment();
      }
    }

    future.join();

    var pendingMergePaths = new HashSet<Path>();
    var pendingMergeNames = new HashSet<SegmentName>();
    var pendingMerge = new MergedSegmentReader();
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();
      if (SegmentFiles.isIndexPath(path)) {
        pendingMergePaths.add(path);
        pendingMergeNames.add(SegmentName.fromPath(path));
        pendingMerge.addSegment(ImmutableSegment.open(path));
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
    var originalSegments = pendingMergeNames.stream()
        .map(immutableSegmentsMap::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
    var mergedPath = SegmentFiles.getIndexPath(directory, mergeName);
    var mergedSegment = segmentWriter.write(
        mergeName,
        pendingMerge,
        this::peek);
    var writeDuration = Duration.between(start, Instant.now());

    // Synchronized to ensure peek() doesn't change while we replace the segments.
    synchronized (this) {
      var seekDuration = Duration.between(start, Instant.now()).minus(writeDuration);
      logger.info("Merged {} segments; entries={}, size={}, writeDuration={}, "
              + "switches={}, seekDuration={}",
          pendingMerge.getSegments().size(),
          mergedSegment.size(),
          DataSize.fromBytes(Files.size(mergedPath)),
          Duration.between(start, Instant.now()),
          pendingMerge.getHeadSwitchDebugCount(),
          seekDuration);
      // At this point we would like to swap the new segment for the input segments.
      // We need to do this swapping within the merged segment view.
      immutableSegmentsMap.put(mergeName, mergedSegment);
      immutableSegments.replaceSegments(originalSegments, mergedSegment);
      immutableSegmentsMap.keySet().removeAll(pendingMergeNames);
    }

    // Close
    for (SegmentReader segment : originalSegments) {
      segment.close();
    }
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

  private WritableSegment getCurrentSegment() throws IOException {
    if (currentSegment == null) {
      Files.createDirectories(directory);
      currentSegment = nextWritableSegment();
    }
    return currentSegment;
  }

  private Entry adjustEntryDeadline(Entry entry) {
    // If item is non-null then this is a pending entry
    var item = entry.item();
    if (item != null && last != null && entry.key().compareTo(last) < 0) {
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

  public synchronized Entry write(Entry entry) throws IOException {
    entry = adjustEntryDeadline(entry);
    getCurrentSegment().add(entry);
    checkFlushCurrentSegment();
    return entry;
  }

  private void checkFlushCurrentSegment() throws IOException {
    // TODO: Fix this...
    if (getCurrentSegment().size() > 128 * 1024) {
      flushCurrentSegment();
    }
  }

  @VisibleForTesting
  CompletableFuture<Void> flushCurrentSegment() throws IOException {
    var start = Instant.now();
    // Freeze and flush current segment to disk
    var future = flush(currentSegment);
    logger.debug("Scheduled in-memory segment for compaction; waitTime={}",
        Duration.between(start, Instant.now()));

    // Start new writable segment
    currentSegment = nextWritableSegment();

    return future;
  }

  private CompletableFuture<Void> flush(WritableSegment segment) throws IOException {
    // Freeze segment to close WAL and prevent future writes
    Segment frozenInMemory = segment.freeze();

    // Wrap the segment in a decorator before adding it to the set of immutable segments.
    // This decorator allows us to transparently swap the in-memory copy of the segment
    // for the on-disk copy once the write is complete.
    var replaceableSegment = new ReplaceableSegment(frozenInMemory);
    immutableSegmentsMap.put(segment.getName(), replaceableSegment);
    immutableSegments.addSegment(replaceableSegment);

    // Schedule the write operation to happen asynchronously
    var writeFuture = ioScheduler.schedule(() -> writeAndDeleteLog(segment), new Parameters());

    // TODO: Handle exceptional completion of the future

    // Replace the in-memory copy of the segment with the frozen on-disk copy.
    return writeFuture.thenAccept(opener -> {
      try {
        replaceableSegment.replaceFrom(opener);
      } catch (IOException e) {
        // TODO: Crash the process or handle the error or something...
        logger.error("Failed to replace in-memory segment with on-disk copy", e);
      }
    });
  }

  private Segment writeAndDeleteLog(SegmentName name, SegmentReader source) throws IOException {
    var frozenSegment = segmentWriter.write(name, source, this::peek);
    StandardWriteAheadLog.delete(directory, name);
    return frozenSegment;
  }

  public synchronized Entry.Key peek() {
    return head().key;
  }

  public synchronized Entry next() throws IOException {
    var segment = head().segment;
    if (segment == null) {
      return null;
    }
    var entry = segment.next();
    if (entry != null) {
      last = entry.key();
    }
    return entry;
  }

  private KeySegmentPair head() {
    var currentSegmentKey = currentSegment == null ? null : currentSegment.peek();
    var immutableSegmentsKey = immutableSegments.peek();

    if (currentSegmentKey == null && immutableSegmentsKey == null) {
      // Queue is empty
      return new KeySegmentPair(null, null);
    }

    if (currentSegmentKey == null) {
      return new KeySegmentPair(immutableSegmentsKey, immutableSegments);
    }

    if (immutableSegmentsKey == null) {
      return new KeySegmentPair(currentSegmentKey, currentSegment);
    }

    if (currentSegmentKey.compareTo(immutableSegmentsKey) < 0) {
      return new KeySegmentPair(currentSegmentKey, currentSegment);
    } else {
      return new KeySegmentPair(immutableSegmentsKey, immutableSegments);
    }
  }

  public synchronized long getQueueSize() {
    var immutableMeta = immutableSegments.getMetadata();
    if (currentSegment == null) {
      return immutableMeta;
    }

    var memMeta = currentSegment.getMetadata();
    if (memMeta == null) {
      return immutableMeta;
    }

    if (immutableMeta == null) {
      return memMeta;
    }

    return SegmentMetadata.merge(memMeta, immutableMeta);
  }

  @Override
  public synchronized void close() throws IOException {
    currentSegment.freeze();
    writeAndDeleteLog(currentSegment);
    currentSegment.close();
    immutableSegments.close();
  }

  private static class KeySegmentPair {
    private final Entry.Key key;
    private final SegmentReader segment;

    private KeySegmentPair(Entry.Key key, SegmentReader segment) {
      this.key = key;
      this.segment = segment;
    }
  }

  /**
   * Segment whose source segment can be replaced.
   */
  private static class ReplaceableSegment implements Segment {
    private Segment active;
    private List<ReplaceableReader> readers = new ArrayList<>(1);

    private ReplaceableSegment(Segment active) {
      this.active = active;
    }

    @Override
    public synchronized SegmentMetadata getMetadata() {
      return active.getMetadata();
    }

    @Override
    public synchronized SegmentReader newReader(Key position) throws IOException {
      if (readers == null) {
        return active.newReader(position);
      }

      var reader = new ReplaceableReader(active.newReader(position));
      readers.add(reader);
      return reader;
    }

    synchronized void replaceFrom(Segment replacement) throws IOException {
      Preconditions.checkState(readers != null, "replaceFrom can only be called once");
      active = replacement;
      for (ReplaceableReader reader : readers) {
        reader.replaceFrom(replacement);
      }
      // Allow readers to be GC'ed
      readers = null;
    }

    private static class ReplaceableReader implements SegmentReader {
      private volatile SegmentReader active;

      private ReplaceableReader(SegmentReader active) {
        this.active = active;
      }

      @Override
      public Key peek() {
        return active.peek();
      }

      @Override
      public synchronized Entry next() throws IOException {
        return active.next();
      }

      @Override
      public void close() throws IOException {
        active.close();
      }

      synchronized void replaceFrom(Segment replacement) throws IOException {
        active = replacement.newReader(peek());
      }
    }
  }
}
