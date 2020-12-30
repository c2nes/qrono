package net.qrono.server;

import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.util.concurrent.AbstractIdleService;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import net.qrono.server.IOScheduler.Parameters;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Entry.Key;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableQueueStorageStats;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.QueueStorageStats;
import net.qrono.server.data.SegmentMetadata;
import net.qrono.server.util.DataSize;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Rename? Queue...Reader? PendingSet? EntryLog?
public class QueueData extends AbstractIdleService {
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

  @Override
  protected void startUp() throws Exception {
    // TODO: Use some sort of file locking to prevent multiple processes
    //  from accessing the queue data simultaneously?
    Files.createDirectories(directory);

    // TODO: Recover from failed compaction

    // Load any segment logs and write out segments. Also remove any temporary files.
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

      if (SegmentFiles.isTemporaryPath(path)) {
        logger.warn("Removing orphaned temporary file {}", path);
        Files.delete(path);
      }
    }

    var segmentPaths = new HashMap<SegmentName, Path>();
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();
      if (SegmentFiles.isIndexPath(path)) {
        var name = SegmentName.fromPath(path);
        segmentPaths.put(name, path);
      }
    }

    var compactedSegments = new ArrayList<SegmentName>();
    for (SegmentName a : segmentPaths.keySet()) {
      for (SegmentName b : segmentPaths.keySet()) {
        if (a.level() < b.level() && a.id() <= b.id()) {
          var compacted = segmentPaths.get(a);
          var replacement = segmentPaths.get(b);
          logger.warn("Removing previously compacted segment {} (superseded by {})",
              compacted, replacement);
          Files.delete(compacted);
          compactedSegments.add(a);
        }
      }
    }

    // Remove all of the segment paths we just deleted.
    segmentPaths.keySet().removeAll(compactedSegments);

    // Open all of the remaining segments
    long maxSegmentId = -1;
    for (Path path : segmentPaths.values()) {
      var segment = ImmutableSegment.open(path);
      logger.debug("Opening segment {}; meta={}", path, segment.metadata());
      immutableSegments.addSegment(segment, Key.ZERO);

      var segmentName = SegmentName.fromPath(path);
      var segmentId = segmentName.id();
      if (segmentId > maxSegmentId) {
        maxSegmentId = segmentId;
      }
    }

    segmentCounter.set(maxSegmentId + 1);

    // Initialize next segment
    currentSegment = nextWritableSegment();
  }

  @Override
  protected synchronized void shutDown() throws Exception {
    writeAndDeleteLog(currentSegment.freeze());
    currentSegment.close();
    immutableSegments.close();
  }

  public void compact() throws IOException {
    var stats = getStorageStats();
    // Skip compaction if we do not have enough tombstones
    if (stats.totalTombstoneCount() < 0.2 * stats.totalPendingCount()) {
      return;
    }

    // Force flush current segment so it is included in the compaction.
    forceFlushCurrentSegment().join();

    var segments = immutableSegments.getSegments();
    var pendingMerge = new MergedSegmentReader();
    for (Segment segment : segments) {
      pendingMerge.addSegment(segment, Key.ZERO);
    }

    var mergeName = new SegmentName(
        segments.stream()
            .mapToInt(s -> s.name().level())
            .max()
            .orElse(0) + 1,
        segments.stream()
            .mapToLong(s -> s.name().id())
            .max()
            .orElse(0));

    var start = Instant.now();
    var mergedSegment = segmentWriter.write(mergeName, pendingMerge, () -> last);
    var writeDuration = Duration.between(start, Instant.now());

    // Synchronized to ensure "last" doesn't change while we replace the segments.
    synchronized (this) {
      // At this point we would like to swap the new segment for the input segments.
      // We need to do this swapping within the merged segment view.
      immutableSegments.replaceSegments(segments, mergedSegment, last);
    }

    var seekDuration = Duration.between(start, Instant.now()).minus(writeDuration);
    var mergedPath = SegmentFiles.getIndexPath(directory, mergeName);
    logger.info("Merged {} segments; entries={}, pending={}, tombstone={}, "
            + "size={}, writeDuration={}, switches={}, seekDuration={}",
        segments.size(),
        mergedSegment.size(),
        mergedSegment.metadata().pendingCount(),
        mergedSegment.metadata().tombstoneCount(),
        DataSize.fromBytes(Files.size(mergedPath)),
        Duration.between(start, Instant.now()),
        pendingMerge.getHeadSwitchDebugCount(),
        seekDuration);

    // Delete old segments
    for (Segment segment : segments) {
      Files.delete(SegmentFiles.getIndexPath(directory, segment.name()));
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
      var lastDeadline = last.deadline();

      // Move the deadline only as far forward as is needed to ensure "last" will precede "entry".
      var newDeadline = entry.key().id() < last.id()
          ? ImmutableTimestamp.of(lastDeadline.millis() + 1)
          : lastDeadline;

      var newItem = ImmutableItem.builder()
          .from(item)
          .deadline(newDeadline)
          .build();
      var newEntry = Entry.newPendingEntry(newItem);

      // Verify that ordering is now correct
      Verify.verify(last.compareTo(newEntry.key()) < 0);

      return newEntry;
    }

    return entry;
  }

  private List<Entry> adjustEntryDeadlines(List<Entry> entries) {
    List<Entry> updatedEntries = entries;
    int i = 0;
    for (Entry entry : entries) {
      var updated = adjustEntryDeadline(entry);
      if (updated != entry) {
        if (updatedEntries == entries) {
          // (Lazily) make mutable copy before updating
          updatedEntries = new ArrayList<>(entries);
        }
        updatedEntries.set(i, updated);
      }
      i++;
    }
    return updatedEntries;
  }

  public List<Entry> write(List<Entry> entries) throws IOException {
    synchronized (this) {
      entries = adjustEntryDeadlines(entries);
      currentSegment.addAll(entries);
    }

    checkFlushCurrentSegment();
    return entries;
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

    flush(frozen);
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

    logger.debug("Scheduled in-memory segment for compaction; waitTime={}"
            + ", pending={}, tombstone={}",
        Duration.between(start, Instant.now()),
        frozen.metadata().pendingCount(),
        frozen.metadata().tombstoneCount());

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

  public synchronized Entry peekEntry() throws IOException {
    return headReader().peekEntry();
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

  static QueueStorageStats getStorageStats(MergedSegmentReader immutableSegments) {
    return ImmutableQueueStorageStats.builder()
        .persistedPendingCount(immutableSegments.getSegments().stream()
            .map(Segment::metadata)
            .mapToLong(SegmentMetadata::pendingCount)
            .sum())
        .persistedTombstoneCount(immutableSegments.getSegments().stream()
            .map(Segment::metadata)
            .mapToLong(SegmentMetadata::tombstoneCount)
            .sum())
        .bufferedPendingCount(0)
        .bufferedTombstoneCount(0)
        .build();
  }

  public synchronized QueueStorageStats getStorageStats() {
    return ImmutableQueueStorageStats.copyOf(getStorageStats(immutableSegments))
        .withBufferedPendingCount(currentSegment.pendingCount())
        .withBufferedTombstoneCount(currentSegment.tombstoneCount());
  }
}
