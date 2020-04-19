package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;
import static java.util.Objects.requireNonNull;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.server.IOScheduler.Parameters;
import com.brewtab.queue.server.SegmentFreezer.FrozenSegment;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueData {
  private static final Logger logger = LoggerFactory.getLogger(QueueData.class);

  private final Path directory;
  private final IOScheduler ioScheduler;
  private final SegmentFreezer segmentFreezer;

  private final AtomicLong segmentCounter = new AtomicLong();
  private final MergedSegmentView<Segment> immutableSegments = new MergedSegmentView<>();
  private WritableSegment currentSegment = null;

  public QueueData(Path directory, IOScheduler ioScheduler, SegmentFreezer segmentFreezer) {
    this.directory = directory;
    this.ioScheduler = ioScheduler;
    this.segmentFreezer = segmentFreezer;
  }

  // TODO: Refactor loading process
  public QueueLoadSummary load() throws IOException {
    // TODO: Use some sort of file locking to prevent multiple processes
    //  from accessing the queue data simultaneously?
    Files.createDirectories(directory);

    // Load and freeze any existing segment logs
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();

      if (SegmentFiles.isClosedLogPath(path)) {
        String segmentName = SegmentFiles.getSegmentNameFromPath(path);
        List<Entry> entries = StandardWriteAheadLog.read(path);
        segmentFreezer.freeze(segmentName, entries);
        Files.delete(path);
      }

      if (SegmentFiles.isLogPath(path)) {
        String segmentName = SegmentFiles.getSegmentNameFromPath(path);
        // TODO: Use lax read method here
        List<Entry> entries = StandardWriteAheadLog.read(path);
        segmentFreezer.freeze(segmentName, entries);
        Files.delete(path);
      }
    }

    long maxSegmentId = -1;
    long maxId = 0;
    for (File file : requireNonNull(directory.toFile().listFiles())) {
      Path path = file.toPath();
      if (SegmentFiles.isAnyIndexPath(path)) {
        var segment = ImmutableSegment.open(path);
        immutableSegments.addSegment(segment);
        if (segment.getMaxId() > maxId) {
          maxId = segment.getMaxId();
        }

        var segmentName = SegmentFiles.getSegmentNameFromPath(path);
        var segmentId = Long.parseLong(segmentName);
        if (segmentId > maxSegmentId) {
          maxSegmentId = segmentId;
        }
      }
    }

    segmentCounter.set(maxSegmentId + 1);

    return new QueueLoadSummary(maxId);
    // SegmentPaths(name).getLogPath
    // SegmentPaths(name).getClosedLogPath
    // SegmentPaths(name).getCombinedIndexPath
    // SegmentPaths(name).getPendingIndexPath
    // SegmentPaths(name).getTombstoneIndexPath
  }

  // Read segment
  // Write segment
  // Write WAL
  // Rename WAL
  // Remove WAL

  private WritableSegment nextWritableSegment() throws IOException {
    var name = String.format("%010d", segmentCounter.getAndIncrement());
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

  public Entry write(Entry entry) throws IOException {
    // TODO: Have currentSegment.add advance the deadline if necessary?
    getCurrentSegment().add(entry);
    flushCurrentSegment();
    return entry;
  }

  private void flushCurrentSegment() throws IOException {
    // TODO: Fix this...
    if (currentSegment.size() > 128 * 1024) {
      var start = Instant.now();
      currentSegment.close();
      convertToFrozen(currentSegment);
      currentSegment = nextWritableSegment();
      System.out.println("xfrSegmentWriter: " + Duration.between(start, Instant.now()));
    }
  }

  private void convertToFrozen(WritableSegment segment) {
    // Wrap the segment in a decorator before adding it to the set of immutable segments.
    // This decorator allows us to transparently swap the in-memory copy of the segment
    // for the on-disk copy once the freeze is complete.
    var replaceOnFreezeSegment = new ReplaceOnFreezeSegment(segment);
    immutableSegments.addSegment(replaceOnFreezeSegment);

    // Schedule the freeze operation to happen asynchronously
    var freezeFuture = ioScheduler.schedule(() -> freezeAndDeleteLog(segment), new Parameters());

    // TODO: Handle exceptional completion of the future

    // Replace the in-memory copy of the segment with the frozen on-disk copy.
    freezeFuture.thenAccept(frozenSegment -> {
      try {
        replaceOnFreezeSegment.replace(frozenSegment);
      } catch (IOException e) {
        // TODO: Crash the process or handle the error or something...
        logger.error("Failed to replace in-memory segment with on-disk copy", e);
      }
    });
  }

  private FrozenSegment freezeAndDeleteLog(WritableSegment segment) throws IOException {
    var segmentName = segment.getName();
    var entries = segment.entries();
    var frozenSegment = segmentFreezer.freeze(segmentName, entries);
    StandardWriteAheadLog.delete(directory, segmentName);
    return frozenSegment;
  }

  public Entry.Key peek() {
    return head().key;
  }

  public Entry next() throws IOException {
    var segment = head().segment;
    return segment == null ? null : segment.next();
  }

  private KeySegmentPair head() {
    var currentSegmentKey = currentSegment.peek();
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

    if (entryKeyComparator().compare(currentSegmentKey, immutableSegmentsKey) < 0) {
      return new KeySegmentPair(currentSegmentKey, currentSegment);
    } else {
      return new KeySegmentPair(immutableSegmentsKey, immutableSegments);
    }
  }

  private static class KeySegmentPair {
    private final Key key;
    private final Segment segment;

    private KeySegmentPair(Key key, Segment segment) {
      this.key = key;
      this.segment = segment;
    }
  }

  private static class ReplaceOnFreezeSegment implements Segment {
    private volatile Segment active;

    private ReplaceOnFreezeSegment(Segment active) {
      this.active = active;
    }

    @Override
    public long size() {
      return active.size();
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
    public Key first() {
      return active.first();
    }

    @Override
    public Key last() {
      return active.last();
    }

    @Override
    public long getMaxId() {
      return active.getMaxId();
    }

    synchronized void replace(FrozenSegment frozenSegment) throws IOException {
      active = frozenSegment.open(peek());
    }
  }
}
