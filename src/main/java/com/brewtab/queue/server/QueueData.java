package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;
import static com.google.common.base.Verify.verifyNotNull;
import static java.util.Objects.requireNonNull;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Verify;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueData {
  private static final Logger logger = LoggerFactory.getLogger(QueueData.class);

  private final Path directory;
  private final SegmentFreezer segmentFreezer;

  private final AtomicLong segmentCounter = new AtomicLong();
  private final MergedSegmentView<Segment> immutableSegments = new MergedSegmentView<>();
  private WritableSegment currentSegment = null;

  public QueueData(Path directory, SegmentFreezer segmentFreezer) throws IOException {
    this.directory = directory;
    this.segmentFreezer = segmentFreezer;
  }

  // TODO: Return "QueueLoadSummary"
  public void load() throws IOException {
    Files.createDirectories(directory);
    var files = requireNonNull(directory.toFile().listFiles());

    for (File child : files) {
      child.getName().endsWith()
    }
  }

  private WritableSegment nextWritableSegment() throws IOException {
    var name = String.format("%010d", segmentCounter.getAndIncrement());
    return new ReplaceWhenFrozenDecorator(new WritableSegmentImpl(directory, name));
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
      immutableSegments.addSegment(currentSegment);
      segmentFreezer.freezeUninterruptibly(currentSegment);
      currentSegment = nextWritableSegment();
      System.out.println("xfrSegmentWriter: " + Duration.between(start, Instant.now()));
    }
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
}
