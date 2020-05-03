package com.brewtab.queue.server;

import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Supplier;

public class StandardSegmentWriter implements SegmentWriter {
  private final Path directory;

  public StandardSegmentWriter(Path directory) {
    this.directory = directory;
  }

  @Override
  public Opener write(String segmentName, Collection<Entry> entries) throws IOException {
    var tombstoneSegment = new InMemorySegment(entries.stream()
        .filter(entry -> entry.item() == null)
        .collect(toImmutableSortedSet(Comparator.naturalOrder())));

    var pendingSegment = new InMemorySegment(entries.stream()
        .filter(entry -> entry.item() != null)
        .collect(toImmutableSortedSet(Comparator.naturalOrder())));

    if (tombstoneSegment.size() > 0) {
      var tombstoneIdxPath = SegmentFiles.getTombstoneIndexPath(directory, segmentName);
      var tmpTombstoneIdxPath = SegmentFiles.getTemporaryPath(tombstoneIdxPath);
      ImmutableSegment.write(tmpTombstoneIdxPath, tombstoneSegment);
      Files.move(tmpTombstoneIdxPath, tombstoneIdxPath, StandardCopyOption.ATOMIC_MOVE);
    }

    var pendingIdxPath = SegmentFiles.getPendingIndexPath(directory, segmentName);
    var tmpPendingIdxPath = SegmentFiles.getTemporaryPath(pendingIdxPath);
    var offsets = writeIfNonEmpty(tmpPendingIdxPath, pendingSegment);
    Files.move(tmpPendingIdxPath, pendingIdxPath, StandardCopyOption.ATOMIC_MOVE);

    return position -> {
      Long offset = offsets.get(position);
      if (offset == null) {
        throw new IllegalArgumentException("key not found in segment");
      }

      var segment = ImmutableSegment.open(pendingIdxPath);
      if (offset > 0) {
        segment.position(offset);
      }
      return segment;
    };
  }

  @Override
  public void copy(String segmentName, Segment source) throws IOException {
    var path = SegmentFiles.getCombinedIndexPath(directory, segmentName);
    var tmpPath = SegmentFiles.getTemporaryPath(path);
    ImmutableSegment.write(tmpPath, source);
    Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public Opener write(
      String segmentName,
      Segment source,
      Supplier<Key> liveReaderOffset
  ) throws IOException {
    var path = SegmentFiles.getCombinedIndexPath(directory, segmentName);
    var tmpPath = SegmentFiles.getTemporaryPath(path);
    var offset = ImmutableSegment.write(tmpPath, source, liveReaderOffset);
    Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);

    // TODO: Log how long it takes to do this
    return position -> {
      var segment = ImmutableSegment.open(path);
      if (position.compareTo(offset.getKey()) < 0) {
        // TODO: Log warning
      } else {
        segment.position(offset.getPosition());
      }
      while (segment.peek().compareTo(position) < 0) {
        segment.next();
      }
      return segment;
    };
  }

  private Map<Entry.Key, Long> writeIfNonEmpty(Path path, Segment segment) throws IOException {
    if (segment.size() == 0) {
      return Collections.emptyMap();
    }
    return ImmutableSegment.writeWithOffsetTracking(path, segment);
  }
}
