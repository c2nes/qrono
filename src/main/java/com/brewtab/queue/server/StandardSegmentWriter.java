package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryComparator;
import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class StandardSegmentWriter implements SegmentWriter {
  private final Path directory;

  public StandardSegmentWriter(Path directory) {
    this.directory = directory;
  }

  @Override
  public Opener write(String segmentName, Collection<Entry> entries) throws IOException {
    var tombstoneSegment = new InMemorySegment(entries.stream()
        .filter(Entry::hasTombstone)
        .collect(toImmutableSortedSet(entryComparator())));

    var pendingSegment = new InMemorySegment(entries.stream()
        .filter(Entry::hasPending)
        .collect(toImmutableSortedSet(entryComparator())));

    if (tombstoneSegment.size() > 0) {
      var tombstoneIdxPath = SegmentFiles.getTombstoneIndexPath(directory, segmentName);
      ImmutableSegment.write(tombstoneIdxPath, tombstoneSegment);
    }

    var pendingIdxPath = SegmentFiles.getPendingIndexPath(directory, segmentName);
    var offsets = writeIfNonEmpty(pendingIdxPath, pendingSegment);

    return position -> {
      Long offset = offsets.get(position);
      if (offset == null) {
        throw new IllegalArgumentException("key not found in segment");
      }

      return ImmutableSegment.open(pendingIdxPath, offset);
    };
  }

  @Override
  public void copy(String segmentName, Segment source) throws IOException {
    var path = SegmentFiles.getCombinedIndexPath(directory, segmentName);
    ImmutableSegment.write(path, source);
  }

  private Map<Key, Long> writeIfNonEmpty(Path path, Segment segment) throws IOException {
    if (segment.size() == 0) {
      return Collections.emptyMap();
    }
    return ImmutableSegment.writeWithOffsetTracking(path, segment);
  }
}
