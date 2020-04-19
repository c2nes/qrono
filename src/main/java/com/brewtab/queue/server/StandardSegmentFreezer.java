package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryComparator;
import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;

public class StandardSegmentFreezer implements SegmentFreezer {
  private final Path directory;

  // TODO: Pass name to freeze()
  public StandardSegmentFreezer(Path directory) {
    this.directory = directory;
  }

  @Override
  public FrozenSegment freeze(WritableSegment segment) throws IOException {
    var name = segment.getName();
    var entries = segment.entries();
    var tombstoneSegment = new InMemorySegment(entries.stream()
        .filter(Entry::hasTombstone)
        .collect(toImmutableSortedSet(entryComparator())));

    var pendingSegment = new InMemorySegment(entries.stream()
        .filter(Entry::hasPending)
        .collect(toImmutableSortedSet(entryComparator())));

    if (tombstoneSegment.size() > 0) {
      var tombstoneIdxPath = SegmentFiles.getTombstoneIndexPath(directory, name);
      ImmutableSegment.write(tombstoneIdxPath, tombstoneSegment);
    }

    var pendingIdxPath = SegmentFiles.getPendingIndexPath(directory, name);
    var offsets = writeIfNonEmpty(pendingIdxPath, pendingSegment);

    return position -> {
      Long offset = offsets.get(position);
      if (offset == null) {
        throw new IllegalArgumentException("key not found in segment");
      }

      return ImmutableSegment.newReader(FileChannel.open(pendingIdxPath), offset);
    };
  }

  private Map<Key, Long> writeIfNonEmpty(Path path, Segment segment) throws IOException {
    if (segment.size() == 0) {
      return Collections.emptyMap();
    }
    return ImmutableSegment.writeWithOffsetTracking(path, segment);
  }
}
