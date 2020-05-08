package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.collect.ImmutableSortedSet;
import java.io.IOException;

public class InMemorySegment implements Segment {
  private final ImmutableSortedSet<Entry> entries;

  public InMemorySegment(Iterable<Entry> entries) {
    this(ImmutableSortedSet.copyOf(entries));
  }

  public InMemorySegment(ImmutableSortedSet<Entry> entries) {
    this.entries = entries;
  }

  @Override
  public SegmentMetadata getMetadata() {
    var pendingCount = entries.stream().filter(Entry::isPending).count();
    var tombstoneCount = entries.size() - pendingCount;
    return ImmutableSegmentMetadata.builder()
        .pendingCount(pendingCount)
        .tombstoneCount(tombstoneCount)
        .maxId(entries.stream()
            .mapToLong(e -> e.key().id())
            .max()
            .orElse(0))
        .firstKey(entries.iterator().next().key())
        .lastKey(entries.descendingIterator().next().key())
        .build();
  }

  @Override
  public SegmentReader newReader(Key position) throws IOException {
    return new InMemorySegmentReader(entries.tailSet(Entry.newTombstoneEntry(position)));
  }
}
