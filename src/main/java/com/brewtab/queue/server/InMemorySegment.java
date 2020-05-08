package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.collect.ImmutableSortedSet;

public class InMemorySegment implements Segment {
  private final SegmentName name;
  private final ImmutableSortedSet<Entry> entries;

  public InMemorySegment(SegmentName name, Iterable<Entry> entries) {
    this(name, ImmutableSortedSet.copyOf(entries));
  }

  public InMemorySegment(SegmentName name, ImmutableSortedSet<Entry> entries) {
    this.name = name;
    this.entries = entries;
  }

  @Override
  public SegmentName name() {
    return name;
  }

  @Override
  public SegmentMetadata metadata() {
    var pendingCount = entries.stream().filter(Entry::isPending).count();
    var tombstoneCount = entries.size() - pendingCount;
    return ImmutableSegmentMetadata.builder()
        .pendingCount(pendingCount)
        .tombstoneCount(tombstoneCount)
        .maxId(entries.stream()
            .mapToLong(e -> e.key().id())
            .max()
            .orElse(0))
        .build();
  }

  @Override
  public SegmentReader newReader(Key position) {
    return new InMemorySegmentReader(entries.tailSet(Entry.newTombstoneEntry(position), false));
  }
}
