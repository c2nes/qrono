package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class InMemorySegment implements Segment {
  private final ImmutableSortedSet<Entry> entries;
  private final PeekingIterator<Entry> it;
  private boolean closed = false;

  public InMemorySegment(Entry... entries) {
    this(Arrays.asList(entries));
  }

  public InMemorySegment(Collection<Entry> entries) {
    this.entries = ImmutableSortedSet.copyOf(entries);
    it = Iterators.peekingIterator(this.entries.iterator());
  }

  @Override
  public SegmentMetadata getMetadata() {
    if (entries.isEmpty()) {
      return null;
    }

    return ImmutableSegmentMetadata.builder()
        .pendingCount(entries.stream().filter(Entry::isPending).count())
        .tombstoneCount(entries.stream().filter(Entry::isTombstone).count())
        .firstKey(entries.iterator().next().key())
        .lastKey(entries.descendingIterator().next().key())
        .maxId(entries.stream()
            .map(Entry::item)
            .filter(Objects::nonNull)
            .mapToLong(Item::id)
            .max()
            .orElse(0))
        .build();
  }

  public long size() {
    return entries.size();
  }

  @Override
  public Entry.Key peek() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.peek().key() : null;
  }

  @Override
  public Entry next() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.next() : null;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
