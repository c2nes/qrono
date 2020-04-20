package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryComparator;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

public class InMemorySegment implements Segment {
  private final ImmutableSortedSet<Entry> entries;
  private final PeekingIterator<Entry> it;
  private boolean closed = false;

  public InMemorySegment(Entry... entries) {
    this(Arrays.asList(entries));
  }

  public InMemorySegment(Collection<Entry> entries) {
    this.entries = ImmutableSortedSet.copyOf(entryComparator(), entries);
    it = Iterators.peekingIterator(this.entries.iterator());
  }

  @Override
  public long size() {
    return entries.size();
  }

  @Override
  public Key peek() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? entryKey(it.peek()) : null;
  }

  @Override
  public Entry next() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.next() : null;
  }

  @Override
  public Key first() {
    return entries.isEmpty() ? null : entryKey(entries.iterator().next());
  }

  @Override
  public Key last() {
    return entries.isEmpty() ? null : entryKey(entries.descendingIterator().next());
  }

  @Override
  public long getMaxId() {
    return entries.stream()
        .filter(Entry::hasPending)
        .map(Entry::getPending)
        .mapToLong(Item::getId)
        .max()
        .orElse(0);
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
