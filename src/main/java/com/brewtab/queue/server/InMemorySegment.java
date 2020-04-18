package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryComparator;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Arrays;
import java.util.Collection;

public class InMemorySegment implements Segment {
  private final ImmutableSortedSet<Entry> entries;
  private final PeekingIterator<Entry> it;

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
    return it.hasNext() ? entryKey(it.peek()) : null;
  }

  @Override
  public Entry next() {
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
}
