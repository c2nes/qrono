package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class InMemorySegment implements Segment {
  private final NavigableMap<Key, Entry> entries = new TreeMap<>(entryKeyComparator());
  private final NavigableMap<Key, Entry> removed = new TreeMap<>(entryKeyComparator());

  @Override
  public long size() {
    return entries.size() + removed.size();
  }

  @Override
  public Key peek() {
    return entries.isEmpty() ? null : entries.firstKey();
  }

  @Override
  public Entry next() {
    Map.Entry<Key, Entry> entry = entries.pollFirstEntry();
    if (entry == null) {
      return null;
    }
    removed.put(entry.getKey(), entry.getValue());
    return entry.getValue();
  }

  // TODO: This should return the first
  @Override
  public Key first() {
    Key a = entries.isEmpty() ? null : entries.firstKey();
    Key b = removed.isEmpty() ? null : removed.firstKey();
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return entryKeyComparator().compare(a, b) <= 0 ? a : b;
  }

  @Override
  public Key last() {
    Key a = entries.isEmpty() ? null : entries.lastKey();
    Key b = removed.isEmpty() ? null : removed.lastKey();
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return entryKeyComparator().compare(a, b) >= 0 ? a : b;
  }

  public void addEntry(Entry entry) {
    entries.put(entry.getKey(), entry);
    removed.remove(entry.getKey());
  }

  public Segment freeze() {
    return new FrozenSegment(
        ImmutableList.copyOf(
            Iterables.mergeSorted(
                List.of(entries.values(), removed.values()),
                SegmentEntryComparators.entryComparator())));
  }

  private static class FrozenSegment implements Segment {
    private final ImmutableList<Entry> entries;
    private final PeekingIterator<Entry> it;

    // Entries must be sorted
    private FrozenSegment(ImmutableList<Entry> entries) {
      this.entries = entries;
      it = Iterators.peekingIterator(entries.iterator());
    }

    @Override
    public long size() {
      return entries.size();
    }

    @Override
    public Key peek() {
      return it.hasNext() ? it.peek().getKey() : null;
    }

    @Override
    public Entry next() {
      return it.hasNext() ? it.next() : null;
    }

    @Override
    public Key first() {
      return entries.isEmpty() ? null : entries.get(0).getKey();
    }

    @Override
    public Key last() {
      return entries.isEmpty() ? null : entries.get(entries.size() - 1).getKey();
    }
  }
}
