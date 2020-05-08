package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Arrays;
import java.util.Collection;

public class InMemorySegmentReader implements SegmentReader {
  private final PeekingIterator<Entry> it;
  private boolean closed = false;

  public InMemorySegmentReader(Entry... entries) {
    this(Arrays.asList(entries));
  }

  public InMemorySegmentReader(Collection<Entry> entries) {
    it = Iterators.peekingIterator(ImmutableSortedSet.copyOf(entries).iterator());
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
  public void close() {
    closed = true;
  }
}
