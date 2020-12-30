package net.qrono.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import java.util.Arrays;
import java.util.Collection;
import net.qrono.server.data.Entry;

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
  public Entry peekEntry() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.peek() : null;
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
