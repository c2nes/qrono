package net.qrono.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import java.util.Arrays;
import java.util.Collection;
import java.util.NavigableSet;
import net.qrono.server.data.Entry;

public class InMemorySegmentReader implements SegmentReader {
  private final PeekingIterator<Entry> it;
  private boolean closed = false;
  private final ReferenceCounted owner;

  public InMemorySegmentReader(Entry... entries) {
    this(Arrays.asList(entries));
  }

  public InMemorySegmentReader(Collection<Entry> entries) {
    it = Iterators.peekingIterator(ImmutableSortedSet.copyOf(entries).iterator());
    owner = null;
  }

  public InMemorySegmentReader(NavigableSet<Entry> entries, ReferenceCounted owner) {
    it = Iterators.peekingIterator(entries.iterator());
    this.owner = owner;
  }

  @Override
  public Entry peekEntry() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.peek().retain() : null;
  }

  @Override
  public Entry next() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.next().retain() : null;
  }

  @Override
  public void close() {
    ReferenceCountUtil.release(owner);
    closed = true;
  }
}
