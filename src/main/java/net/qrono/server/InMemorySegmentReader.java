package net.qrono.server;

import static io.netty.util.ReferenceCountUtil.retain;

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
import net.qrono.server.data.Entry.Key;

public class InMemorySegmentReader implements SegmentReader {
  private final PeekingIterator<Entry> it;
  private final ReferenceCounted owner;
  private boolean closed = false;

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

  private Entry peekEntryUnretained() {
    Preconditions.checkState(!closed, "closed");
    return it.hasNext() ? it.peek() : null;
  }

  @Override
  public Entry peekEntry() {
    return retain(peekEntryUnretained());
  }

  @Override
  public Key peek() {
    var entry = peekEntryUnretained();
    return entry == null ? null : entry.key();
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
