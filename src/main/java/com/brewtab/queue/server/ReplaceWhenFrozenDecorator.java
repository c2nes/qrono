package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;

/**
 * Decorates a WritableSegment, discarding the original WritableSegment with a frozen copy after
 * {@link #freeze()} is called.
 */
class ReplaceWhenFrozenDecorator implements WritableSegment {
  private volatile WritableSegment delegate;

  ReplaceWhenFrozenDecorator(WritableSegment delegate) {
    this.delegate = delegate;
  }

  @Override
  public void add(Entry entry) throws IOException {
    delegate.add(entry);
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  @Override
  public Segment freeze() throws IOException {
    var frozen = delegate.freeze();
    delegate = new FrozenSegmentAdapter(frozen);
    return frozen;
  }

  @Override
  public long size() {
    return delegate.size();
  }

  @Override
  public Key peek() {
    return delegate.peek();
  }

  @Override
  public Entry next() throws IOException {
    return delegate.next();
  }

  @Override
  public Key first() {
    return delegate.first();
  }

  @Override
  public Key last() {
    return delegate.last();
  }

  private static class FrozenSegmentAdapter implements WritableSegment {
    private final Segment segment;

    private FrozenSegmentAdapter(Segment segment) {
      this.segment = segment;
    }

    @Override
    public long size() {
      return segment.size();
    }

    @Override
    public Key peek() {
      return segment.peek();
    }

    @Override
    public Entry next() throws IOException {
      return segment.next();
    }

    @Override
    public Key first() {
      return segment.first();
    }

    @Override
    public Key last() {
      return segment.last();
    }

    @Override
    public void add(Entry entry) {
      throw new IllegalStateException("frozen");
    }

    @Override
    public void close() {
      throw new IllegalStateException("frozen");
    }

    @Override
    public Segment freeze() {
      throw new IllegalStateException("frozen");
    }
  }
}
