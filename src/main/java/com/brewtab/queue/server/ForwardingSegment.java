package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.SegmentMetadata;
import java.io.IOException;

public abstract class ForwardingSegment implements Segment {
  protected abstract Segment delegate();

  @Override
  public SegmentMetadata getMetadata() {
    return delegate().getMetadata();
  }

  @Override
  public SegmentReader newReader() throws IOException {
    return delegate().newReader();
  }

  @Override
  public SegmentReader newReader(Key position) throws IOException {
    return delegate().newReader(position);
  }

  @Override
  public long size() {
    return delegate().size();
  }

  @Override
  public String toString() {
    return delegate().toString();
  }
}
