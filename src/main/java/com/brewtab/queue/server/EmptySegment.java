package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.base.Preconditions;
import java.io.IOException;

public class EmptySegment implements Segment {
  private boolean closed = false;

  @Override
  public SegmentMetadata getMetadata() {
    return null;
  }

  @Override
  public Entry.Key peek() {
    Preconditions.checkState(!closed, "closed");
    return null;
  }

  @Override
  public Entry next() throws IOException {
    Preconditions.checkState(!closed, "closed");
    return null;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
