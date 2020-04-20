package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Preconditions;
import java.io.IOException;

public class EmptySegment implements Segment {
  private boolean closed = false;

  @Override
  public long size() {
    return 0;
  }

  @Override
  public Key peek() {
    Preconditions.checkState(!closed, "closed");
    return null;
  }

  @Override
  public Entry next() throws IOException {
    Preconditions.checkState(!closed, "closed");
    return null;
  }

  @Override
  public Key first() {
    return null;
  }

  @Override
  public Key last() {
    return null;
  }

  @Override
  public long getMaxId() {
    return 0;
  }

  @Override
  public void close() throws IOException {
    closed = true;
  }
}
