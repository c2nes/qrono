package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Metadata;
import com.google.common.base.Preconditions;
import java.io.IOException;

public class EmptySegment implements Segment {
  private boolean closed = false;

  @Override
  public Metadata getMetadata() {
    // TODO: "hasFoo" required -- never null
    return Metadata.getDefaultInstance();
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
  public void close() throws IOException {
    closed = true;
  }
}
