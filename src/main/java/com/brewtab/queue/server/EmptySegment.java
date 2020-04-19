package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;

public class EmptySegment implements Segment {
  @Override
  public long size() {
    return 0;
  }

  @Override
  public Key peek() {
    return null;
  }

  @Override
  public Entry next() throws IOException {
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
}
