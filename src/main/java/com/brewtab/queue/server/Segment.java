package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import java.io.IOException;

public interface Segment {
  long size();

  Entry.Key peek();

  Entry next() throws IOException;

  Entry.Key first();

  Entry.Key last();
}
