package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import java.io.Closeable;
import java.io.IOException;

public interface SegmentReader extends Closeable {
  Entry.Key peek();

  Entry next() throws IOException;
}
