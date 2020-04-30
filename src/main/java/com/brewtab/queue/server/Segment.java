package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.SegmentMetadata;
import java.io.Closeable;
import java.io.IOException;

public interface Segment extends Closeable {
  SegmentMetadata getMetadata();

  Entry.Key peek();

  Entry next() throws IOException;

  /**
   * Size without other metadata
   */
  default long size() {
    var meta = getMetadata();
    return meta == null ? 0 : meta.pendingCount() + meta.tombstoneCount();
  }
}
