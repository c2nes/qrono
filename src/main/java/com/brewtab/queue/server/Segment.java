package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.SegmentMetadata;
import java.io.IOException;

public interface Segment {
  SegmentMetadata getMetadata();

  default SegmentReader newReader() throws IOException {
    return newReader(Entry.Key.ZERO);
  }

  SegmentReader newReader(Entry.Key position) throws IOException;

  /**
   * Size without other metadata
   */
  default long size() {
    var meta = getMetadata();
    return meta == null ? 0 : meta.pendingCount() + meta.tombstoneCount();
  }
}
