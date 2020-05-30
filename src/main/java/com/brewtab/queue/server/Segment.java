package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.SegmentMetadata;
import java.io.IOException;

public interface Segment {

  SegmentName name();

  SegmentMetadata metadata();

  default SegmentReader newReader() throws IOException {
    return newReader(Entry.Key.ZERO);
  }

  /**
   * Returns a new reader positioned at the first key <em>after</em> the given position.
   */
  SegmentReader newReader(Entry.Key position) throws IOException;

  /**
   * Total number of entries. Equivalent to,
   *
   * <pre>
   *   metadata().pendingCount() + metadata().tombstoneCount()
   * </pre>
   */
  default long size() {
    var meta = metadata();
    return meta == null ? 0 : meta.pendingCount() + meta.tombstoneCount();
  }
}
