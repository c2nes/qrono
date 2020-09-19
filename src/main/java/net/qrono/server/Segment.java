package net.qrono.server;

import java.io.IOException;
import net.qrono.server.data.Entry;
import net.qrono.server.data.SegmentMetadata;

/**
 * A segment is an immutable and ordered sequence of entries.
 */
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
