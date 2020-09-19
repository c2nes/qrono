package net.qrono.server;

import java.io.IOException;
import java.util.function.Supplier;
import net.qrono.server.data.Entry;

public interface SegmentWriter {

  default Segment write(SegmentName segmentName, SegmentReader source) throws IOException {
    var firstKey = source.peek();
    return write(segmentName, source, () -> firstKey);
  }

  /**
   * Writes a segment.
   */
  Segment write(
      SegmentName segmentName,
      SegmentReader source,
      // This feels finicky
      Supplier<Entry.Key> liveReaderOffset
  ) throws IOException;
}
