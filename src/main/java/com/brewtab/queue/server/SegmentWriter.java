package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import java.io.IOException;
import java.util.function.Supplier;

public interface SegmentWriter {

  default Opener write(String segmentName, Segment source) throws IOException {
    var firstKey = source.getMetadata().firstKey();
    return write(segmentName, source, () -> firstKey);
  }

  /**
   * Writes a segment. Returns an {@link Opener} for the newly written segment allowing it to be
   * opened to a specific key.
   */
  Opener write(
      String segmentName,
      Segment source,
      // This feels finicky
      Supplier<Entry.Key> liveReaderOffset
  ) throws IOException;

  interface Opener {
    // Open to this key, or the first key after it.
    Segment open(Entry.Key position) throws IOException;
  }
}
