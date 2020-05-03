package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

public interface SegmentWriter {

  /**
   * Writes a segment. Returns an {@link Opener} for the newly written segment allowing it to be
   * opened to a specific key.
   */
  Opener write(String segmentName, Collection<Entry> entries) throws IOException;

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

  /**
   * Writes a segment copied from the given source segment.
   */
  void copy(String segmentName, Segment source) throws IOException;

  interface Opener {
    // Open to this key, or the first key after it.
    Segment open(Entry.Key position) throws IOException;
  }
}
