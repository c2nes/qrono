package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Item;
import java.io.IOException;
import java.util.Collection;

public interface SegmentWriter {

  /**
   * Writes a segment. Returns an {@link Opener} for the newly written segment allowing it to be
   * opened to a specific key.
   */
  Opener write(String segmentName, Collection<Entry> entries) throws IOException;

  /**
   * Writes a segment copied from the given source segment.
   */
  void copy(String segmentName, Segment source) throws IOException;

  interface Opener {
    Segment open(Entry.Key position) throws IOException;
  }
}
