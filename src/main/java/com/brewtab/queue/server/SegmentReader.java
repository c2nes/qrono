package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import java.io.Closeable;
import java.io.IOException;

/**
 * A segment reader. Allows sequential reads of the entries in a segment.
 */
public interface SegmentReader extends Closeable {
  /**
   * Returns the key for the next entry in the segment without advancing the reader position.
   * Returns {@code null} when the end of the segment has been reached.
   */
  Entry.Key peek();

  /**
   * Returns the next entry in the segment. Returns {@code null} when the end of the segment has
   * been reached.
   */
  Entry next() throws IOException;
}
