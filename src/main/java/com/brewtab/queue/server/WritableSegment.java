package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import java.io.IOException;

public interface WritableSegment extends Segment {
  /**
   * Add a new entry to the segment.
   */
  // TODO: Make this asynchronous?
  void add(Entry entry) throws IOException;

  /**
   * Close the segment, making it read-only.
   */
  void close() throws IOException;

  /**
   * Freeze a previously closed segment. Returns a new segment which should
   * replace this one.
   */
  Segment freeze() throws IOException;
}
