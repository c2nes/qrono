package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import java.io.IOException;

// TODO: Rename this...its not a true "Segment" anymore
public interface WritableSegment extends SegmentReader {
  /**
   * Returns the name of this segment.
   */
  SegmentName name();

  /**
   * Add a new entry to the segment.
   */
  Entry add(Entry entry) throws IOException;

  long pendingCount();

  long tombstoneCount();

  long size();

  /**
   * Freeze the segment, making it read-only.
   */
  Segment freeze() throws IOException;
}
