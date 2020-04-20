package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import java.io.IOException;
import java.util.Collection;

public interface WritableSegment extends Segment {
  /**
   * Returns the name of this segment.
   */
  // TODO: Rename "ID"?
  String getName();

  /**
   * Add a new entry to the segment.
   */
  // TODO: Make this asynchronous?
  void add(Entry entry) throws IOException;

  /**
   * Freeze the segment, making it read-only.
   */
  void freeze() throws IOException;

  /**
   * Returns the entries in the segment. The segment must be frozen before the entries can be read.
   */
  Collection<Entry> entries();
}
