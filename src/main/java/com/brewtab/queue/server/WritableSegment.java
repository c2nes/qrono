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
   * Close the segment, making it read-only.
   */
  void close() throws IOException;

  /**
   * Returns the entries in the segment. The segment must be closed before the entries can be read.
   */
  Collection<Entry> entries();
}
