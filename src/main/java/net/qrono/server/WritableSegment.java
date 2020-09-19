package net.qrono.server;

import net.qrono.server.data.Entry;
import java.io.IOException;
import java.util.List;

// TODO: Rename this...its not a true "Segment" anymore
public interface WritableSegment extends SegmentReader {
  /**
   * Returns the name of this segment.
   */
  SegmentName name();

  /**
   * Add a new entry to the segment.
   */
  void add(Entry entry) throws IOException;

  void addAll(List<Entry> entries) throws IOException;

  long pendingCount();

  long tombstoneCount();

  long size();

  /**
   * Freeze the segment, making it read-only.
   */
  Segment freeze() throws IOException;
}
