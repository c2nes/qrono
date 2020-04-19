package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;
import java.util.Collection;

public interface SegmentFreezer {
  FrozenSegment freeze(String segmentName, Collection<Entry> entries) throws IOException;

  interface FrozenSegment {
    Segment open(Key position) throws IOException;
  }
}
