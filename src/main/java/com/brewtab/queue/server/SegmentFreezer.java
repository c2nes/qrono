package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;

public interface SegmentFreezer {
  FrozenSegment freeze(WritableSegment segment) throws IOException;

  interface FrozenSegment {
    Segment open(Key position) throws IOException;
  }
}
