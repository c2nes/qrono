package com.brewtab.queue.server.data;

import java.util.stream.Collector;
import org.immutables.value.Value;

@Value.Immutable
public interface SegmentMetadata {

  long maxId();

  long pendingCount();

  long tombstoneCount();

  static SegmentMetadata merge(SegmentMetadata a, SegmentMetadata b) {
    return SegmentMetadataUtils.merge(a, b);
  }

  static Collector<SegmentMetadata, ?, SegmentMetadata> merge() {
    return SegmentMetadataUtils.merge();
  }
}