package com.brewtab.queue.server.data;

import com.brewtab.queue.server.data.Entry.Key;
import java.util.stream.Collector;

final class SegmentMetadataUtils {
  private static class Accumulator implements SegmentMetadata {
    private Key firstKey;
    private Key lastKey;
    private long maxId;
    private long pendingCount;
    private long tombstoneCount;

    @Override
    public Key firstKey() {
      return firstKey;
    }

    @Override
    public Key lastKey() {
      return lastKey;
    }

    @Override
    public long maxId() {
      return maxId;
    }

    @Override
    public long pendingCount() {
      return pendingCount;
    }

    @Override
    public long tombstoneCount() {
      return tombstoneCount;
    }

    public SegmentMetadata toImmutable() {
      if (firstKey == null || lastKey == null) {
        return null;
      }
      return ImmutableSegmentMetadata.copyOf(this);
    }
  }

  private static <C extends Comparable<C>> C min(C a, C b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return a.compareTo(b) < 0 ? a : b;
  }

  private static <C extends Comparable<C>> C max(C a, C b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return a.compareTo(b) < 0 ? b : a;
  }

  private static Accumulator accumulate(Accumulator acc, SegmentMetadata meta) {
    acc.pendingCount = acc.pendingCount() + meta.pendingCount();
    acc.tombstoneCount = acc.tombstoneCount() + meta.tombstoneCount();
    acc.firstKey = min(acc.firstKey(), meta.firstKey());
    acc.lastKey = max(acc.lastKey(), meta.lastKey());
    acc.maxId = Math.max(acc.maxId(), meta.maxId());
    return acc;
  }

  static SegmentMetadata merge(SegmentMetadata a, SegmentMetadata b) {
    var acc = new Accumulator();
    accumulate(acc, a);
    accumulate(acc, b);
    return ImmutableSegmentMetadata.copyOf(acc);
  }

  static Collector<SegmentMetadata, ?, SegmentMetadata> merge() {
    return Collector.of(
        Accumulator::new,
        SegmentMetadataUtils::accumulate,
        SegmentMetadataUtils::accumulate,
        Accumulator::toImmutable
    );
  }
}
