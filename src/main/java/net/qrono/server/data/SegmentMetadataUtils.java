package net.qrono.server.data;

import java.util.stream.Collector;

final class SegmentMetadataUtils {
  private static class Accumulator implements SegmentMetadata {
    private long maxId;
    private long pendingCount;
    private long tombstoneCount;

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
      return ImmutableSegmentMetadata.copyOf(this);
    }
  }

  private static Accumulator accumulate(Accumulator acc, SegmentMetadata meta) {
    acc.pendingCount = acc.pendingCount() + meta.pendingCount();
    acc.tombstoneCount = acc.tombstoneCount() + meta.tombstoneCount();
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
