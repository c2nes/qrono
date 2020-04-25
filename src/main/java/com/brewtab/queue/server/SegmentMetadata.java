package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Footer;
import com.brewtab.queue.Api.Segment.Metadata;
import com.brewtab.queue.Api.Segment.Metadata.Builder;
import com.brewtab.queue.Api.Segment.MetadataOrBuilder;
import java.util.Comparator;
import java.util.stream.Collector;

final class SegmentMetadata {
  private SegmentMetadata() {
  }


  private static <C> C min(C a, C b, Comparator<C> comparator) {
    return comparator.compare(a, b) < 0 ? a : b;
  }

  private static <C> C max(C a, C b, Comparator<C> comparator) {
    return comparator.compare(a, b) < 0 ? b : a;
  }

  private static Metadata.Builder accumulate(Metadata.Builder acc, MetadataOrBuilder meta) {
    acc.setPendingCount(acc.getPendingCount() + meta.getPendingCount());
    acc.setTombstoneCount(acc.getTombstoneCount() + meta.getTombstoneCount());
    acc.setFirstKey(min(acc.getFirstKey(), meta.getFirstKey(), entryKeyComparator()));
    acc.setLastKey(max(acc.getLastKey(), meta.getLastKey(), entryKeyComparator()));
    acc.setMaxId(Math.max(acc.getMaxId(), meta.getMaxId()));
    return acc;
  }

  static Metadata merge(Metadata a, Metadata b) {
    return accumulate(a.toBuilder(), b).build();
  }

  static Collector<MetadataOrBuilder, Builder, Metadata> merge() {
    return Collector.of(
        Metadata::newBuilder,
        SegmentMetadata::accumulate,
        SegmentMetadata::accumulate,
        Metadata.Builder::build
    );
  }

  static Footer toFooter(Metadata metadata) {
    return Footer.newBuilder()
        .setPendingCount(metadata.getPendingCount())
        .setTombstoneCount(metadata.getTombstoneCount())
        .setLastKey(metadata.getLastKey())
        .setMaxId(metadata.getMaxId())
        .build();
  }

  static Metadata fromFooterAndFirstKey(Footer footer, Entry.Key firstKey) {
    return Metadata.newBuilder()
        .setPendingCount(footer.getPendingCount())
        .setTombstoneCount(footer.getTombstoneCount())
        .setFirstKey(firstKey)
        .setLastKey(footer.getLastKey())
        .setMaxId(footer.getMaxId())
        .build();
  }
}
