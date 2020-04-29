package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Type;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import java.io.Closeable;
import java.io.IOException;

public interface Segment extends Closeable {
  SegmentMetadata getMetadata();

  Entry.Key peek();

  Entry next() throws IOException;

  /**
   * Size without other metadata
   */
  default long size() {
    var meta = getMetadata();
    return meta == null ? 0 : meta.pendingCount() + meta.tombstoneCount();
  }

  static Entry.Key pendingItemKey(Item item) {
    return ImmutableEntry.Key.builder()
        .deadline(item.deadline())
        .id(item.id())
        .entryType(Type.PENDING)
        .build();
  }

  static Entry.Key tombstoneItemKey(Item item) {
    return ImmutableEntry.Key.builder()
        .deadline(item.deadline())
        .id(item.id())
        .entryType(Type.TOMBSTONE)
        .build();
  }
}
