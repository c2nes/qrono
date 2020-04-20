package com.brewtab.queue.server;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.Closeable;
import java.io.IOException;

public interface Segment extends Closeable {
  long size();

  Entry.Key peek();

  Entry next() throws IOException;

  Entry.Key first();

  Entry.Key last();

  long getMaxId();

  static Entry.Key itemKey(Item item) {
    return Key.newBuilder()
        .setDeadline(item.getDeadline())
        .setId(item.getId())
        .build();
  }

  static Entry.Key entryKey(Entry entry) {
    switch (entry.getEntryCase()) {
      case PENDING:
        return itemKey(entry.getPending());

      case TOMBSTONE:
        return entry.getTombstone();

      default:
        throw new IllegalArgumentException("invalid entry type: " + entry.getEntryCase());
    }
  }
}
