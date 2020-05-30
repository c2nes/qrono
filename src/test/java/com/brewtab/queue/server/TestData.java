package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableItem.Stats;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Item;
import com.google.protobuf.ByteString;

public class TestData {
  static final long BASE_TIME = 1_234_567_000; // 2009-02-13T23:16:40Z

  static final Stats ZERO_STATS = ImmutableItem.Stats.builder()
      .enqueueTime(ImmutableTimestamp.ZERO)
      .requeueTime(ImmutableTimestamp.ZERO)
      .dequeueCount(0)
      .build();

  static final ByteString VALUE = ByteString.copyFromUtf8("Hello, world!");

  static final Item ITEM_1_T5 = ImmutableItem.builder()
      .deadline(ImmutableTimestamp.of(BASE_TIME + 5))
      .id(1001)
      .stats(ZERO_STATS)
      .value(VALUE)
      .build();

  static final Item ITEM_2_T0 = ImmutableItem.builder()
      .deadline(ImmutableTimestamp.of(BASE_TIME))
      .id(1002)
      .stats(ZERO_STATS)
      .value(VALUE)
      .build();

  static final Item ITEM_3_T10 = ImmutableItem.builder()
      .deadline(ImmutableTimestamp.of(BASE_TIME + 10))
      .id(1003)
      .stats(ZERO_STATS)
      .value(VALUE)
      .build();

  static final Entry PENDING_1_T5 = Entry.newPendingEntry(ITEM_1_T5);
  static final Entry TOMBSTONE_1_T5 = Entry.newTombstoneEntry(ITEM_1_T5);

  static final Entry PENDING_2_T0 = Entry.newPendingEntry(ITEM_2_T0);
  static final Entry TOMBSTONE_2_T0 = Entry.newTombstoneEntry(ITEM_2_T0);

  static final Entry PENDING_3_T10 = Entry.newPendingEntry(ITEM_3_T10);
  static final Entry TOMBSTONE_3_T10 = Entry.newTombstoneEntry(ITEM_3_T10);

  static Item withValue(Item item, ByteString value) {
    return ImmutableItem.builder()
        .from(item)
        .value(value)
        .build();
  }

  static Entry withValue(Entry entry, ByteString value) {
    var item = entry.item();
    if (item == null) {
      throw new IllegalArgumentException("entry must be pending");
    }
    return Entry.newPendingEntry(withValue(item, value));
  }
}
