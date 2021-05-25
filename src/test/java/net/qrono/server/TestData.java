package net.qrono.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import net.qrono.server.data.Entry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableItem.Stats;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Item;

public class TestData {
  static final long BASE_TIME = 1_234_567_000; // 2009-02-13T23:16:40Z

  static final Stats ZERO_STATS = ImmutableItem.Stats.builder()
      .enqueueTime(ImmutableTimestamp.ZERO)
      .requeueTime(ImmutableTimestamp.ZERO)
      .dequeueCount(0)
      .build();

  static final ByteBuf VALUE = Unpooled.copiedBuffer("Hello, world!", CharsetUtil.UTF_8);

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

  static final Item ITEM_4_T15 = ImmutableItem.builder()
      .deadline(ImmutableTimestamp.of(BASE_TIME + 15))
      .id(1004)
      .stats(ZERO_STATS)
      .value(VALUE)
      .build();

  static final Item ITEM_5_T20 = ImmutableItem.builder()
      .deadline(ImmutableTimestamp.of(BASE_TIME + 20))
      .id(1005)
      .stats(ZERO_STATS)
      .value(VALUE)
      .build();

  static final Entry PENDING_1_T5 = Entry.newPendingEntry(ITEM_1_T5);
  static final Entry TOMBSTONE_1_T5 = Entry.newTombstoneEntry(ITEM_1_T5);

  static final Entry PENDING_2_T0 = Entry.newPendingEntry(ITEM_2_T0);
  static final Entry TOMBSTONE_2_T0 = Entry.newTombstoneEntry(ITEM_2_T0);

  static final Entry PENDING_3_T10 = Entry.newPendingEntry(ITEM_3_T10);
  static final Entry TOMBSTONE_3_T10 = Entry.newTombstoneEntry(ITEM_3_T10);

  static final Entry PENDING_4_T15 = Entry.newPendingEntry(ITEM_4_T15);
  static final Entry TOMBSTONE_4_T15 = Entry.newTombstoneEntry(ITEM_4_T15);

  static final Entry PENDING_5_T20 = Entry.newPendingEntry(ITEM_5_T20);
  static final Entry TOMBSTONE_5_T20 = Entry.newTombstoneEntry(ITEM_5_T20);

  static Item withValue(Item item, ByteBuf value) {
    return ImmutableItem.builder()
        .from(item)
        .value(value)
        .build();
  }

  static Entry withValue(Entry entry, ByteBuf value) {
    var item = entry.item();
    if (item == null) {
      throw new IllegalArgumentException("entry must be pending");
    }
    return Entry.newPendingEntry(withValue(item, value));
  }

  static Item withId(Item item, int id) {
    return ImmutableItem.builder()
        .from(item)
        .id(id)
        .build();
  }

  static Entry withId(Entry entry, int id) {
    var item = entry.item();
    if (item == null) {
      throw new IllegalArgumentException("entry must be pending");
    }
    return Entry.newPendingEntry(withId(item, id));
  }
}
