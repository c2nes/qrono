package com.brewtab.queue.server;

import static com.brewtab.queue.server.data.Entry.Type.PENDING;
import static com.brewtab.queue.server.data.Entry.Type.TOMBSTONE;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Item;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.immutables.value.Value;

@Value.Enclosing
final class Encoding {
  private static final int TIMESTAMP_SIZE = 6;

  // [enqueue time][requeue time][int:dequeue count]
  static final int STATS_SIZE = TIMESTAMP_SIZE + TIMESTAMP_SIZE + 4;

  // Key is bit packed
  // [14:reserved][48:deadline][64:id][2:type]
  static final int KEY_SIZE = 16;

  // [long:pending count][long:tombstone count][long:max id]
  static final int FOOTER_SIZE = 8 + 8 + 8;

  static int writeStats(ByteBuffer bb, Item.Stats stats) {
    long enqueueTime = stats.enqueueTime().millis();
    long requeueTime = stats.requeueTime().millis();
    int dequeueCount = stats.dequeueCount();

    long upper = (enqueueTime << 16) | (requeueTime >>> 32);
    long lower = (requeueTime << 32) | dequeueCount;
    bb.putLong(upper);
    bb.putLong(lower);

    return STATS_SIZE;
  }

  static int writeStats(ByteBuf bb, Item.Stats stats) {
    long enqueueTime = stats.enqueueTime().millis();
    long requeueTime = stats.requeueTime().millis();
    int dequeueCount = stats.dequeueCount();

    long upper = (enqueueTime << 16) | (requeueTime >>> 32);
    long lower = (requeueTime << 32) | dequeueCount;
    bb.writeLong(upper);
    bb.writeLong(lower);

    return STATS_SIZE;
  }

  static Item.Stats readStats(ByteBuffer bb) {
    long upper = bb.getLong();
    long lower = bb.getLong();

    long enqueueTime = upper >>> 16;
    long requeueTime = ((upper & ((1L << 16) - 1)) << 32) | (lower >>> 32);
    int dequeueCount = (int) (lower & ((1L << 32) - 1));

    return ImmutableItem.Stats.builder()
        .dequeueCount(dequeueCount)
        .enqueueTime(ImmutableTimestamp.of(enqueueTime))
        .requeueTime(ImmutableTimestamp.of(requeueTime))
        .build();
  }

  static Item.Stats readStats(ByteBuf bb) {
    long upper = bb.readLong();
    long lower = bb.readLong();

    long enqueueTime = upper >>> 16;
    long requeueTime = ((upper & ((1L << 16) - 1)) << 32) | (lower >>> 32);
    int dequeueCount = (int) (lower & ((1L << 32) - 1));

    return ImmutableItem.Stats.builder()
        .dequeueCount(dequeueCount)
        .enqueueTime(ImmutableTimestamp.of(enqueueTime))
        .requeueTime(ImmutableTimestamp.of(requeueTime))
        .build();
  }

  static int writeKey(ByteBuffer bb, Entry.Key key) {
    var deadline = key.deadline().millis();
    var id = key.id();
    var type = key.entryType();

    long upper = (deadline << 2) | (id >>> 62);
    long lower = (id << 2) | entryTypeToBits(type);
    bb.putLong(upper);
    bb.putLong(lower);

    return KEY_SIZE;
  }

  static int writeKey(ByteBuf bb, Entry.Key key) {
    var deadline = key.deadline().millis();
    var id = key.id();
    var type = key.entryType();

    long upper = (deadline << 2) | (id >>> 62);
    long lower = (id << 2) | entryTypeToBits(type);
    bb.writeLong(upper);
    bb.writeLong(lower);

    return KEY_SIZE;
  }

  static Entry.Key readKey(ByteBuffer bb) {
    long upper = bb.getLong();
    long lower = bb.getLong();

    long deadline = (upper >>> 2) & ((1L << 48) - 1);
    long id = (lower >>> 2) | ((upper & 0b11) << 62);
    var type = entryTypeFromBits(((int) lower) & 0b11);

    return ImmutableEntry.Key.builder()
        .deadline(ImmutableTimestamp.of(deadline))
        .id(id)
        .entryType(type)
        .build();
  }

  static Entry.Key readKey(ByteBuf bb) {
    long upper = bb.readLong();
    long lower = bb.readLong();

    long deadline = (upper >>> 2) & ((1L << 48) - 1);
    long id = (lower >>> 2) | ((upper & 0b11) << 62);
    var type = entryTypeFromBits(((int) lower) & 0b11);

    return ImmutableEntry.Key.builder()
        .deadline(ImmutableTimestamp.of(deadline))
        .id(id)
        .entryType(type)
        .build();
  }

  static int writeFooter(ByteBuffer bb, Footer footer) {
    bb.putLong(footer.pendingCount());
    bb.putLong(footer.tombstoneCount());
    bb.putLong(footer.maxId());
    return FOOTER_SIZE;
  }

  static Footer readFooter(ByteBuffer bb) {
    return ImmutableEncoding.Footer.builder()
        .pendingCount(bb.getLong())
        .tombstoneCount(bb.getLong())
        .maxId(bb.getLong())
        .build();
  }

  static int entrySize(Entry entry) {
    var item = entry.item();
    if (item != null) {
      return KEY_SIZE + STATS_SIZE + 4 + item.value().size();
    }
    return KEY_SIZE;
  }

  static void writeEntry(ByteBuf bb, Entry entry) {
    writeKey(bb, entry.key());

    var item = entry.item();
    if (item != null) {
      writeStats(bb, item.stats());
      bb.writeInt(item.value().size());
      bb.writeBytes(item.value().asReadOnlyByteBuffer());
    }
  }

  static Entry readEntry(ByteBuf bb) {
    var key = readKey(bb);
    if (key.entryType() == TOMBSTONE) {
      return Entry.newTombstoneEntry(key);
    }

    var stats = readStats(bb);
    var valueSize = bb.readInt();
    var valueBuf = bb.readBytes(valueSize);

    ByteString value;
    try {
      value = ByteString.copyFrom(valueBuf.nioBuffer());
    } finally {
      valueBuf.release();
    }

    var item = ImmutableItem.builder()
        .deadline(key.deadline())
        .id(key.id())
        .stats(stats)
        .value(value)
        .build();

    return Entry.newPendingEntry(item);
  }

  static byte[] toByteArray(Entry entry) {
    var item = entry.item();

    if (item == null) {
      var bytes = new byte[KEY_SIZE];
      writeKey(wrapByteBuffer(bytes), entry.key());
      return bytes;
    }

    var value = item.value();
    var bytes = new byte[KEY_SIZE + STATS_SIZE + 4 + value.size()];
    var buffer = wrapByteBuffer(bytes);
    writeKey(buffer, entry.key());
    writeStats(buffer, item.stats());
    buffer.putInt(value.size());
    value.copyTo(buffer);
    return bytes;
  }

  static Entry.Type entryTypeFromBits(int bits) {
    switch (bits) {
      case 0b00:
        return PENDING;

      case 0b11:
        return TOMBSTONE;
    }

    throw new IllegalArgumentException("unsupported type");
  }

  static int entryTypeToBits(Entry.Type type) {
    switch (type) {
      case PENDING:
        return 0b00;

      case TOMBSTONE:
        return 0b11;
    }
    throw new IllegalArgumentException("unsupported type");
  }

  static ByteBuffer wrapByteBuffer(byte[] array) {
    return ByteBuffer.wrap(array).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Value.Immutable
  public interface Footer {
    long pendingCount();

    long tombstoneCount();

    long maxId();
  }
}
