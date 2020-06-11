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
    return writeStats(BYTE_BUFFER, bb, stats);
  }

  static int writeStats(ByteBuf bb, Item.Stats stats) {
    return writeStats(BYTE_BUF, bb, stats);
  }

  private static <B> int writeStats(ByteBufferAdapter<B> adapter, B bb, Item.Stats stats) {
    long enqueueTime = stats.enqueueTime().millis();
    long requeueTime = stats.requeueTime().millis();
    int dequeueCount = stats.dequeueCount();

    long upper = (enqueueTime << 16) | (requeueTime >>> 32);
    long lower = (requeueTime << 32) | dequeueCount;
    adapter.putLong(bb, upper);
    adapter.putLong(bb, lower);

    return STATS_SIZE;
  }

  static Item.Stats readStats(ByteBuffer bb) {
    return statsFromBits(bb.getLong(), bb.getLong());
  }

  static Item.Stats readStats(ByteBuf bb) {
    return statsFromBits(bb.readLong(), bb.readLong());
  }

  private static Item.Stats statsFromBits(long upper, long lower) {
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
    return writeKey(BYTE_BUFFER, bb, key);
  }

  static int writeKey(ByteBuf bb, Entry.Key key) {
    return writeKey(BYTE_BUF, bb, key);
  }

  private static <B> int writeKey(ByteBufferAdapter<B> adapter, B bb, Entry.Key key) {
    var deadline = key.deadline().millis();
    var id = key.id();
    var type = key.entryType();

    long upper = (deadline << 2) | (id >>> 62);
    long lower = (id << 2) | entryTypeToBits(type);
    adapter.putLong(bb, upper);
    adapter.putLong(bb, lower);

    return KEY_SIZE;
  }

  static Entry.Key readKey(ByteBuffer bb) {
    return keyFromBits(bb.getLong(), bb.getLong());
  }

  static Entry.Key readKey(ByteBuf bb) {
    return keyFromBits(bb.readLong(), bb.readLong());
  }

  private static Entry.Key keyFromBits(long upper, long lower) {
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

  private static Entry.Type entryTypeFromBits(int bits) {
    switch (bits) {
      case 0b00:
        return PENDING;

      case 0b11:
        return TOMBSTONE;
    }

    throw new IllegalArgumentException("unsupported type");
  }

  private static int entryTypeToBits(Entry.Type type) {
    switch (type) {
      case PENDING:
        return 0b00;

      case TOMBSTONE:
        return 0b11;
    }
    throw new IllegalArgumentException("unsupported type");
  }

  private static final ByteBufferAdapter<ByteBuffer> BYTE_BUFFER = ByteBuffer::putLong;
  private static final ByteBufferAdapter<ByteBuf> BYTE_BUF = ByteBuf::writeLong;

  private interface ByteBufferAdapter<B> {
    void putLong(B bb, long value);
  }

  @Value.Immutable
  public interface Footer {
    long pendingCount();

    long tombstoneCount();

    long maxId();
  }
}
