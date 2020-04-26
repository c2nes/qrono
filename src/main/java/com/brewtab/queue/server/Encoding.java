package com.brewtab.queue.server;

import com.brewtab.queue.Api;
import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.nio.ByteBuffer;

final class Encoding {
  // Entry:
  //   key          EntryKey(128)
  //  [Type:Pending]
  //   stats        Stats(128)
  //   value_length uint32
  //   value        bytes(value_length)
  // 
  // EntryKey(128):
  //   <reserved> bits(14)
  //   deadline   Timestamp(48)
  //   id         uint64
  //   type       bits(2) {00: Pending,
  //                       01: Reserved,
  //                       10: Reserved,
  //                       11: Tombstone}
  // 
  // Timestamp(48):
  //   millis bits(48)
  // 
  // Stats(128):
  //   enqueue_time  Timestamp(48)
  //   requeue_time  Timestamp(48)
  //   dequeue_count uint32
  // 
  // Footer(320):
  //   pending_count   uint64
  //   tombstone_count uint64
  //   last_key        EntryKey(128)
  //   max_id          uint64

  //  [Type:Pending]
  //   stats        Stats(128)
  //   value_length uint32
  //   value        bytes(value_length)
  static class PendingPreamble {
    static final int SIZE = 20;

    final Stats stats;
    final int valueLength;

    PendingPreamble(Stats stats, int valueLength) {
      this.stats = stats;
      this.valueLength = valueLength;
    }

    int write(ByteBuffer bb) {
      stats.write(bb);
      bb.putInt(valueLength);
      return SIZE;
    }

    static PendingPreamble read(ByteBuffer bb) {
      return new PendingPreamble(
          Stats.read(bb),
          bb.getInt());
    }

    static PendingPreamble fromPending(Item item) {
      return new PendingPreamble(
          Stats.fromItemStats(item.getStats()),
          item.getValue().size());
    }
  }

  // Stats(128):
  //   enqueue_time  Timestamp(48)
  //   requeue_time  Timestamp(48)
  //   dequeue_count uint32
  static class Stats {
    static final int SIZE = 16;
    private final long enqueueTime;
    private final long requeueTime;
    private final long dequeueCount;

    Stats(long enqueueTime, long requeueTime, long dequeueCount) {
      this.enqueueTime = enqueueTime;
      this.requeueTime = requeueTime;
      this.dequeueCount = dequeueCount;
    }

    int write(ByteBuffer bb) {
      long upper = (enqueueTime << 16) | (requeueTime >>> 32);
      long lower = (requeueTime << 32) | dequeueCount;
      bb.putLong(upper);
      bb.putLong(lower);
      return SIZE;
    }

    Api.Stats toApiStats() {
      return Api.Stats.newBuilder()
          .setEnqueueTime(fromTimestamp(enqueueTime))
          .setRequeueTime(fromTimestamp(requeueTime))
          .setDequeueCount((int) dequeueCount)
          .build();
    }

    static Stats read(ByteBuffer bb) {
      long upper = bb.getLong();
      long lower = bb.getLong();
      long enqueueTime = upper >>> 16;
      long requeueTime = ((upper & ((1L << 16) - 1)) << 32) | (lower >>> 32);
      long dequeueCount = (lower & ((1L << 32) - 1));
      return new Stats(enqueueTime, requeueTime, dequeueCount);
    }

    static Stats fromItemStats(Api.Stats stats) {
      return new Stats(
          toTimestamp(stats.getEnqueueTime()),
          toTimestamp(stats.getRequeueTime()),
          stats.getDequeueCount());
    }
  }

  // EntryKey(128):
  //   <reserved> bits(14)
  //   deadline   Timestamp(48)
  //   id         uint64
  //   type       bits(2) {00: Pending,
  //                       01: Reserved,
  //                       10: Reserved,
  //                       11: Tombstone}
  //
  static class Key {
    static final int SIZE = 16;
    final long deadline;
    final long id;
    final Type type;

    Key(long deadline, long id, Type type) {
      this.deadline = deadline;
      this.id = id;
      this.type = type;
    }

    int write(ByteBuffer bb) {
      long upper = (deadline << 2) | (id >>> 62);
      long lower = (id << 2) | type.bits;
      bb.putLong(upper);
      bb.putLong(lower);
      return SIZE;
    }

    Entry.Key toEntryKey() {
      return Entry.Key.newBuilder()
          .setDeadline(fromTimestamp(deadline))
          .setId(id)
          .build();
    }

    static Key read(ByteBuffer bb) {
      long upper = bb.getLong();
      long lower = bb.getLong();

      long deadline = (upper >>> 2) & ((1L << 48) - 1);
      long id = (lower >>> 2) | ((upper & 0b11) << 62);
      Type type = Type.fromBits(((int) lower) & 0b11);

      return new Key(deadline, id, type);
    }

    static Key fromEntry(Entry entry) {
      if (entry.hasTombstone()) {
        var tombstone = entry.getTombstone();
        long deadline = toTimestamp(tombstone.getDeadline());
        long id = tombstone.getId();
        return new Key(deadline, id, Type.TOMBSTONE);
      } else if (entry.hasPending()) {
        var pending = entry.getPending();
        long deadline = toTimestamp(pending.getDeadline());
        long id = pending.getId();
        return new Key(deadline, id, Type.PENDING);
      } else {
        throw new IllegalArgumentException();
      }
    }

    enum Type {
      PENDING(0b00),
      TOMBSTONE(0b11);

      private final int bits;

      Type(int bits) {
        this.bits = bits;
      }

      private static Type fromBits(int bits) {
        switch (bits) {
          case 0b00:
            return PENDING;
          case 0b11:
            return TOMBSTONE;
          default:
            throw new IllegalArgumentException();
        }
      }
    }
  }

  // Footer(320):
  //   pending_count   uint64
  //   tombstone_count uint64
  //   last_key        EntryKey(128)
  //   max_id          uint64
  static class Footer {
    static final int SIZE = 40;

    final long pendingCount;
    final long tombstoneCount;
    final Key lastKey;
    final long maxId;

    Footer(long pendingCount, long tombstoneCount, Key lastKey, long maxId) {
      this.pendingCount = pendingCount;
      this.tombstoneCount = tombstoneCount;
      this.lastKey = lastKey;
      this.maxId = maxId;
    }

    int write(ByteBuffer bb) {
      bb.putLong(pendingCount);
      bb.putLong(tombstoneCount);
      lastKey.write(bb);
      bb.putLong(maxId);
      return SIZE;
    }

    Api.Segment.Footer toApiFooter() {
      return Api.Segment.Footer.newBuilder()
          .setLastKey(lastKey.toEntryKey())
          .setMaxId(maxId)
          .setPendingCount(pendingCount)
          .setTombstoneCount(tombstoneCount)
          .build();
    }

    static Footer read(ByteBuffer bb) {
      return new Footer(
          bb.getLong(),
          bb.getLong(),
          Key.read(bb),
          bb.getLong());
    }
  }

  static Timestamp fromTimestamp(long timestamp) {
    return Timestamps.fromMillis(timestamp);
  }

  static long toTimestamp(Timestamp timestamp) {
    // TODO: Check deadline overflow
    return Timestamps.toMillis(timestamp);
  }
}
