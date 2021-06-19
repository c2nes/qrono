package net.qrono.server.data;

import com.google.common.base.MoreObjects;
import io.netty.buffer.ByteBuf;
import java.util.Objects;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import net.qrono.server.data.Item.Stats;

public class MutableEntry implements Entry {

  // Key
  public long deadlineMillis = 0;
  public long id = 0;
  // Item stats
  public long enqueueTimeMillis = 0;
  public long requeueTimeMillis = 0;
  public int dequeueCount = 0;
  // Item value
  public ByteBuf value = null;

  public MutableEntry() {
  }

  public MutableEntry(Entry entry) {
    if (entry instanceof MutableEntry) {
      copyFrom((MutableEntry) entry);
    } else {
      copyFrom(entry);
    }
  }

  public MutableEntry(MutableEntry entry) {
    copyFrom(entry);
  }

  private void copyFrom(Entry entry) {
    reset();
    deadlineMillis = entry.key().deadline().millis();
    id = entry.key().id();
    var item = entry.item();
    if (item != null) {
      enqueueTimeMillis = item.stats().enqueueTime().millis();
      requeueTimeMillis = item.stats().requeueTime().millis();
      dequeueCount = item.stats().dequeueCount();
      value = item.value();
    }
  }

  private void copyFrom(MutableEntry entry) {
    deadlineMillis = entry.deadlineMillis;
    id = entry.id;
    enqueueTimeMillis = entry.enqueueTimeMillis;
    requeueTimeMillis = entry.requeueTimeMillis;
    dequeueCount = entry.dequeueCount;
    value = entry.value;
  }

  private final Timestamp deadline = new T(() -> deadlineMillis);
  private final Timestamp enqueueTime = new T(() -> enqueueTimeMillis);
  private final Timestamp requeueTime = new T(() -> requeueTimeMillis);

  private static class T implements Timestamp {
    private final LongSupplier millis;

    private T(LongSupplier millis) {
      this.millis = millis;
    }

    @Override
    public long millis() {
      return millis.getAsLong();
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(millis());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Timestamp)) {
        return false;
      }
      Timestamp o = (Timestamp) obj;
      return millis() == o.millis();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper("Timestamp")
          .add("millis", millis())
          .toString();
    }
  }

  private final Key key = new Key() {
    @Override
    public Timestamp deadline() {
      return deadline;
    }

    @Override
    public long id() {
      return id;
    }

    @Override
    public Type entryType() {
      return value == null ? Type.TOMBSTONE : Type.PENDING;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Key)) {
        return false;
      }
      return compareTo((Key) obj) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(deadline, id, entryType());
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper("Entry.Key")
          .add("deadline", deadlineMillis)
          .add("id", id)
          .add("entryType", entryType())
          .toString();
    }
  };

  private final Item.Stats stats = new Stats() {
    @Override
    public Timestamp enqueueTime() {
      return enqueueTime;
    }

    @Override
    public Timestamp requeueTime() {
      return requeueTime;
    }

    @Override
    public int dequeueCount() {
      return dequeueCount;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Stats)) {
        return false;
      }
      Stats o = (Stats) obj;
      return enqueueTime.equals(o.enqueueTime())
          && requeueTime.equals(o.requeueTime())
          && dequeueCount == o.dequeueCount();
    }

    @Override
    public int hashCode() {
      return Objects.hash(enqueueTime, requeueTime, dequeueCount);
    }
  };

  private final Item item = new Item() {
    @Override
    public Timestamp deadline() {
      return deadline;
    }

    @Override
    public long id() {
      return id;
    }

    @Override
    public Stats stats() {
      return stats;
    }

    @Override
    public ByteBuf value() {
      return value;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof Item)) {
        return false;
      }
      Item o = (Item) obj;
      return deadline().equals(o.deadline())
          && id() == o.id()
          && stats().equals(o.stats())
          && value().equals(o.value());
    }

    @Override
    public int hashCode() {
      return Objects.hash(deadline, id, stats, value);
    }
  };

  @Override
  public Key key() {
    return key;
  }

  @Nullable
  @Override
  public Item item() {
    return value == null ? null : item;
  }

  @Override
  public boolean isPending() {
    return value != null;
  }

  @Override
  public boolean isTombstone() {
    return value == null;
  }

  public void reset() {
    deadlineMillis = 0;
    id = 0;
    enqueueTimeMillis = 0;
    requeueTimeMillis = 0;
    dequeueCount = 0;
    value = null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o instanceof MutableEntry) {
      MutableEntry that = (MutableEntry) o;
      return deadlineMillis == that.deadlineMillis && id == that.id
          && enqueueTimeMillis == that.enqueueTimeMillis
          && requeueTimeMillis == that.requeueTimeMillis
          && dequeueCount == that.dequeueCount && Objects.equals(value, that.value);
    }

    if (o instanceof Entry) {
      return compareTo((Entry) o) == 0;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(deadlineMillis, id, enqueueTimeMillis, requeueTimeMillis, dequeueCount, value);
  }

  @Override
  public int compareTo(Entry o) {
    if (o instanceof MutableEntry) {
      return compareTo((MutableEntry) o);
    }
    return Entry.super.compareTo(o);
  }

  private int compareTo(MutableEntry o) {
     /*
    int cmp = deadline().compareTo(o.deadline());
      if (cmp != 0) {
        return cmp;
      }

      cmp = Long.compare(id(), o.id());
      if (cmp != 0) {
        return cmp;
      }

      return entryType().compareTo(o.entryType());
     */
    if (deadlineMillis != o.deadlineMillis) {
      if (deadlineMillis < o.deadlineMillis) {
        return -1;
      } else {
        return 1;
      }
    }

    if (id != o.id) {
      if (id < o.id) {
        return -1;
      } else {
        return 1;
      }
    }

    // Tombstone < Pending
    if (value == null) {
      if (o.value != null) {
        return -1;
      }
    } else {
      if (o.value == null) {
        return 1;
      }
    }

    return 0;
  }

  @Override
  public String toString() {
    return "MutableEntry{" +
        "deadlineMillis=" + deadlineMillis +
        ", id=" + id +
        ", enqueueTimeMillis=" + enqueueTimeMillis +
        ", requeueTimeMillis=" + requeueTimeMillis +
        ", dequeueCount=" + dequeueCount +
        ", entryType=" + key().entryType() +
        '}';
  }
}
