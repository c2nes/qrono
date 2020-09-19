package net.qrono.server.data;

import com.google.protobuf.ByteString;
import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public interface Item extends Comparable<Item> {
  Timestamp deadline();

  long id();

  Stats stats();

  ByteString value();

  @Override
  default int compareTo(Item o) {
    int cmp = deadline().compareTo(o.deadline());
    if (cmp != 0) {
      return cmp;
    }

    return Long.compare(id(), o.id());
  }

  @Value.Immutable
  interface Stats {
    Timestamp enqueueTime();

    Timestamp requeueTime();

    int dequeueCount();
  }
}
