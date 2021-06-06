package net.qrono.server.data;

import io.netty.buffer.ByteBuf;
import org.immutables.value.Value;

@Value.Immutable
@Value.Enclosing
public interface Item extends Comparable<Item>, ForwardingReferenceCounted<Item> {
  Timestamp deadline();

  long id();

  Stats stats();

  ByteBuf value();

  @Override
  default ByteBuf ref() {
    return value();
  }

  @Override
  default Item self() {
    return this;
  }

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
