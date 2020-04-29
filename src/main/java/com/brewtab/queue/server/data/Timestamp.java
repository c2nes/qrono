package com.brewtab.queue.server.data;

import com.google.common.base.Preconditions;
import org.immutables.value.Value;

@Value.Immutable
public interface Timestamp extends Comparable<Timestamp> {
  Timestamp ZERO = ImmutableTimestamp.of(0);

  @Value.Parameter
  long millis();

  @Override
  default int compareTo(Timestamp o) {
    return Long.compare(millis(), o.millis());
  }

  @Value.Check
  default void check() {
    Preconditions.checkArgument(millis() < (1L << 48));
  }
}
