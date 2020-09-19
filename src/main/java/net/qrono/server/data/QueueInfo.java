package net.qrono.server.data;

import org.immutables.value.Value;

@Value.Immutable
public interface QueueInfo {
  long pendingCount();

  long dequeuedCount();
}
