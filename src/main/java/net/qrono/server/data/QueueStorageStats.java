package net.qrono.server.data;

import org.immutables.value.Value;

@Value.Immutable
public interface QueueStorageStats {
  long persistedPendingCount();

  long persistedTombstoneCount();

  long bufferedPendingCount();

  long bufferedTombstoneCount();
}
