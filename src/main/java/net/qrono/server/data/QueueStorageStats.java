package net.qrono.server.data;

import org.immutables.value.Value;

@Value.Immutable
public interface QueueStorageStats {
  long persistedPendingCount();

  long persistedTombstoneCount();

  long bufferedPendingCount();

  long bufferedTombstoneCount();

  default long totalPendingCount() {
    return persistedPendingCount() + bufferedPendingCount();
  }

  default long totalTombstoneCount() {
    return persistedTombstoneCount() + bufferedTombstoneCount();
  }
}
