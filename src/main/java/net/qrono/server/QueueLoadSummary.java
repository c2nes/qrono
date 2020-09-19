package net.qrono.server;

public class QueueLoadSummary {
  private final long maxId;

  QueueLoadSummary(long maxId) {
    this.maxId = maxId;
  }

  public long getMaxId() {
    return maxId;
  }
}
