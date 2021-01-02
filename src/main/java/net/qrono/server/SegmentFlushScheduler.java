package net.qrono.server;

public class SegmentFlushScheduler {
  private final long flushThreshold;
  private long totalSize = 0;
  private long segmentCount = 0;

  public SegmentFlushScheduler(long flushThreshold) {
    this.flushThreshold = flushThreshold;
  }

  public synchronized Handle register() {
    segmentCount += 1;
    return new Handle();
  }

  private synchronized void handleUpdate(Handle handle, long currentSize) {
    long oldSize = handle.size;
    totalSize = totalSize - oldSize + currentSize;
    handle.size = currentSize;
  }

  private synchronized boolean handleIsFlushRequired(Handle handle, long currentSize) {
    // Update size
    handleUpdate(handle, currentSize);

    // Determine if flush is required. We never flush if total utilization is less than the
    // configured threshold, then we start flushing segments which are larger than average.
    if (totalSize < flushThreshold) {
      return false;
    }

    long averageSize = totalSize / segmentCount;
    return handle.size >= averageSize;
  }

  private synchronized void handleRemove(Handle handle) {
    totalSize -= handle.size;
    segmentCount -= 1;
  }

  public class Handle {
    private long size;

    public boolean isFlushRequired(long currentSize) {
      return handleIsFlushRequired(this, currentSize);
    }

    public void update(long currentSize) {
      handleUpdate(this, currentSize);
    }

    public void cancel() {
      handleRemove(this);
    }
  }
}
