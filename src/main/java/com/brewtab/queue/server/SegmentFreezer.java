package com.brewtab.queue.server;

import com.google.common.util.concurrent.Uninterruptibles;

public interface SegmentFreezer {
  void freeze(WritableSegment segment) throws InterruptedException;

  default void freezeUninterruptibly(WritableSegment segment) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          freeze(segment);
          return;
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
