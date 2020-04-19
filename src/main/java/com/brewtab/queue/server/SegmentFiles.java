package com.brewtab.queue.server;

import java.nio.file.Path;

class SegmentFiles {
  private static final String LOG_SUFFIX = ".log";
  private static final String CLOSED_LOG_SUFFIX = ".closed-log";
  private static final String TOMBSTONE_INDEX_SUFFIX = ".t-idx";
  private static final String PENDING_INDEX_SUFFIX = ".p-idx";
  private static final String COMBINED_INDEX_SUFFIX = ".c-idx";

  static boolean isLogPath(Path path) {
    return path.getFileName().endsWith(LOG_SUFFIX);
  }

  static boolean isClosedLogPath(Path path) {
    return path.getFileName().endsWith(CLOSED_LOG_SUFFIX);
  }

  static boolean isTombstoneIndexPath(Path path) {
    return path.getFileName().endsWith(TOMBSTONE_INDEX_SUFFIX);
  }

  static boolean isPendingIndexPath(Path path) {
    return path.getFileName().endsWith(PENDING_INDEX_SUFFIX);
  }

  static boolean isCombinedIndexPath(Path path) {
    return path.getFileName().endsWith(COMBINED_INDEX_SUFFIX);
  }

  static Path getLogPath(Path directory, String base) {
    return directory.resolve(base + LOG_SUFFIX);
  }

  static Path getClosedLogPath(Path directory, String base) {
    return directory.resolve(base + CLOSED_LOG_SUFFIX);
  }

  static Path getTombstoneIndexPath(Path directory, String base) {
    return directory.resolve(base + TOMBSTONE_INDEX_SUFFIX);
  }

  static Path getPendingIndexPath(Path directory, String base) {
    return directory.resolve(base + PENDING_INDEX_SUFFIX);
  }

  static Path getCombinedIndexPath(Path directory, String base) {
    return directory.resolve(base + COMBINED_INDEX_SUFFIX);
  }
}
