package com.brewtab.queue.server;

import java.nio.file.Path;
import java.util.List;

class SegmentFiles {
  private static final String LOG_SUFFIX = ".log";
  private static final String CLOSED_LOG_SUFFIX = ".closed-log";
  private static final String TOMBSTONE_INDEX_SUFFIX = ".t-idx";
  private static final String PENDING_INDEX_SUFFIX = ".p-idx";
  private static final String COMBINED_INDEX_SUFFIX = ".c-idx";
  private static final String TMP_SUFFIX = ".tmp";

  private static final List<String> SUFFIXES = List.of(
      LOG_SUFFIX,
      CLOSED_LOG_SUFFIX,
      TOMBSTONE_INDEX_SUFFIX,
      PENDING_INDEX_SUFFIX,
      COMBINED_INDEX_SUFFIX
  );

  static boolean isLogPath(Path path) {
    return path.getFileName().toString().endsWith(LOG_SUFFIX);
  }

  static boolean isClosedLogPath(Path path) {
    return path.getFileName().toString().endsWith(CLOSED_LOG_SUFFIX);
  }

  static boolean isAnyLogPath(Path path) {
    return isLogPath(path) || isClosedLogPath(path);
  }

  static boolean isTombstoneIndexPath(Path path) {
    return path.getFileName().toString().endsWith(TOMBSTONE_INDEX_SUFFIX);
  }

  static boolean isPendingIndexPath(Path path) {
    return path.getFileName().toString().endsWith(PENDING_INDEX_SUFFIX);
  }

  static boolean isCombinedIndexPath(Path path) {
    return path.getFileName().toString().endsWith(COMBINED_INDEX_SUFFIX);
  }

  static boolean isTemporaryPath(Path path) {
    return path.getFileName().toString().endsWith(TMP_SUFFIX);
  }

  static boolean isAnyIndexPath(Path path) {
    return isTombstoneIndexPath(path)
        || isPendingIndexPath(path)
        || isCombinedIndexPath(path);
  }

  static Path getLogPath(Path directory, String segmentName) {
    return directory.resolve(segmentName + LOG_SUFFIX);
  }

  static Path getClosedLogPath(Path directory, String segmentName) {
    return directory.resolve(segmentName + CLOSED_LOG_SUFFIX);
  }

  static Path getTombstoneIndexPath(Path directory, String segmentName) {
    return directory.resolve(segmentName + TOMBSTONE_INDEX_SUFFIX);
  }

  static Path getPendingIndexPath(Path directory, String segmentName) {
    return directory.resolve(segmentName + PENDING_INDEX_SUFFIX);
  }

  static Path getCombinedIndexPath(Path directory, String segmentName) {
    return directory.resolve(segmentName + COMBINED_INDEX_SUFFIX);
  }

  static Path getTemporaryPath(Path path) {
    return path.getParent().resolve(path.getFileName().toString() + TMP_SUFFIX);
  }

  static String getSegmentNameFromPath(Path path) {
    String basename = path.getFileName().toString();
    for (String suffix : SUFFIXES) {
      if (basename.endsWith(suffix)) {
        return basename.substring(0, basename.length() - suffix.length());
      }
    }
    throw new IllegalArgumentException("unrecognized segment file");
  }
}
