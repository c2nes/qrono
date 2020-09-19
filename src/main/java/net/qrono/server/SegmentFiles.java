package net.qrono.server;

import java.nio.file.Path;

class SegmentFiles {
  private static final String LOG_SUFFIX = ".log";
  private static final String CLOSED_LOG_SUFFIX = ".closed-log";
  private static final String INDEX_SUFFIX = ".idx";
  private static final String TMP_SUFFIX = ".tmp";

  static boolean isLogPath(Path path) {
    return path.getFileName().toString().endsWith(LOG_SUFFIX);
  }

  static boolean isClosedLogPath(Path path) {
    return path.getFileName().toString().endsWith(CLOSED_LOG_SUFFIX);
  }

  static boolean isAnyLogPath(Path path) {
    return isLogPath(path) || isClosedLogPath(path);
  }

  static boolean isTemporaryPath(Path path) {
    return path.getFileName().toString().endsWith(TMP_SUFFIX);
  }

  static boolean isIndexPath(Path path) {
    return path.getFileName().toString().endsWith(INDEX_SUFFIX);
  }

  static Path getLogPath(Path directory, SegmentName segmentName) {
    return directory.resolve(segmentName.toPath(LOG_SUFFIX));
  }

  static Path getClosedLogPath(Path directory, SegmentName segmentName) {
    return directory.resolve(segmentName.toPath(CLOSED_LOG_SUFFIX));
  }

  static Path getIndexPath(Path directory, SegmentName segmentName) {
    return directory.resolve(segmentName.toPath(INDEX_SUFFIX));
  }

  static Path getTemporaryPath(Path path) {
    return path.getParent().resolve(path.getFileName().toString() + TMP_SUFFIX);
  }
}
