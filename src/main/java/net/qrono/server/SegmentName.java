package net.qrono.server;

import java.nio.file.Path;
import java.util.Objects;

public class SegmentName {
  private final int level;
  private final long id;

  public SegmentName(int level, long id) {
    this.level = level;
    this.id = id;
  }

  public int level() {
    return level;
  }

  public long id() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentName that = (SegmentName) o;
    return level == that.level &&
        id == that.id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(level, id);
  }

  @Override
  public String toString() {
    return "SegmentName{" +
        "level=" + level +
        ", id=" + id +
        '}';
  }

  public Path toPath(String suffix) {
    return Path.of(String.format("%d-%016x%s", level, id, suffix));
  }

  public static SegmentName fromPath(Path path) {
    String basename = path.getFileName().toString();

    int endOfLevel = basename.indexOf('-');
    if (endOfLevel < 0) {
      throw new IllegalArgumentException("invalid path");
    }

    int endOfName = basename.indexOf('.');
    if (endOfName < 0) {
      endOfName = basename.length();
    }

    int level = Integer.parseInt(basename.substring(0, endOfLevel));
    long id = Long.parseLong(basename.substring(endOfLevel + 1, endOfName), 16);
    return new SegmentName(level, id);
  }
}
