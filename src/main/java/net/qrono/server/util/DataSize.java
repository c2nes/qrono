package net.qrono.server.util;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Objects;

public class DataSize {
  private final long bytes;

  private DataSize(long bytes) {
    this.bytes = bytes;
  }

  public long bytes() {
    return bytes;
  }

  @Override
  public String toString() {
    var suffixes = List.of("B", "kB", "MB", "GB", "TB", "PB");
    double amount = bytes;
    for (String suffix : suffixes.subList(0, suffixes.size() - 1)) {
      if (amount < 1024) {
        return new DecimalFormat("####.#").format(amount) + suffix;
      }
      amount = amount / 1024;
    }
    return String.format("%.1g%s", amount, suffixes.get(suffixes.size() - 1));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataSize dataSize = (DataSize) o;
    return bytes == dataSize.bytes;
  }

  @Override
  public int hashCode() {
    return Objects.hash(bytes);
  }

  public static DataSize fromBytes(long bytes) {
    return new DataSize(bytes);
  }
}
