package net.qrono.server.util;

import com.google.common.base.Preconditions;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataSize {
  private static final Pattern PARSER = Pattern.compile("(\\d+)\\s*([a-zA-Z]*)$");
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

  public static DataSize fromString(String value) {
    Matcher matcher = PARSER.matcher(value);
    Preconditions.checkArgument(matcher.matches(), "unsupported format");
    long base = Long.parseLong(matcher.group(1));
    long unit = parseUnit(matcher.group(2).strip());
    return fromBytes(base * unit);
  }

  private static long parseUnit(String unit) {
    if (unit.isEmpty() || unit.equals("B")) {
      return 1;
    }
    if (unit.endsWith("B") && unit.length() == 2) {
      switch (Character.toLowerCase(unit.charAt(0))) {
        case 'k':
          return 1024L;
        case 'm':
          return 1024L * 1024;
        case 'g':
          return 1024L * 1024 * 1024;
        case 't':
          return 1024L * 1024 * 1024 * 1024;
        case 'p':
          return 1024L * 1024 * 1024 * 1024 * 1024;
      }
    }
    throw new IllegalArgumentException("unknown unit: " + unit);
  }

  public static DataSize fromBytes(long bytes) {
    return new DataSize(bytes);
  }
}
