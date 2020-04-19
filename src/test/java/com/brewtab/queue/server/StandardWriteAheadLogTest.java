package com.brewtab.queue.server;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Stats;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StandardWriteAheadLogTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public void test(long n, int valueSize, Duration syncDuration, boolean memoizeValue)
      throws IOException {
    var baseTime = System.currentTimeMillis();
    var generator = new IdGeneratorImpl(baseTime);
    var id = generator.generateId();
    var deadline = Timestamps.fromMillis(baseTime);
    var value = ByteString.copyFromUtf8(Strings.repeat("0", valueSize));
    Supplier<Entry> entrySupplier = () -> Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(deadline)
            .setId(id)
            .setStats(Stats.newBuilder()
                .setEnqueueTime(deadline))
            .setValue(value))
        .build();

    if (memoizeValue) {
      entrySupplier = new Memoize<>(entrySupplier);
    }

    var name = "log-test-" + UUID.randomUUID();
    var log = StandardWriteAheadLog.create(temporaryFolder.getRoot().toPath(), name, syncDuration);
    var start = Instant.now();
    for (var i = 0; i < n; i++) {
      log.append(entrySupplier.get());
    }
    var duration = Duration.between(start, Instant.now());
    var seconds = 1e-9 * duration.toNanos();
    var entryPerSec = (long) (n / seconds);
    var bytesPerSec = valueSize * n / seconds;
    var mbPerSec = bytesPerSec / (1024 * 1024);
    System.out.format("%8d / %5d %10s / %10s || %14s / %7d / %.2f MB/s\n",
        n, valueSize, memoizeValue ? "(memoized)" : "", syncDuration, duration, entryPerSec,
        mbPerSec);
  }

  @Test
  public void test() throws IOException {
    test(1_000_000, 128, Duration.ofSeconds(1), true);
    System.out.println(Strings.repeat("-", 40));

    for (var size : List.of(32, 256, 1024)) {
      for (var duration : List
          .of(Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(5))) {
        for (var memoize : List.of(false, true)) {
          test(1_000_000, size, duration, memoize);
        }
      }
    }
  }

  static class Memoize<T> implements Supplier<T> {
    private final Supplier<T> delegate;
    private T value;
    private boolean initialized;

    Memoize(Supplier<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public T get() {
      if (!initialized) {
        value = delegate.get();
        initialized = true;
      }
      return value;
    }
  }
}