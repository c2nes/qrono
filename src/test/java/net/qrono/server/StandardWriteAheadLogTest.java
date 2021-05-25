package net.qrono.server;

import com.google.common.base.Strings;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import net.qrono.server.data.Entry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Timestamp;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StandardWriteAheadLogTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public void benchmark(long n, int valueSize, Duration syncDuration, boolean memoizeValue)
      throws IOException {
    var baseTime = System.currentTimeMillis();
    IdGenerator generator = new AtomicLong()::incrementAndGet;
    var id = generator.generateId();
    var deadline = ImmutableTimestamp.of(baseTime);
    var value = Unpooled.copiedBuffer(Strings.repeat("0", valueSize), CharsetUtil.UTF_8);
    Supplier<Entry> entrySupplier = () -> Entry.newPendingEntry(
        ImmutableItem.builder()
            .deadline(deadline)
            .id(id)
            .stats(ImmutableItem.Stats.builder()
                .enqueueTime(deadline)
                .requeueTime(Timestamp.ZERO)
                .dequeueCount(0)
                .build())
            .value(value)
            .build());

    if (memoizeValue) {
      entrySupplier = new Memoize<>(entrySupplier);
    }

    var name = new SegmentName(0, 0);
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

  @Ignore("benchmark")
  @Test
  public void benchmark() throws IOException {
    benchmark(1_000_000, 128, Duration.ofSeconds(1), true);
    System.out.println(Strings.repeat("-", 40));

    for (var size : List.of(32, 256, 1024)) {
      for (var duration : List
          .of(Duration.ofMillis(100), Duration.ofSeconds(1), Duration.ofSeconds(5))) {
        for (var memoize : List.of(false, true)) {
          benchmark(1_000_000, size, duration, memoize);
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