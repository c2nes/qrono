package net.qrono.server;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Entry.Type;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Item;
import net.qrono.server.data.Timestamp;
import net.qrono.server.util.DataSize;
import org.junit.Test;

@SuppressWarnings("UnstableApiUsage")
public class QronoBenchmark {
  private Duration benchDataEnqueue(QueueFactory queueFactory, int n, double rate)
      throws Exception {
    var value = ByteString.copyFrom(Strings.repeat("a", 16), UTF_8);
    var q = queueFactory.createQueue("q0");
    var entries = new ArrayList<Entry>();
    var m = 100;
    var stats = ImmutableItem.Stats.builder()
        .enqueueTime(Timestamp.ZERO)
        .requeueTime(Timestamp.ZERO)
        .dequeueCount(0)
        .build();

    var entryBase = ImmutableItem.builder()
        .stats(stats)
        .value(value);

    var pool = new ArrayList<MutableEntry>();
    for (int i = 0; i < m; i++) {
      var entry = new MutableEntry();
      entry.stats = stats;
      entry.value = value;
      entry.type = Type.PENDING;
      entry.id = 0;
      entry.deadlineMillis = 0;
      pool.add(entry);
    }

    try {
      var limit = RateLimiter.create(rate);
      var sw = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
//        var item = ImmutableItem.builder()
//            .id(i)
//            .deadline(ImmutableTimestamp.of(i))
//            .stats(ImmutableItem.Stats.builder()
//                .enqueueTime(Timestamp.ZERO)
//                .requeueTime(Timestamp.ZERO)
//                .dequeueCount(0)
//                .build())
//            .value(ByteString.copyFrom(value))
//            .build();

        var entry = pool.get(i % m);
        entry.id = i;
        entry.deadlineMillis = i;
        entries.add(entry);

        if (entries.size() == m || i == (n - 1)) {
          limit.acquire(m);
          q.data.write(entries);
          entries.clear();
        }
      }

      return sw.stop().elapsed();
    } finally {
      q.stopAsync().awaitTerminated();
      q.delete();
    }
  }

  private Duration benchEnqueue(QueueFactory queueFactory, int n, double rate) throws Exception {
    var value = Strings.repeat("a", 16).getBytes(UTF_8);
    var q = queueFactory.createQueue("q0");
    try {
      var latch = new CountDownLatch(n);
      var limit = RateLimiter.create(rate);
      var sw = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
        q.enqueueAsync(ByteString.copyFrom(value), null).thenRun(latch::countDown);
        limit.acquire();
      }
      System.out.println("Submit rate: " + n / (1e-9 * sw.elapsed().toNanos())
          + ", awaiting: " + latch.getCount());
      while (!latch.await(500, TimeUnit.MILLISECONDS)) {
        System.out.println(latch.getCount());
      }
      return sw.stop().elapsed();
    } finally {
      q.stopAsync().awaitTerminated();
      q.delete();
    }
  }

  @Test
  public void test() throws Exception {
    var root = Paths.get("/tmp/qrono-benchmark");
    var ioScheduler = new ExecutorIOScheduler(
        Executors.newFixedThreadPool(4, new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Qrono-IOWorker-%d")
            .build()));

    var queuesDirectory = root.resolve("queues");
    Files.createDirectories(queuesDirectory);

    var idGenerator = new PersistentIdGenerator(root.resolve("last-id"));
    idGenerator.startAsync().awaitRunning();

    var workingSetDirectory = root.resolve("working");
    var workingSet = new DiskBackedWorkingSet(
        workingSetDirectory,
        toIntExact(DataSize.fromString("1GB").bytes()),
        ioScheduler);

    workingSet.startAsync().awaitRunning();

    var segmentFlushScheduler = new SegmentFlushScheduler(DataSize.fromString("256MB").bytes());

    var queueFactory = new QueueFactory(
        queuesDirectory,
        idGenerator,
        ioScheduler,
        workingSet,
        segmentFlushScheduler);

    System.out.println("Warming up...");
    var warmupRate = 50_000;
    var warmupDuration = Duration.ofSeconds(15);
    benchDataEnqueue(queueFactory, (int) (warmupDuration.toSeconds() * warmupRate), warmupRate);

    System.out.println("Starting!");
    CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS)
        .execute(() -> System.out.println("Go!"));

    var n = 25_000_000;
    var delta = benchDataEnqueue(queueFactory, n, 1_000_000);
    var deltaSecs = 1e-9 * delta.toNanos();
    System.out.printf("%,d\n", (int) (n / deltaSecs));
  }

  @Test
  public void benchSort() {
    var value = ByteString.copyFrom(Strings.repeat("a", 16), UTF_8);
    var entries = new ArrayList<Entry>();
    var n = 30_000_000;

    for (int i = 0; i < n; i++) {
      var item = ImmutableItem.builder()
          .id(i)
          .deadline(ImmutableTimestamp.of(i))
          .stats(ImmutableItem.Stats.builder()
              .enqueueTime(Timestamp.ZERO)
              .requeueTime(Timestamp.ZERO)
              .dequeueCount(0)
              .build())
          .value(value)
          .build();

      entries.add(Entry.newPendingEntry(item));
    }

    var sw = Stopwatch.createStarted();
    System.out.println(ImmutableSortedSet.copyOf(entries).size());
    System.out.println(sw);
  }

  private static class MutableEntry implements Entry {
    long deadlineMillis;
    long id;
    Type type;
    ByteString value;
    Item.Stats stats;

    private final Timestamp deadline = new Timestamp() {
      @Override
      public long millis() {
        return deadlineMillis;
      }
    };

    private final Key key = new Key() {
      @Override
      public Timestamp deadline() {
        return deadline;
      }

      @Override
      public long id() {
        return id;
      }

      @Override
      public Type entryType() {
        return type;
      }
    };

    private final Item item = new Item() {
      @Override
      public Timestamp deadline() {
        return deadline;
      }

      @Override
      public long id() {
        return id;
      }

      @Override
      public Stats stats() {
        return stats;
      }

      @Override
      public ByteString value() {
        return value;
      }
    };

    @Override
    public Key key() {
      return key;
    }

    @Nullable
    @Override
    public Item item() {
      return item;
    }
  }
}
