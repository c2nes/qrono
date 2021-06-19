package net.qrono.server;

import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.Unpooled;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import net.qrono.server.data.Entry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.MutableEntry;
import net.qrono.server.data.Timestamp;
import net.qrono.server.util.DataSize;

@SuppressWarnings("UnstableApiUsage")
public class QronoBenchmark {
  private Duration benchDataEnqueue(QueueFactory queueFactory, int n, double rate)
      throws Exception {
    var value = Unpooled.copiedBuffer(Strings.repeat("a", 16), UTF_8);
    var q = queueFactory.createQueue("q0");
    var entries = new ArrayList<Entry>();
    var m = 100;

    var pool = new ArrayList<MutableEntry>();
    for (int i = 0; i < m; i++) {
      var entry = new MutableEntry();
      entry.enqueueTimeMillis = 0;
      entry.requeueTimeMillis = 0;
      entry.dequeueCount = 0;
      entry.value = value;
      entry.id = 0;
      entry.deadlineMillis = 0;
      pool.add(entry);
    }

    try {
      var limit = RateLimiter.create(rate);
      var sw = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
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

  private Duration benchQueueEnqueue(QueueFactory queueFactory, int n, double rate)
      throws Exception {

    var value = Unpooled.copiedBuffer(Strings.repeat("a", 16), UTF_8);
    var q = queueFactory.createQueue("q0");
    var latch = new CountDownLatch(n);

    try {
      var limit = RateLimiter.create(rate);
      var sw = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
        limit.acquire();
        q.enqueueAsync(value, null).thenRun(latch::countDown);
      }
      latch.await();
      return sw.stop().elapsed();
    } finally {
      q.stopAsync().awaitTerminated();
      q.delete();
    }
  }

  private Duration benchEnqueue(QueueFactory queueFactory, int n, double rate) throws Exception {
    var value = Unpooled.copiedBuffer(Strings.repeat("a", 16), UTF_8);
    var q = queueFactory.createQueue("q0");
    try {
      var latch = new CountDownLatch(n);
      var limit = RateLimiter.create(rate);
      var sw = Stopwatch.createStarted();
      for (int i = 0; i < n; i++) {
        q.enqueueAsync(value, null).thenRun(latch::countDown);
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

  private static boolean continueRunning() {
    var con = System.console();
    if (con == null) {
      return false;
    }

    do {
      var resp = con.readLine("Continue? [y/n] ");
      if (resp == null) {
        return false;
      }
      switch (resp) {
        case "y":
          return true;
        case "n":
          return false;
      }
    } while (true);
  }

  public void queueEnqueue() throws Exception {
    var root = Paths.get("/tmp/qrono-benchmark");
    var ioScheduler = new ExecutorTaskScheduler(
        Executors.newFixedThreadPool(4, new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Qrono-IOWorker-%d")
            .build()));

    var cpuScheduler = new ExecutorTaskScheduler(ForkJoinPool.commonPool());

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
        cpuScheduler,
        workingSet,
        segmentFlushScheduler);

    try {
      System.out.println("Warming up...");
      var warmupRate = 50_000;
      var warmupDuration = Duration.ofSeconds(15);
      benchQueueEnqueue(queueFactory, (int) (warmupDuration.toSeconds() * warmupRate), warmupRate);

      do {
        System.out.println("Starting!");
        CompletableFuture.delayedExecutor(5, TimeUnit.SECONDS)
            .execute(() -> System.out.println("Go!"));

        var n = 25_000_000;
        var delta = benchQueueEnqueue(queueFactory, n, 1_000_000);
        var deltaSecs = 1e-9 * delta.toNanos();
        System.out.printf("%,d\n", (int) (n / deltaSecs));

        var osw = new OutputStreamWriter(System.out);
        TextFormat.writeFormat(
            TextFormat.CONTENT_TYPE_OPENMETRICS_100,
            osw,
            CollectorRegistry.defaultRegistry.metricFamilySamples());
        osw.flush();
      } while (continueRunning());
    } finally {
      idGenerator.stopAsync().awaitTerminated();
    }
  }

  public void dataEnqueue() throws Exception {
    var root = Paths.get("/tmp/qrono-benchmark");
    var ioScheduler = new ExecutorTaskScheduler(
        Executors.newFixedThreadPool(4, new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Qrono-IOWorker-%d")
            .build()));

    var cpuScheduler = new ExecutorTaskScheduler(ForkJoinPool.commonPool());

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
        cpuScheduler,
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

  public void benchSort() {
    var value = Unpooled.copiedBuffer(Strings.repeat("a", 16), UTF_8);
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

  public static void main(String[] args) throws Exception {
    var bench = new QronoBenchmark();
    switch (args[0]) {
      case "queueEnqueue" -> bench.queueEnqueue();
      case "dataEnqueue" -> bench.dataEnqueue();
      case "benchSort" -> bench.benchSort();
    }
  }
}
