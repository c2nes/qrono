package com.brewtab.queue.server;

import com.brewtab.queue.Api.EnqueueRequest;
import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Pending;
import com.brewtab.queue.Api.Stats;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.SynchronousQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Queue {
  private static final Logger logger = LoggerFactory.getLogger(Queue.class);
  private final String name;
  private final IdGenerator idGenerator;
  private final Path directory;
  private final Clock clock;

  private WriteAheadLog log;
  private InMemorySegment currentSegment = new InMemorySegment();

  // TODO: Redo this properly
  private final SynchronousQueue<Segment> segmentWriterTransfer = new SynchronousQueue<>();
  private final Thread segmentWriterThread = new Thread(() -> {
    int idx = 0;
    try {
      while (true) {
        Segment segment = segmentWriterTransfer.take();
        Path segmentPath = getDirectory().resolve(String.format("segment-%03d", idx));
        Instant start = Instant.now();
        try (OutputStream out = new FileOutputStream(segmentPath.toFile())) {
          ImmutableSegment.write(out, segment);
        }
        idx++;
        System.out.println("SegmentWrite:" + Duration.between(start, Instant.now()));
      }
    } catch (InterruptedException e) {
      return;
    } catch (Exception e) {
      // TODO: Implement error recovery
      logger.error("Failed to write segment!", e);
    }
  });

  public Queue(String name, IdGenerator idGenerator, Path directory) throws IOException {
    this(name, idGenerator, directory, Clock.systemUTC());
  }

  public Queue(String name, IdGenerator idGenerator, Path directory, Clock clock)
      throws IOException {
    this.name = name;
    this.idGenerator = idGenerator;
    this.directory = directory;
    this.clock = clock;

    Files.createDirectories(directory);
    log = new WriteAheadLog(directory.resolve("op.log"));
    segmentWriterThread.start();
  }

  private Path getDirectory() {
    return directory;
  }

  public synchronized Item enqueue(EnqueueRequest request) throws IOException {
    Timestamp enqueueTime = Timestamps.fromMillis(clock.millis());
    Timestamp deadline = request.hasDeadline() ? request.getDeadline() : enqueueTime;
    long id = idGenerator.generateId();

    Key key = Key.newBuilder()
        .setDeadline(deadline)
        .setId(id)
        .build();

    Item item = Item.newBuilder()
        .setId(id)
        .setDeadline(deadline)
        .setValue(request.getValue())
        .setStats(Stats.newBuilder().setEnqueueTime(enqueueTime))
        .build();

    Entry entry = Entry.newBuilder()
        .setKey(key)
        .setPending(Pending.newBuilder().setItem(item))
        .build();

    // TODO: Do we even need a separate LogEntry message?

    log.write(entry);
    currentSegment.addEntry(entry);

    // TODO: Fix this...
    if (currentSegment.size() > 128 * 1024) {
      var start = Instant.now();
      Uninterruptibles.putUninterruptibly(segmentWriterTransfer, currentSegment.freeze());
      currentSegment = new InMemorySegment();
      System.out.println("xfrSegmentWriter: " + Duration.between(start, Instant.now()));
    }

    return item;
  }
}
