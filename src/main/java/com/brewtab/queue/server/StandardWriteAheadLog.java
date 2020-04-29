package com.brewtab.queue.server;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.Item;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class StandardWriteAheadLog implements WriteAheadLog {
  public static final Duration DEFAULT_SYNC_INTERVAL = Duration.ofSeconds(1);

  private final Path directory;
  private final String segmentName;
  private final Duration syncInterval;
  private final FileOutputStream out;
  private Instant nextSyncDeadline;

  // TODO: Dependency inject Clock
  // TODO: Consider WriteAheadLogFactory to capture sync parameters and clock?

  // TODO: Some sort of "SyncableOutputStream" interface would make testing easier
  //  (e.g. we could pass byte array backed OutputStreams or Channels)
  private StandardWriteAheadLog(
      Path directory,
      String segmentName,
      Duration syncInterval,
      FileOutputStream out
  ) {
    this.directory = directory;
    this.segmentName = segmentName;
    this.syncInterval = syncInterval;
    this.out = out;
    nextSyncDeadline = Instant.now().plus(syncInterval);
  }

  @Override
  public void append(Entry entry) throws IOException {
    // TODO: Buffer writes and re-use buffer
    out.write(Encoding.toByteArray(entry));
    Instant now = Instant.now();
    if (now.isAfter(nextSyncDeadline)) {
      out.flush();
      out.getFD().sync();
      nextSyncDeadline = now.plus(syncInterval);
    }
  }

  @Override
  public void close() throws IOException {
    out.flush();
    out.getFD().sync();
    out.close();

    Files.move(
        SegmentFiles.getLogPath(directory, segmentName),
        SegmentFiles.getClosedLogPath(directory, segmentName),
        ATOMIC_MOVE);
  }

  public static WriteAheadLog create(Path directory, String segmentName) throws IOException {
    return create(directory, segmentName, DEFAULT_SYNC_INTERVAL);
  }

  public static WriteAheadLog create(Path directory, String segmentName, Duration syncInterval)
      throws IOException {
    var outputPath = SegmentFiles.getLogPath(directory, segmentName);
    var output = new FileOutputStream(outputPath.toFile());
    return new StandardWriteAheadLog(directory, segmentName, syncInterval, output);
  }

  public static List<Entry> read(Path path) throws IOException {
    // TODO: Handle corruption or at least truncated writes
    var entries = new ImmutableList.Builder<Entry>();

    // TODO: Don't buffer the entire file in memory
    // This log was for an in-memory segment so its reasonably safe
    // to read it fully into memory. However, if we're sufficiently
    // constrained on memory (e.g. the in-memory segment took more than
    // half of available memory itself) then this may pose an issue.
    var bytes = Files.readAllBytes(path);
    var buffer = Encoding.wrapByteBuffer(bytes);

    while (buffer.hasRemaining()) {
      var encodedKey = Encoding.readKey(buffer);
      var key = ImmutableEntry.Key.copyOf(encodedKey);
      Item item = null;
      if (encodedKey.entryType() == Entry.Type.PENDING) {
        var stats = Encoding.readStats(buffer);
        var valueLength = buffer.getInt();
        var value = ByteString.copyFrom(buffer, valueLength);
        item = ImmutableItem.builder()
            .deadline(key.deadline())
            .id(key.id())
            .stats(stats)
            .value(value)
            .build();
      }

      entries.add(ImmutableEntry.builder()
          .key(key)
          .item(item)
          .build());
    }

    return entries.build();
  }

  public static void delete(Path directory, String segmentName) throws IOException {
    Files.delete(SegmentFiles.getClosedLogPath(directory, segmentName));
  }
}
