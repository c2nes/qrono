package com.brewtab.queue.server;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import com.brewtab.queue.Api.Segment.Entry;
import com.google.common.collect.ImmutableList;
import java.io.FileInputStream;
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
    entry.writeDelimitedTo(out);
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

    try (FileInputStream in = new FileInputStream(path.toFile())) {
      var entry = Entry.parseDelimitedFrom(in);
      while (entry != null) {
        entries.add(entry);
        entry = Entry.parseDelimitedFrom(in);
      }
    }

    return entries.build();
  }

  public static void delete(Path directory, String segmentName) throws IOException {
    Files.delete(SegmentFiles.getClosedLogPath(directory, segmentName));
  }
}
