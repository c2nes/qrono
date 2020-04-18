package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.TreeMap;

public class WriteAheadLog implements Closeable {
  private final Path path;
  private final Duration fsyncInterval;
  private FileOutputStream out;
  private Instant fsyncDeadline;

  public WriteAheadLog(Path path) throws IOException {
    this(path, Duration.ofSeconds(1));
  }

  public WriteAheadLog(Path path, Duration fsyncInterval) throws IOException {
    this.path = path;
    this.fsyncInterval = fsyncInterval;
    out = new FileOutputStream(path.toFile());
    fsyncDeadline = Instant.now().plus(fsyncInterval);
  }

  public Path getPath() {
    return path;
  }

  public void write(Entry entry) throws IOException {
    entry.writeDelimitedTo(out);
    Instant now = Instant.now();
    if (now.isAfter(fsyncDeadline)) {
      out.flush();
      out.getFD().sync();
      fsyncDeadline = now.plus(fsyncInterval);
      // System.out.println("LogSync: " + Duration.between(now, Instant.now()));
    }
  }

  public static Segment load(Path path) throws IOException {
    // TODO: Handle corruption or at least truncated writes
    var entries = new TreeMap<Key, Entry>(entryKeyComparator());

    try (FileInputStream in = new FileInputStream(path.toFile())) {
      var entry = Entry.parseDelimitedFrom(in);
      while (entry != null) {
        var key = entryKey(entry);
        // Only add tombstones if they don't cancel out a previous entry
        if (entry.getEntryCase() != EntryCase.TOMBSTONE || entries.remove(key) == null) {
          entries.put(key, entry);
        }
        entry = Entry.parseDelimitedFrom(in);
      }
    }

    // TODO: Build segment from tree?
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    out.close();
  }
}
