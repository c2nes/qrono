package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Header;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class ImmutableSegment {
  private ImmutableSegment() {
  }

  public static class Reader implements Segment, Closeable {
    private final InputStream input;
    private final Header header;
    private final Entry.Key firstKey;

    private Entry.Key nextKey;
    private boolean closed = false;

    private Reader(InputStream input, Header header, Key firstKey, Key nextKey) {
      this.input = input;
      this.header = header;
      this.firstKey = firstKey;
      this.nextKey = nextKey;
    }

    @Override
    public long size() {
      return header.getEntryCount();
    }

    @Override
    public Key peek() {
      Preconditions.checkState(!closed, "closed");
      return nextKey;
    }

    @Override
    public Entry next() throws IOException {
      Preconditions.checkState(!closed, "closed");
      if (nextKey == null) {
        return null;
      }

      Entry entry = Entry.parseDelimitedFrom(input);
      nextKey = Entry.Key.parseDelimitedFrom(input);

      if (nextKey == null) {
        input.close();
      }

      return entry;
    }

    @Override
    public Key first() {
      return firstKey;
    }

    @Override
    public Key last() {
      return header.getLastKey();
    }

    @Override
    public void close() throws IOException {
      closed = true;
      nextKey = null;
      input.close();
    }
  }

  public static Reader newReader(InputStream input) throws IOException {
    Header header = Header.parseDelimitedFrom(input);
    Key firstKey = Key.parseDelimitedFrom(input);
    return new Reader(input, header, firstKey, firstKey);
  }

  public static Reader newReader(SeekableByteChannel input, long offset) throws IOException {
    var inputStream = Channels.newInputStream(input);
    var header = Header.parseDelimitedFrom(inputStream);
    var firstKey = Key.parseDelimitedFrom(inputStream);
    var nextKey = firstKey;
    if (offset > 0) {
      input.position(offset);
      nextKey = Key.parseDelimitedFrom(inputStream);
    }
    return new Reader(inputStream, header, firstKey, nextKey);
  }

  public static void write(OutputStream output, Segment segment) throws IOException {
    Header.newBuilder()
        .setEntryCount(segment.size())
        .setLastKey(segment.last())
        .build()
        .writeDelimitedTo(output);

    for (Entry entry = segment.next(); entry != null; entry = segment.next()) {
      entryKey(entry).writeDelimitedTo(output);
      entry.toBuilder().build().writeDelimitedTo(output);
    }
  }

  public static void write(Path path, Segment segment) throws IOException {
    try (var output = new FileOutputStream(path.toFile())) {
      write(output, segment);
      output.getFD().sync();
    }
  }

  public static Map<Key, Long> writeWithOffsetTracking(Path path, Segment segment)
      throws IOException {
    try (var output = new FileOutputStream(path.toFile())) {
      Header.newBuilder()
          .setEntryCount(segment.size())
          .setLastKey(segment.last())
          .build()
          .writeDelimitedTo(output);

      var offsets = new HashMap<Key, Long>();
      for (Entry entry = segment.next(); entry != null; entry = segment.next()) {
        var key = entryKey(entry);
        offsets.put(key, output.getChannel().position());
        key.writeDelimitedTo(output);
        entry.writeDelimitedTo(output);
      }

      output.getFD().sync();
      return offsets;
    }
  }

}
