package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Header;
import com.google.common.base.Preconditions;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class ImmutableSegment implements Segment, Closeable {
  private final InputStream input;
  private final Header header;
  private final Entry.Key firstKey;

  private Entry.Key nextKey;
  private boolean closed = false;

  private ImmutableSegment(InputStream input, Header header, Key firstKey, Key nextKey) {
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
  public long getMaxId() {
    return header.getMaxId();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    nextKey = null;
    input.close();
  }

  public static ImmutableSegment newReader(InputStream input) throws IOException {
    Header header = Header.parseDelimitedFrom(input);
    Key firstKey = Key.parseDelimitedFrom(input);
    return new ImmutableSegment(input, header, firstKey, firstKey);
  }

  public static ImmutableSegment newReader(SeekableByteChannel input, long offset)
      throws IOException {
    var inputStream = Channels.newInputStream(input);
    var header = Header.parseDelimitedFrom(inputStream);
    var firstKey = Key.parseDelimitedFrom(inputStream);
    var nextKey = firstKey;
    if (offset > 0) {
      input.position(offset);
      nextKey = Key.parseDelimitedFrom(inputStream);
      if (nextKey == null) {
        inputStream.close();
      }
    }
    return new ImmutableSegment(inputStream, header, firstKey, nextKey);
  }

  public static ImmutableSegment open(Path path, long offset) throws IOException {
    return newReader(FileChannel.open(path), offset);
  }

  public static ImmutableSegment open(Path path) throws IOException {
    return open(path, 0);
  }

  public static void write(OutputStream output, Segment segment) throws IOException {
    write(output, segment, false);
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
      var offsets = write(output, segment, true);
      output.getFD().sync();
      return offsets;
    }
  }

  private static Map<Key, Long> write(OutputStream output, Segment segment, boolean trackOffsets)
      throws IOException {
    var bufferSize = 4 * 1024;

    ByteArrayOutputStream buffer = new ByteArrayOutputStream(bufferSize);
    Header.newBuilder()
        .setEntryCount(segment.size())
        .setLastKey(segment.last())
        .setMaxId(segment.getMaxId())
        .build()
        .writeDelimitedTo(buffer);

    var offsets = trackOffsets ? new HashMap<Key, Long>() : null;
    var offsetBase = 0L;

    for (Entry entry = segment.next(); entry != null; entry = segment.next()) {
      var key = entryKey(entry);
      if (trackOffsets) {
        offsets.put(key, offsetBase + buffer.size());
      }
      key.writeDelimitedTo(buffer);
      entry.writeDelimitedTo(buffer);
      if (buffer.size() > (bufferSize >> 1)) {
        buffer.writeTo(output);
        offsetBase += buffer.size();
        buffer.reset();
      }
    }

    buffer.writeTo(output);

    return offsets;
  }
}
