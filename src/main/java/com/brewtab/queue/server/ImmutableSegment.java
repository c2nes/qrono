package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Footer;
import com.brewtab.queue.Api.Segment.Metadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class ImmutableSegment implements Segment {
  @VisibleForTesting
  static final int FOOTER_SIZE = 128; // current actual max size 60 bytes

  private final InputStream input;
  private final Footer footer;
  private final Entry.Key firstKey;

  private Entry.Key nextKey;
  private boolean closed = false;

  private ImmutableSegment(InputStream input, Footer footer, Key firstKey, Key nextKey) {
    this.input = input;
    this.footer = footer;
    this.firstKey = firstKey;
    this.nextKey = nextKey;
  }

  @Override
  public Metadata getMetadata() {
    return SegmentMetadata.fromFooterAndFirstKey(footer, firstKey);
  }

  public long size() {
    return footer.getPendingCount() + footer.getTombstoneCount();
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

  public long getMaxId() {
    return footer.getMaxId();
  }

  @Override
  public void close() throws IOException {
    closed = true;
    nextKey = null;
    input.close();
  }

  public static ImmutableSegment newReader(InputStream input) throws IOException {
    Footer footer = Footer.parseDelimitedFrom(input);
    Key firstKey = Key.parseDelimitedFrom(input);
    return new ImmutableSegment(input, footer, firstKey, firstKey);
  }

  public static ImmutableSegment newReader(SeekableByteChannel input, long offset)
      throws IOException {
    var footer = readFooter(input);
    var inputStream = Channels.newInputStream(input);
    var firstKey = Key.parseDelimitedFrom(inputStream);
    var nextKey = firstKey;
    if (offset > 0) {
      input.position(offset);
      nextKey = Key.parseDelimitedFrom(inputStream);
      if (nextKey == null) {
        inputStream.close();
      }
    }
    return new ImmutableSegment(inputStream, footer, firstKey, nextKey);
  }

  private static Footer readFooter(SeekableByteChannel input) throws IOException {
    long original = input.position();
    try {
      input.position(input.size() - FOOTER_SIZE);
      var bytes = new byte[FOOTER_SIZE];
      var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      input.read(buffer);
      int footerMessageSize = buffer.getInt(0);
      return Footer.parser().parseFrom(bytes, 4, footerMessageSize);
    } finally {
      input.position(original);
    }
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

    var buffer = new ByteArrayOutputStream(bufferSize);

    var offsets = trackOffsets ? new HashMap<Key, Long>() : null;
    var offsetBase = 0L;

    // Footer fields
    Key lastKey = null;
    long maxId = Long.MIN_VALUE;
    long pendingCount = 0;
    long tombstoneCount = 0;

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

      lastKey = key;
      maxId = Math.max(maxId, key.getId());
      if (entry.hasPending()) {
        pendingCount++;
      }
      if (entry.hasTombstone()) {
        tombstoneCount++;
      }
    }

    // Write footer
    var footerMessage = Footer.newBuilder()
        .setLastKey(lastKey)
        .setMaxId(maxId)
        .setPendingCount(pendingCount)
        .setTombstoneCount(tombstoneCount)
        .build();

    // Create buffer for fixed sized footer
    var footerBytes = new byte[FOOTER_SIZE];

    // Wrap buffer for writing and skip the first 4 bytes
    // to leave room for a length prefix.
    var footerBB = ByteBuffer.wrap(footerBytes)
        .order(ByteOrder.LITTLE_ENDIAN)
        .position(4);

    // Write message to buffer and then add length prefix
    var codedOutput = CodedOutputStream.newInstance(footerBB);
    footerMessage.writeTo(codedOutput);
    codedOutput.flush();
    footerBB.putInt(0, footerBB.position() - 4);

    // Write footer to output buffer
    buffer.write(footerBytes);

    // Flush buffer to output
    buffer.writeTo(output);

    return offsets;
  }
}
