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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class ImmutableSegment implements Segment {
  @VisibleForTesting
  static final int FOOTER_SIZE = 128; // current actual max size 60 bytes

  private final SeekableByteChannel channel;
  private final Footer footer;
  private final Entry.Key firstKey;
  private final long limit;

  private Entry.Key nextKey;
  private boolean closed = false;

  private final ByteBuffer buffer;

  private ImmutableSegment(SeekableByteChannel channel,
      Footer footer, Key firstKey, Key nextKey, long limit) {
    this.channel = channel;
    this.footer = footer;
    this.firstKey = firstKey;
    this.nextKey = nextKey;
    this.limit = limit;
    // TODO: Maybe use a growable buffer and/or main the buffer with a weak reference?
    buffer = ByteBuffer.allocate(4096).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public Metadata getMetadata() {
    return SegmentMetadata.fromFooterAndFirstKey(footer, firstKey);
  }

  public long size() {
    return footer.getPendingCount() + footer.getTombstoneCount();
  }

  private static int readSize(SeekableByteChannel channel, ByteBuffer buffer) throws IOException {
    buffer.position(0).limit(4);
    channel.read(buffer);
    return buffer.getInt(0);
  }

  private static Key readNextKey(SeekableByteChannel channel, long limit, ByteBuffer buffer)
      throws IOException {
    if (channel.position() < limit) {
      int size = readSize(channel, buffer);
      buffer.position(0).limit(size);
      channel.read(buffer);
      buffer.flip();

      return Key.parser().parseFrom(buffer);
    }

    return null;
  }

  private ByteBuffer getPreparedBuffer(int size) {
    if (size <= buffer.capacity()) {
      return buffer.position(0).limit(size);
    } else {
      return ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
    }
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

    var size = readSize(channel, buffer);
    var buf = getPreparedBuffer(size);
    channel.read(buf);
    buf.flip();
    var entry = Entry.parseFrom(buf);
    nextKey = readNextKey(channel, limit, buffer);

    if (nextKey == null) {
      channel.close();
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
    channel.close();
  }

  public static ImmutableSegment newReader(SeekableByteChannel input, long offset)
      throws IOException {
    var footer = readFooter(input);
    var limit = input.size() - FOOTER_SIZE;
    var buffer = ByteBuffer.allocate(1024).order(ByteOrder.LITTLE_ENDIAN);
    var firstKey = readNextKey(input, limit, buffer);
    var nextKey = firstKey;
    if (offset > 0) {
      input.position(offset);
      nextKey = readNextKey(input, limit, buffer);
    }
    if (nextKey == null) {
      input.close();
    }
    return new ImmutableSegment(input, footer, firstKey, nextKey, limit);
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
    Key lastKey = Key.getDefaultInstance();
    long maxId = Long.MIN_VALUE;
    long pendingCount = 0;
    long tombstoneCount = 0;

    var sizePrefix = ByteBuffer.wrap(new byte[4]).order(ByteOrder.LITTLE_ENDIAN);

    for (Entry entry = segment.next(); entry != null; entry = segment.next()) {
      var key = entryKey(entry);
      if (trackOffsets) {
        offsets.put(key, offsetBase + buffer.size());
      }

      // Write key
      sizePrefix.putInt(0, key.getSerializedSize());
      buffer.write(sizePrefix.array());
      key.writeTo(buffer);

      // Write entry
      sizePrefix.putInt(0, entry.getSerializedSize());
      buffer.write(sizePrefix.array());
      entry.writeTo(buffer);

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
