package com.brewtab.queue.server;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Metadata;
import com.brewtab.queue.server.Encoding.Footer;
import com.brewtab.queue.server.Encoding.Key.Type;
import com.brewtab.queue.server.Encoding.PendingPreamble;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Map;

public class ImmutableSegment implements Segment {
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final BufferedReadableChannel channel;
  private final Metadata metadata;
  private final long limit;
  private Encoding.Key nextKey;

  private boolean closed = false;

  private ImmutableSegment(
      BufferedReadableChannel channel,
      Metadata metadata,
      long limit,
      Encoding.Key nextKey
  ) {
    this.channel = channel;
    this.metadata = metadata;
    this.limit = limit;
    this.nextKey = nextKey;
  }

  private void checkOpen() {
    Preconditions.checkState(!closed, "closed");
  }

  @Override
  public Metadata getMetadata() {
    return metadata;
  }

  @Override
  public Key peek() {
    checkOpen();
    var key = nextKey;
    return key == null ? null : key.toEntryKey();
  }

  private Entry readNextEntry() throws IOException {
    if (nextKey == null) {
      return null;
    }

    if (nextKey.type == Type.PENDING) {
      channel.ensureReadableBytes(PendingPreamble.SIZE);
      var preamble = PendingPreamble.read(channel.buffer);
      channel.ensureReadableBytes(preamble.valueLength);
      var value = ByteString.copyFrom(channel.buffer, preamble.valueLength);

      return Entry.newBuilder()
          .setPending(Item.newBuilder()
              .setDeadline(Encoding.fromTimestamp(nextKey.deadline))
              .setId(nextKey.id)
              .setStats(preamble.stats.toApiStats())
              .setValue(value))
          .build();
    }

    if (nextKey.type == Type.TOMBSTONE) {
      return Entry.newBuilder()
          .setTombstone(nextKey.toEntryKey())
          .build();
    }

    throw new IllegalStateException("unrecognized entry type");
  }

  private Encoding.Key readNextKey() throws IOException {
    if (position() < limit) {
      channel.ensureReadableBytes(Encoding.Key.SIZE);
      return Encoding.Key.read(channel.buffer);
    }
    return null;
  }

  @Override
  public Entry next() throws IOException {
    checkOpen();
    var entry = readNextEntry();
    nextKey = readNextKey();
    return entry;
  }

  @Override
  public void close() throws IOException {
    closed = true;
    nextKey = null;
    channel.channel.close();
  }

  public long position() throws IOException {
    return channel.channel.position() - channel.buffer.remaining();
  }

  public void position(long newPosition) throws IOException {
    channel.position(newPosition);
    // Read next key at new offset
    nextKey = readNextKey();
  }

  public static ImmutableSegment open(Path path) throws IOException {
    return newReader(FileChannel.open(path));
  }

  public static ImmutableSegment newReader(SeekableByteChannel source) throws IOException {
    var channel = new BufferedReadableChannel(source);

    // Read footer
    var footerPosition = channel.channel.size() - Footer.SIZE;
    channel.position(footerPosition);
    channel.ensureReadableBytes(Footer.SIZE);
    var footer = Footer.read(channel.buffer);

    // Put position back to beginning of channel
    channel.position(0);

    // Read first key (if any)
    Encoding.Key firstKey = null;
    if (channel.channel.size() > Footer.SIZE) {
      channel.ensureReadableBytes(Encoding.Key.SIZE);
      firstKey = Encoding.Key.read(channel.buffer);
    } else {
      channel.channel.close();
    }

    // Build metadata from first key and data in footer
    var metadata = Metadata.newBuilder()
        .setPendingCount(footer.pendingCount)
        .setTombstoneCount(footer.tombstoneCount)
        .setFirstKey(firstKey == null ? Key.getDefaultInstance() : firstKey.toEntryKey())
        .setLastKey(footer.lastKey.toEntryKey())
        .setMaxId(footer.maxId)
        .build();

    return new ImmutableSegment(channel, metadata, footerPosition, firstKey);
  }

  static class BufferedReadableChannel {
    private final SeekableByteChannel channel;
    private ByteBuffer buffer = newByteBuffer(DEFAULT_BUFFER_SIZE);

    BufferedReadableChannel(SeekableByteChannel channel) {
      this.channel = channel;
    }

    void position(long newPosition) throws IOException {
      channel.position(newPosition);
      // Discard all data in buffer
      buffer.position(0).limit(0);
    }

    void ensureReadableBytes(int required) throws IOException {
      if (buffer.remaining() < required) {
        if (buffer.capacity() >= required) {
          buffer.compact();
        } else {
          var oldBuffer = buffer;
          buffer = newByteBuffer(required);
          buffer.put(oldBuffer.flip());
        }

        // Do the read
        channel.read(buffer);

        // Ensure we read as many bytes as were requested
        if (buffer.position() < required) {
          throw new EOFException();
        }

        // Prepare for reading
        buffer.flip();
      }
    }
  }

  static class Writer {
    private final SeekableByteChannel channel;
    private final Segment source;
    private final ImmutableMap.Builder<Key, Long> offsets;
    private ByteBuffer buffer = newByteBuffer(DEFAULT_BUFFER_SIZE);
    private long position = 0;

    private Writer(
        SeekableByteChannel channel,
        Segment source,
        ImmutableMap.Builder<Key, Long> offsets
    ) {
      this.channel = channel;
      this.source = source;
      this.offsets = offsets;
    }

    private void recordOffset(Entry entry) {
      if (offsets != null) {
        offsets.put(Segment.entryKey(entry), position);
      }
    }

    private void flushBuffer() throws IOException {
      // Flush whatever data is currently in the buffer
      channel.write(buffer.flip());

      // Compact the buffer and ensure position is reset to 0 (i.e. the write
      // flushed all of the data in the buffer). WritableByteChannel specifies
      // that this should be the case unless the concrete type specifies
      // otherwise (e.g. non-blocking socket channel).
      buffer.compact();
      Verify.verifyNotNull(buffer.position() == 0,
          "short write (unsupported channel type?)");
    }

    // Ensure there is capacity to write the requested number of bytes
    private void ensureBufferCapacity(int required) throws IOException {
      if (buffer.remaining() < required) {
        // Flush buffer if non-empty
        if (buffer.position() > 0) {
          flushBuffer();
        }

        // New, bigger buffer required
        if (buffer.capacity() < required) {
          buffer = newByteBuffer(required);
        }
      }
    }

    private void writeKey(Encoding.Key key) throws IOException {
      ensureBufferCapacity(Encoding.Key.SIZE);
      position += key.write(buffer);
    }

    private void writePendingItem(Item item) throws IOException {
      // Write preamble
      ensureBufferCapacity(Encoding.PendingPreamble.SIZE);
      position += PendingPreamble.fromPending(item).write(buffer);

      // Write value
      var value = item.getValue();
      ensureBufferCapacity(value.size());
      value.copyTo(buffer);
      position += value.size();
    }

    private void write() throws IOException {
      // Footer fields
      var pendingCount = 0;
      var tombstoneCount = 0;
      var lastKey = new Encoding.Key(0, 0, Type.PENDING);
      var maxId = Long.MIN_VALUE;

      for (Entry entry = source.next(); entry != null; entry = source.next()) {
        // Record offset to entry
        recordOffset(entry);

        // Write key and pending item data
        var key = Encoding.Key.fromEntry(entry);
        writeKey(key);

        if (entry.hasPending()) {
          writePendingItem(entry.getPending());
        }

        // Track metadata for footer
        if (entry.hasPending()) {
          pendingCount++;
        } else if (entry.hasTombstone()) {
          tombstoneCount++;
        }

        if (key.id > maxId) {
          maxId = key.id;
        }

        lastKey = key;
      }

      // TODO: Consider versioning the footer

      // Write footer
      ensureBufferCapacity(Footer.SIZE);
      new Footer(pendingCount, tombstoneCount, lastKey, maxId).write(buffer);
      flushBuffer();
    }
  }

  public static void write(Path path, Segment segment) throws IOException {
    try (var output = FileChannel.open(path, WRITE, CREATE)) { // TODO:
      write(output, segment);
      output.force(false);
    }
  }

  public static void write(SeekableByteChannel channel, Segment source) throws IOException {
    new Writer(channel, source, null).write();
  }

  public static Map<Key, Long> writeWithOffsetTracking(
      Path path,
      Segment source
  ) throws IOException {
    try (var output = FileChannel.open(path, WRITE, CREATE)) {
      var offsets = writeWithOffsetTracking(output, source);
      output.force(false);
      return offsets;
    }
  }

  public static Map<Key, Long> writeWithOffsetTracking(
      SeekableByteChannel channel,
      Segment source
  ) throws IOException {
    var writer = new Writer(channel, source, ImmutableMap.builder());
    writer.write();
    return writer.offsets.build();
  }

  private static ByteBuffer newByteBuffer(int capacity) {
    return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
  }
}
