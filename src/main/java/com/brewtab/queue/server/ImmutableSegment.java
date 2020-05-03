package com.brewtab.queue.server;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.protobuf.ByteString;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.function.Supplier;

public class ImmutableSegment implements Segment {
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final BufferedReadableChannel channel;
  private final SegmentMetadata metadata;
  private final long limit;
  private Entry.Key nextKey;

  private boolean closed = false;

  private ImmutableSegment(
      BufferedReadableChannel channel,
      SegmentMetadata metadata,
      long limit,
      Entry.Key nextKey
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
  public SegmentMetadata getMetadata() {
    return metadata;
  }

  @Override
  public Entry.Key peek() {
    checkOpen();
    return nextKey;
  }

  private Entry readNextEntry() throws IOException {
    if (nextKey == null) {
      return null;
    }

    if (nextKey.entryType() == Entry.Type.PENDING) {
      channel.ensureReadableBytes(Encoding.STATS_SIZE + 4);
      var stats = Encoding.readStats(channel.buffer);
      var valueLength = channel.buffer.getInt();
      channel.ensureReadableBytes(valueLength);
      var value = ByteString.copyFrom(channel.buffer, valueLength);

      var item = ImmutableItem.builder()
          .deadline(nextKey.deadline())
          .id(nextKey.id())
          .stats(stats)
          .value(value)
          .build();

      return ImmutableEntry.builder()
          .key(nextKey)
          .item(item)
          .build();
    }

    if (nextKey.entryType() == Entry.Type.TOMBSTONE) {
      return ImmutableEntry.builder()
          .key(nextKey)
          .build();
    }

    throw new IllegalStateException("unrecognized entry type");
  }

  private Entry.Key readNextKey() throws IOException {
    if (position() < limit) {
      channel.ensureReadableBytes(Encoding.KEY_SIZE);
      return Encoding.readKey(channel.buffer);
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
    var footerPosition = channel.channel.size() - Encoding.FOOTER_SIZE;
    channel.position(footerPosition);
    channel.ensureReadableBytes(Encoding.FOOTER_SIZE);
    var footer = Encoding.readFooter(channel.buffer);

    // Put position back to beginning of channel
    channel.position(0);

    // Read first key
    channel.ensureReadableBytes(Encoding.KEY_SIZE);
    var firstKey = Encoding.readKey(channel.buffer);

    // Build metadata from first key and data in footer
    var metadata = ImmutableSegmentMetadata.builder()
        .pendingCount(footer.pendingCount())
        .tombstoneCount(footer.tombstoneCount())
        .firstKey(firstKey)
        .lastKey(footer.lastKey())
        .maxId(footer.maxId())
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

    // Gets the current read offset. We can use this to more quickly seek to the required
    // offset in the newly written segment.
    private final Supplier<Entry.Key> liveReaderOffset;

    private ByteBuffer buffer = newByteBuffer(DEFAULT_BUFFER_SIZE);
    private long position = 0;

    private Writer(
        SeekableByteChannel channel,
        Segment source,
        Supplier<Key> liveReaderOffset) {
      this.channel = channel;
      this.source = source;
      this.liveReaderOffset = liveReaderOffset;
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

    private void writeKey(Entry.Key key) throws IOException {
      ensureBufferCapacity(Encoding.KEY_SIZE);
      position += Encoding.writeKey(buffer, key);
    }

    private void writePendingItem(Item item) throws IOException {
      var value = item.value();
      ensureBufferCapacity(Encoding.STATS_SIZE + 4 + value.size());

      // stats (Encoding.STATS_SIZE bytes)
      position += Encoding.writeStats(buffer, item.stats());

      // value_length (4 bytes)
      buffer.putInt(value.size());
      position += 4;

      // value (value_length bytes)
      value.copyTo(buffer);
      position += value.size();
    }

    private ReaderOffset write() throws IOException {
      // Get first entry
      var entry = source.next();

      if (entry == null) {
        throw new IllegalArgumentException("source segment must be non-empty");
      }

      // Monitor where the reader is and maintain a corresponding start position
      // in the output file. These are not updated synchronously so the live reader
      // may drift ahead of this start position. That's okay. We maintain the best
      // start position we can and will scan from there in the Opener to get to the
      // requested position.
      var readerOffset = liveReaderOffset.get();
      var readerStartPosition = 0L;
      var readerStartKey = entry.key();

      // Footer fields
      var pendingCount = 0;
      var tombstoneCount = 0;
      Entry.Key lastKey = null;
      var maxId = Long.MIN_VALUE;

      while (entry != null) {
        // Update our copy of the reader's position, but only if we're ahead of where
        // we last knew the reader to be. If we're behind where we last knew the
        // reader to be we should just continue advancing the start position.
        var readerIsAhead = entry.key().compareTo(readerOffset) <= 0;
        if (!readerIsAhead) {
          readerOffset = liveReaderOffset.get();
          readerIsAhead = entry.key().compareTo(readerOffset) <= 0;
        }

        // So long as the reader is ahead of us keep advancing the start position
        if (readerIsAhead) {
          readerStartPosition = position;
          readerStartKey = entry.key();
        }

        // Write key and pending item data
        var key = entry.key();
        writeKey(entry.key());

        var item = entry.item();
        if (item != null) {
          writePendingItem(item);
          pendingCount++;
        } else {
          tombstoneCount++;
        }

        if (key.id() > maxId) {
          maxId = key.id();
        }

        lastKey = key;
        entry = source.next();
      }

      // TODO: Consider versioning the footer

      // Write footer
      ensureBufferCapacity(Encoding.FOOTER_SIZE);
      Encoding.writeFooter(buffer, ImmutableEncoding.Footer.builder()
          .pendingCount(pendingCount)
          .tombstoneCount(tombstoneCount)
          .lastKey(lastKey)
          .maxId(maxId)
          .build());
      flushBuffer();

      return new ReaderOffset(readerStartKey, readerStartPosition);
    }
  }

  public static ReaderOffset write(
      Path path,
      Segment segment,
      Supplier<Key> liveReaderOffset
  ) throws IOException {
    try (var output = FileChannel.open(path, WRITE, CREATE)) { // TODO:
      var readerOffset = write(output, segment, liveReaderOffset);
      output.force(false);
      return readerOffset;
    }
  }

  public static ReaderOffset write(
      SeekableByteChannel channel,
      Segment source,
      Supplier<Key> liveReaderOffset
  ) throws IOException {
    return new Writer(channel, source, liveReaderOffset).write();
  }

  private static ByteBuffer newByteBuffer(int capacity) {
    return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
  }

  public static class ReaderOffset {
    private final Entry.Key key;
    private final long position;

    private ReaderOffset(Key key, long position) {
      this.key = key;
      this.position = position;
    }

    public Key getKey() {
      return key;
    }

    public long getPosition() {
      return position;
    }
  }
}
