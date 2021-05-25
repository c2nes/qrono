package net.qrono.server;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import io.netty.buffer.ByteBufAllocator;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Supplier;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Entry.Key;
import net.qrono.server.data.ImmutableEntry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableSegmentMetadata;
import net.qrono.server.data.Item;
import net.qrono.server.data.SegmentMetadata;
import net.qrono.server.util.ByteBufs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImmutableSegment implements Segment {
  private static final Logger log = LoggerFactory.getLogger(ImmutableSegment.class);

  @VisibleForTesting
  static final int DEFAULT_BUFFER_SIZE = 4096;

  // TODO: Abstract the file opening for easier testing (e.g. Supplier<SeekableByteChannel>)
  private final Path path;
  private final SegmentMetadata metadata;
  private final KnownOffset knownOffset;

  private ImmutableSegment(Path path, SegmentMetadata metadata) {
    this(path, metadata, KnownOffset.ZERO);
  }

  private ImmutableSegment(Path path, SegmentMetadata metadata, KnownOffset knownOffset) {
    this.path = path;
    this.metadata = metadata;
    this.knownOffset = knownOffset;
  }

  @Override
  public SegmentName name() {
    return SegmentName.fromPath(path);
  }

  @Override
  public SegmentMetadata metadata() {
    return metadata;
  }

  @Override
  public SegmentReader newReader(Key position) throws IOException {
    var reader = new Reader(FileChannel.open(path));
    if (position.compareTo(knownOffset.key()) < 0) {
      // "knownOffset" was tracked by the Writer based on a hint about where the head of the
      // queue is currently. The head of the queue may have since moved ahead of this known
      // position, but if the requested position is _behind_ our known position then something
      // has gone wrong and seeking to the appropriate position may be slow.
      //
      // The exception to this is when the requested position is Key.ZERO which is the standard
      // way of opening the reader to the beginning of the segment (as is done during rewriting
      // compactions).
      if (!position.equals(Key.ZERO)) {
        log.warn("Segment reader opened at seemingly stale offset. Performance may suffer."
                + " This is likely a bug; path={}, position={}, knownOffset={}",
            path, position, knownOffset);
      }

      reader.position(0);
    } else {
      reader.position(knownOffset.position());
    }

    // Seek to the requested position
    var peek = reader.peek();
    while (peek != null && peek.compareTo(position) <= 0) {
      reader.next().release();
      peek = reader.peek();
    }

    return reader;
  }

  public static ImmutableSegment open(Path path) throws IOException {
    try (var source = FileChannel.open(path)) {
      Reader reader = new Reader(source);
      var metadata = buildMetadata(reader.readFooter());
      return new ImmutableSegment(path, metadata);
    }
  }

  public static ImmutableSegment write(
      Path path,
      SegmentReader source,
      Supplier<Key> liveReaderOffset
  ) throws IOException {
    var tmpPath = SegmentFiles.getTemporaryPath(path);
    try (var output = FileChannel.open(tmpPath, WRITE, CREATE)) {
      var writer = new Writer(output, source, liveReaderOffset);

      // Do the actual writing
      var readerOffset = writer.write();
      output.force(false);
      Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);

      return new ImmutableSegment(path, writer.metadata, readerOffset);
    }
  }

  private static ByteBuffer newByteBuffer(int capacity) {
    return ByteBuffer.allocate(capacity).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static SegmentMetadata buildMetadata(Encoding.Footer footer) {
    return ImmutableSegmentMetadata.builder()
        .pendingCount(footer.pendingCount())
        .tombstoneCount(footer.tombstoneCount())
        .maxId(footer.maxId())
        .build();
  }

  @VisibleForTesting
  static class Reader implements SegmentReader {
    private final ByteBufAllocator bufAllocator = ByteBufAllocator.DEFAULT;

    private final BufferedReadableChannel channel;
    private final long footerPosition;
    private Entry.Key nextKey = null;
    // Only populated when peekEntry is called
    private Entry nextEntry = null;
    private boolean closed = false;

    @VisibleForTesting
    Reader(SeekableByteChannel channel) throws IOException {
      this(new BufferedReadableChannel(channel));
    }

    private Reader(BufferedReadableChannel channel) throws IOException {
      this.channel = channel;
      footerPosition = channel.channel.size() - Encoding.FOOTER_SIZE;
    }

    private void checkOpen() {
      Preconditions.checkState(!closed, "closed");
    }

    @Override
    public Entry.Key peek() {
      checkOpen();
      if (nextEntry != null) {
        return nextEntry.key();
      }
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

        var value = bufAllocator.buffer(valueLength);
        ByteBufs.writeBytes(value, channel.buffer, valueLength);

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

    Entry.Key readNextKey() throws IOException {
      if (position() < footerPosition) {
        channel.ensureReadableBytes(Encoding.KEY_SIZE);
        return Encoding.readKey(channel.buffer);
      }
      return null;
    }

    @Override
    public Entry next() throws IOException {
      checkOpen();

      // Return entry buffered by peekNext if available.
      // peekNext also populates nextKey with the subsequent key so we
      // do not need to call readNextKey here.
      if (nextEntry != null) {
        var entry = nextEntry;
        nextEntry = null;
        return entry;
      }

      var entry = readNextEntry();
      nextKey = readNextKey();
      return entry;
    }

    @Override
    public Entry peekEntry() throws IOException {
      checkOpen();
      // Ensure the next entry is buffered
      if (nextEntry == null) {
        nextEntry = readNextEntry();
        nextKey = readNextKey();
      }
      return nextEntry;
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

    public Encoding.Footer readFooter() throws IOException {
      channel.position(footerPosition);
      channel.ensureReadableBytes(Encoding.FOOTER_SIZE);
      return Encoding.readFooter(channel.buffer);
    }
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
          buffer.put(oldBuffer);
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

  @VisibleForTesting
  static class Writer {
    private final SeekableByteChannel channel;
    private final SegmentReader source;

    // Gets the current read offset. We can use this to more quickly seek to the required
    // offset in the newly written segment.
    private final Supplier<Entry.Key> liveReaderOffset;

    private ByteBuffer buffer = newByteBuffer(DEFAULT_BUFFER_SIZE);
    private long position = 0;

    // Populated by write()
    private SegmentMetadata metadata;

    Writer(SeekableByteChannel channel, SegmentReader source, Supplier<Key> liveReaderOffset) {
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
      Verify.verify(buffer.position() == 0, "short write (unsupported channel type?)");
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
      var valueLen = value.readableBytes();
      ensureBufferCapacity(Encoding.STATS_SIZE + 4 + valueLen);

      // stats (Encoding.STATS_SIZE bytes)
      position += Encoding.writeStats(buffer, item.stats());

      // value_length (4 bytes)
      buffer.putInt(valueLen);
      position += 4;

      // value (value_length bytes)
      ByteBufs.getBytes(value, buffer, valueLen);
      position += valueLen;
    }

    KnownOffset write() throws IOException {
      // Monitor where the reader is and maintain a corresponding start position
      // in the output file. These are not updated synchronously so the live reader
      // may drift ahead of this start position. That's okay. We maintain the best
      // start position we can and will scan from there in the Opener to get to the
      // requested position.
      var readerOffset = liveReaderOffset.get();
      var readerStartPosition = 0L;
      var readerStartKey = Key.ZERO;

      // Footer fields
      var pendingCount = 0;
      var tombstoneCount = 0;
      var maxId = 0L;

      for (var entry = source.next(); entry != null; entry = source.next()) {
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
      }

      var footer = ImmutableEncoding.Footer.builder()
          .pendingCount(pendingCount)
          .tombstoneCount(tombstoneCount)
          .maxId(maxId)
          .build();

      // Save metadata for use by caller
      metadata = buildMetadata(footer);

      // Write footer
      ensureBufferCapacity(Encoding.FOOTER_SIZE);
      Encoding.writeFooter(buffer, footer);
      flushBuffer();

      return new KnownOffset(readerStartKey, readerStartPosition);
    }
  }

  public static class KnownOffset {
    public static final KnownOffset ZERO = new KnownOffset(Key.ZERO, 0);

    private final Entry.Key key;
    private final long position;

    private KnownOffset(Key key, long position) {
      this.key = key;
      this.position = position;
    }

    public Key key() {
      return key;
    }

    public long position() {
      return position;
    }

    @Override
    public String toString() {
      return "KnownOffset{" +
          "key=" + key +
          ", position=" + position +
          '}';
    }
  }
}
