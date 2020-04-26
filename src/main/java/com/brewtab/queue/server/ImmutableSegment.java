package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Footer;
import com.brewtab.queue.Api.Segment.Metadata;
import com.brewtab.queue.server.Encoding.Key.Type;
import com.brewtab.queue.server.Encoding.PendingPreamble;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.protobuf.ByteString;
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

  private Encoding.Key nextKey;
  private boolean closed = false;

  private final ByteBuffer buffer;

  private ImmutableSegment(SeekableByteChannel channel,
      Footer footer, Key firstKey, Encoding.Key nextKey, long limit) {
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

  private static Encoding.Key readNextKey(
      SeekableByteChannel channel,
      long limit,
      ByteBuffer buffer
  ) throws IOException {
    if (channel.position() < limit) {
      buffer.position(0).limit(Encoding.Key.SIZE);
      channel.read(buffer);
      buffer.flip();
      return Encoding.Key.read(buffer);
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
    return nextKey == null ? null : nextKey.toEntryKey();
  }

  @Override
  public Entry next() throws IOException {
    Preconditions.checkState(!closed, "closed");
    if (nextKey == null) {
      return null;
    }

    Entry entry;
    if (nextKey.type == Type.PENDING) {
      buffer.position(0).limit(PendingPreamble.SIZE);
      channel.read(buffer);
      buffer.flip();
      var preamble = PendingPreamble.read(buffer);
      var buf = getPreparedBuffer(preamble.valueLength);
      channel.read(buf);
      buf.flip();
      var val = ByteString.copyFrom(buf);
      entry = Entry.newBuilder()
          .setPending(Item.newBuilder()
              .setId(nextKey.id)
              .setDeadline(Encoding.fromTimestamp(nextKey.deadline))
              .setStats(preamble.stats.toApiStats())
              .setValue(val))
          .build();
    } else {
      Verify.verify(nextKey.type == Type.TOMBSTONE);
      entry = Entry.newBuilder()
          .setTombstone(nextKey.toEntryKey())
          .build();
    }

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
    return new ImmutableSegment(input, footer, firstKey == null ? null : firstKey.toEntryKey(),
        nextKey, limit);
  }

  private static Footer readFooter(SeekableByteChannel input) throws IOException {
    long original = input.position();
    try {
      input.position(input.size() - FOOTER_SIZE);
      var bytes = new byte[FOOTER_SIZE];
      var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      input.read(buffer);
      buffer.flip();
      return Encoding.Footer.read(buffer).toApiFooter();
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
    var lastEntry = Entry.getDefaultInstance();
    long maxId = Long.MIN_VALUE;
    long pendingCount = 0;
    long tombstoneCount = 0;

    // TODO: Naming...?
    var bb = ByteBuffer.wrap(new byte[128]).order(ByteOrder.LITTLE_ENDIAN);

    for (Entry entry = segment.next(); entry != null; entry = segment.next()) {

      // Reset
      bb.position(0);
      bb.limit(bb.capacity());

      var key = entryKey(entry);
      if (trackOffsets) {
        offsets.put(key, offsetBase + buffer.size());
      }

      Encoding.Key.fromEntry(entry).write(bb);
      if (entry.hasPending()) {
        PendingPreamble.fromPending(entry.getPending()).write(bb);

        var val = entry.getPending().getValue();
        if (bb.remaining() < val.size()) {
          var newBB = ByteBuffer.wrap(new byte[bb.position() + val.size()])
              .order(ByteOrder.LITTLE_ENDIAN);
          bb.flip();
          newBB.put(bb);
          bb = newBB;
        }
        val.copyTo(bb);
      }

      // Copy bb to buffer
      bb.flip();
      buffer.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());

      if (buffer.size() > (bufferSize >> 1)) {
        buffer.writeTo(output);
        offsetBase += buffer.size();
        buffer.reset();
      }

      lastEntry = entry;
      maxId = Math.max(maxId, key.getId());
      if (entry.hasPending()) {
        pendingCount++;
      }
      if (entry.hasTombstone()) {
        tombstoneCount++;
      }
    }

    // Create buffer for fixed sized footer
    var footerBytes = new byte[FOOTER_SIZE];
    var footerBB = ByteBuffer.wrap(footerBytes).order(ByteOrder.LITTLE_ENDIAN);

    new Encoding.Footer(
        pendingCount,
        tombstoneCount,
        Encoding.Key.fromEntry(lastEntry),
        maxId
    ).write(footerBB);

    // Write footer to output buffer
    buffer.write(footerBytes);

    // Flush buffer to output
    buffer.writeTo(output);

    return offsets;
  }
}
