package net.qrono.server;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.collect.ImmutableList;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.zip.Adler32;
import net.qrono.server.data.Entry;

public class StandardWriteAheadLog implements WriteAheadLog {
  public static final Duration DEFAULT_SYNC_INTERVAL = Duration.ofSeconds(1);

  private final PooledByteBufAllocator bufAllocator = new PooledByteBufAllocator();

  private final Path directory;
  private final SegmentName segmentName;
  private final Duration syncInterval;
  private final FileChannel out;
  private Instant nextSyncDeadline;

  // TODO: Dependency inject Clock
  // TODO: Consider WriteAheadLogFactory to capture sync parameters and clock?

  // TODO: Some sort of "SyncableOutputStream" interface would make testing easier
  //  (e.g. we could pass byte array backed OutputStreams or Channels)
  private StandardWriteAheadLog(
      Path directory,
      SegmentName segmentName,
      Duration syncInterval,
      FileChannel out
  ) {
    this.directory = directory;
    this.segmentName = segmentName;
    this.syncInterval = syncInterval;
    this.out = out;
    nextSyncDeadline = Instant.now().plus(syncInterval);
  }

  @Override
  public void append(Entry entry) throws IOException {
    append(List.of(entry));
  }

  @Override
  public void append(List<Entry> entries) throws IOException {
    if (entries.isEmpty()) {
      return;
    }

    // [entries size:32][checksum:32]
    int headerSize = 4 + 4;
    int entriesSize = entries.stream()
        .mapToInt(Encoding::entrySize)
        .sum();

    var buf = bufAllocator.buffer(headerSize + entriesSize);

    try {
      buf.writeInt(entriesSize);
      buf.writerIndex(8); // We'll write the checksum later

      for (Entry entry : entries) {
        Encoding.writeEntry(buf, entry);
      }

      // Compute checksum
      var xsum = new Adler32();
      xsum.update(buf.nioBuffer(8, entriesSize));
      buf.setInt(4, (int) xsum.getValue());

      // Write entire buffer
      buf.readBytes(out, headerSize + entriesSize);
    } finally {
      buf.release();
    }

    Instant now = Instant.now();
    if (now.isAfter(nextSyncDeadline)) {
      out.force(false);
      nextSyncDeadline = now.plus(syncInterval);
    }
  }

  @Override
  public void close() throws IOException {
    out.force(false);
    out.close();

    Files.move(
        SegmentFiles.getLogPath(directory, segmentName),
        SegmentFiles.getClosedLogPath(directory, segmentName),
        ATOMIC_MOVE);
  }

  public static WriteAheadLog create(Path directory, SegmentName segmentName) throws IOException {
    return create(directory, segmentName, DEFAULT_SYNC_INTERVAL);
  }

  public static WriteAheadLog create(Path directory, SegmentName segmentName, Duration syncInterval)
      throws IOException {
    var outputPath = SegmentFiles.getLogPath(directory, segmentName);
    var output = FileChannel.open(outputPath, WRITE, CREATE);
    return new StandardWriteAheadLog(directory, segmentName, syncInterval, output);
  }

  public static List<Entry> read(Path path) throws IOException {
    // TODO: Handle corruption or at least truncated writes
    var entries = new ImmutableList.Builder<Entry>();
    var buf = Unpooled.wrappedBuffer(Files.readAllBytes(path));

    while (buf.readableBytes() > 8) {
      int size = buf.readInt();
      int xsum = buf.readInt();
      if (buf.readableBytes() < size) {
        // TODO: Throw exception which wraps successfully read entries
        throw new IOException("truncated");
      }

      var computedXsum = new Adler32();
      computedXsum.update(buf.nioBuffer(buf.readerIndex(), size));
      int computed = ((int) computedXsum.getValue());
      if (xsum != computed) {
        // TODO: Throw exception wrapping succesfully read entries
        // TODO: If there's still readable bytes then something has gone extra wrong
        throw new IOException(String.format("checksum failure; %x != %x", xsum, computed));
      }

      // Read entries in this chunk
      var endOfChunk = buf.readerIndex() + size;
      while (buf.readerIndex() < endOfChunk) {
        entries.add(Encoding.readEntry(buf));
      }
    }

    // Shouldn't be any readable bytes left
    if (buf.isReadable()) {
      throw new IOException("truncated");
    }

    return entries.build();
  }

  public static void delete(Path directory, SegmentName segmentName) throws IOException {
    Files.delete(SegmentFiles.getClosedLogPath(directory, segmentName));
  }
}
