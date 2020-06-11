package com.brewtab.queue.server;

import static com.brewtab.queue.server.Encoding.FOOTER_SIZE;
import static com.brewtab.queue.server.Encoding.KEY_SIZE;
import static com.brewtab.queue.server.Encoding.STATS_SIZE;
import static com.brewtab.queue.server.ImmutableSegment.DEFAULT_BUFFER_SIZE;
import static com.brewtab.queue.server.TestData.PENDING_1_T5;
import static com.brewtab.queue.server.TestData.PENDING_2_T0;
import static com.brewtab.queue.server.TestData.PENDING_3_T10;
import static com.brewtab.queue.server.TestData.TOMBSTONE_2_T0;
import static com.brewtab.queue.server.TestData.VALUE;
import static com.brewtab.queue.server.TestData.withId;
import static com.brewtab.queue.server.TestData.withValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.util.ArrayList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ImmutableSegmentTest {
  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  @Test
  public void testRoundTripInMem() throws IOException {
    var memSegment = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0, PENDING_3_T10);

    // Item overhead (key + stats + value length)
    var itemOverhead = KEY_SIZE + STATS_SIZE + 4;
    var itemSize = itemOverhead + VALUE.size();
    var footerSize = FOOTER_SIZE;
    var expectedSize = 3 * itemSize + footerSize;

    var channel = new ByteArrayChannel();
    var writer = new ImmutableSegment.Writer(channel, memSegment, () -> Key.ZERO);
    writer.write();
    assertEquals(expectedSize, channel.position());

    // Reset position to 0 so we can read
    channel.position(0);

    ImmutableSegment.Reader reader = newReader(channel);

    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testRoundTripInMem_emptySegment() throws IOException {
    var memSegment = new InMemorySegmentReader();

    var channel = new ByteArrayChannel();
    var writer = new ImmutableSegment.Writer(channel, memSegment, () -> Key.ZERO);
    writer.write();

    // Should be footer only; no entries
    assertEquals(FOOTER_SIZE, channel.position());

    // Reset position to 0 so we can read
    channel.position(0);

    ImmutableSegment.Reader reader = newReader(channel);

    assertNull(reader.next());
  }

  @Test
  public void testRoundTripInMem_emptyValue() throws IOException {
    var entry = withValue(PENDING_1_T5, ByteString.EMPTY);
    var memSegment = new InMemorySegmentReader(entry);

    var channel = new ByteArrayChannel();
    var writer = new ImmutableSegment.Writer(channel, memSegment, () -> Key.ZERO);
    writer.write();

    // [key][stats][value length][value (0 bytes)][footer]
    var expectedSize = KEY_SIZE + STATS_SIZE + 4 + FOOTER_SIZE;
    assertEquals(expectedSize, channel.position());

    // Reset position to 0 so we can read
    channel.position(0);

    ImmutableSegment.Reader reader = newReader(channel);

    assertEquals(0, reader.next().item().value().size());
  }

  @Test
  public void testRoundTrip() throws IOException {
    var memSegment = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0, PENDING_3_T10);

    // Item overhead (key + stats + value length)
    var itemOverhead = KEY_SIZE + STATS_SIZE + 4;
    var itemSize = itemOverhead + VALUE.size();
    var footerSize = FOOTER_SIZE;
    var expectedSize = 3 * itemSize + footerSize;

    var segmentName = new SegmentName(123, 456);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    var segment = ImmutableSegment.write(path, memSegment, () -> Key.ZERO);
    assertEquals(expectedSize, Files.size(path));
    assertEquals(segmentName, segment.name());
    assertEquals(3, segment.metadata().pendingCount());
    assertEquals(0, segment.metadata().tombstoneCount());
    assertEquals(1003, segment.metadata().maxId());

    var reader = segment.newReader();

    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testRoundTrip_withTombstoneEntries() throws IOException {
    var memSegment = new InMemorySegmentReader(PENDING_1_T5, TOMBSTONE_2_T0, PENDING_3_T10);

    // Item overhead (key + stats + value length)
    var itemOverhead = KEY_SIZE + STATS_SIZE + 4;
    var itemSize = itemOverhead + VALUE.size();
    var footerSize = FOOTER_SIZE;
    var expectedSize = 2 * itemSize + KEY_SIZE + footerSize;

    var segmentName = new SegmentName(123, 456);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    var segment = ImmutableSegment.write(path, memSegment, () -> Key.ZERO);
    assertEquals(expectedSize, Files.size(path));
    assertEquals(segmentName, segment.name());
    assertEquals(2, segment.metadata().pendingCount());
    assertEquals(1, segment.metadata().tombstoneCount());
    assertEquals(1003, segment.metadata().maxId());

    var reader = segment.newReader();

    assertEquals(TOMBSTONE_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testRoundTrip_reopen() throws IOException {
    var memSegment = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0, PENDING_3_T10);

    // Item overhead (key + stats + value length)
    var itemOverhead = KEY_SIZE + STATS_SIZE + 4;
    var itemSize = itemOverhead + VALUE.size();
    var footerSize = FOOTER_SIZE;
    var expectedSize = 3 * itemSize + footerSize;

    var segmentName = new SegmentName(123, 456);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    ImmutableSegment.write(path, memSegment, () -> Key.ZERO);
    assertEquals(expectedSize, Files.size(path));

    // Ignore return value from write() and open segment manually.
    var segment = ImmutableSegment.open(path);
    assertEquals(segmentName, segment.name());
    assertEquals(3, segment.metadata().pendingCount());
    assertEquals(0, segment.metadata().tombstoneCount());
    assertEquals(1003, segment.metadata().maxId());

    var reader = segment.newReader();

    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testRoundTrip_emptySegment() throws IOException {
    var memSegment = new InMemorySegmentReader();

    var segmentName = new SegmentName(123, 456);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    var segment = ImmutableSegment.write(path, memSegment, () -> Key.ZERO);

    // Should be footer only; no entries
    assertEquals(FOOTER_SIZE, Files.size(path));
    assertEquals(segmentName, segment.name());
    assertEquals(0, segment.metadata().pendingCount());
    assertEquals(0, segment.metadata().tombstoneCount());
    assertEquals(0, segment.metadata().maxId());

    var reader = segment.newReader();

    assertNull(reader.next());
  }

  @Test
  public void testRoundTrip_emptyValue() throws IOException {
    var entry = withValue(PENDING_1_T5, ByteString.EMPTY);
    var memSegment = new InMemorySegmentReader(entry);

    var segmentName = new SegmentName(123, 456);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    var segment = ImmutableSegment.write(path, memSegment, () -> Key.ZERO);

    // [key][stats][value length][value (0 bytes)][footer]
    var expectedSize = KEY_SIZE + STATS_SIZE + 4 + FOOTER_SIZE;
    assertEquals(expectedSize, Files.size(path));

    assertEquals(segmentName, segment.name());
    assertEquals(1, segment.metadata().pendingCount());
    assertEquals(0, segment.metadata().tombstoneCount());
    assertEquals(1001, segment.metadata().maxId());

    var reader = segment.newReader();

    assertEquals(0, reader.next().item().value().size());
  }

  @Test
  public void testRoundTrip_valueLargerThanDefaultBuffer() throws IOException {
    var valueSize = DEFAULT_BUFFER_SIZE + 1;
    var value = ByteString.copyFrom(new byte[valueSize]);
    var entry = withValue(PENDING_1_T5, value);
    var memSegment = new InMemorySegmentReader(entry);

    var segmentName = new SegmentName(123, 456);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    var segment = ImmutableSegment.write(path, memSegment, () -> Key.ZERO);

    // [key][stats][value length][value][footer]
    var expectedSize = KEY_SIZE + STATS_SIZE + 4 + valueSize + FOOTER_SIZE;
    assertEquals(expectedSize, Files.size(path));

    assertEquals(segmentName, segment.name());
    assertEquals(1, segment.metadata().pendingCount());
    assertEquals(0, segment.metadata().tombstoneCount());
    assertEquals(1001, segment.metadata().maxId());

    var reader = segment.newReader();

    assertEquals(entry, reader.next());
  }

  @Test
  public void testRoundTrip_largeValues() throws IOException {
    // 256 entries, already in order.
    var entries = new ArrayList<Entry>();
    for (int id = 0; id < 256; id++) {
      var valueSize = 1024 * 10 * (id / 10) + id;
      var value = ByteString.copyFrom(new byte[valueSize]);
      var entry = withValue(withId(PENDING_1_T5, id), value);
      entries.add(entry);
    }

    var segmentName = new SegmentName(123, 456);
    var memSegment = new InMemorySegment(segmentName, entries);
    var path = SegmentFiles.getIndexPath(dir.getRoot().toPath(), segmentName);
    var segment = ImmutableSegment.write(path, memSegment.newReader(), () -> Key.ZERO);

    assertEquals(segmentName, segment.name());
    assertEquals(256, segment.metadata().pendingCount());
    assertEquals(0, segment.metadata().tombstoneCount());
    assertEquals(255, segment.metadata().maxId());

    var reader = segment.newReader();
    for (Entry entry : entries) {
      assertEquals(entry, reader.next());
    }
    assertNull(reader.next());
  }

  // TODO: Add tests covering offset tracking and opening to a specific position.

  private static ImmutableSegment.Reader newReader(SeekableByteChannel channel) throws IOException {
    ImmutableSegment.Reader reader = new ImmutableSegment.Reader(channel);
    // Reader requires an initial call to position() to read the first key. Usually this is
    // done by newReader(), but we're accessing the package-private Reader constructor so
    // we have to do it ourselves.
    reader.position(0);
    return reader;
  }
}