package com.brewtab.queue.server;

import static com.brewtab.queue.server.TestData.PENDING_1_T5;
import static com.brewtab.queue.server.TestData.PENDING_2_T0;
import static com.brewtab.queue.server.TestData.PENDING_3_T10;
import static com.brewtab.queue.server.TestData.VALUE;
import static com.brewtab.queue.server.TestData.withValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.server.data.Entry.Key;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import org.junit.Test;

public class ImmutableSegmentTest {
  @Test
  public void testRoundTrip() throws IOException {
    var memSegment = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0, PENDING_3_T10);

    // Item overhead (key + stats + value length)
    var itemOverhead = Encoding.KEY_SIZE + Encoding.STATS_SIZE + 4;
    var itemSize = itemOverhead + VALUE.size();
    var footerSize = Encoding.FOOTER_SIZE;
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
  public void testRoundTrip_emptySegment() throws IOException {
    var memSegment = new InMemorySegmentReader();

    var channel = new ByteArrayChannel();
    var writer = new ImmutableSegment.Writer(channel, memSegment, () -> Key.ZERO);
    writer.write();

    // Should be footer only; no entries
    assertEquals(Encoding.FOOTER_SIZE, channel.position());

    // Reset position to 0 so we can read
    channel.position(0);

    ImmutableSegment.Reader reader = newReader(channel);

    assertNull(reader.next());
  }

  @Test
  public void testRoundTrip_emptyValue() throws IOException {
    var entry = withValue(PENDING_1_T5, ByteString.EMPTY);
    var memSegment = new InMemorySegmentReader(entry);

    var channel = new ByteArrayChannel();
    var writer = new ImmutableSegment.Writer(channel, memSegment, () -> Key.ZERO);
    writer.write();

    // [key][stats][value length][value (0 bytes)][footer]
    var expectedSize = Encoding.KEY_SIZE + Encoding.STATS_SIZE + 4 + Encoding.FOOTER_SIZE;
    assertEquals(expectedSize, channel.position());

    // Reset position to 0 so we can read
    channel.position(0);

    ImmutableSegment.Reader reader = newReader(channel);

    assertEquals(0, reader.next().item().value().size());
  }

  private static ImmutableSegment.Reader newReader(SeekableByteChannel channel) throws IOException {
    ImmutableSegment.Reader reader = new ImmutableSegment.Reader(channel);
    // Reader requires an initial call to position() to read the first key. Usually this is
    // done by newReader(), but we're accessing the package-private Reader constructor so
    // we have to do it ourselves.
    reader.position(0);
    return reader;
  }
}