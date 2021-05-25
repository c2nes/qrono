package net.qrono.server;

import static org.junit.Assert.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import org.junit.Test;

public class EncodingTest {

  @Test
  public void testPendingKey() {
    var expected = TestData.PENDING_1_T5.key();

    var bb = newByteBuffer();
    assertEquals(Encoding.KEY_SIZE, Encoding.writeKey(bb, expected));
    bb.flip();

    var actual = Encoding.readKey(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testPendingKey_ByteBuf() {
    var expected = TestData.PENDING_1_T5.key();

    var bb = newByteBuf();
    assertEquals(Encoding.KEY_SIZE, Encoding.writeKey(bb, expected));

    var actual = Encoding.readKey(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testTombstoneKey() {
    var expected = TestData.TOMBSTONE_1_T5.key();

    var bb = newByteBuffer();
    assertEquals(Encoding.KEY_SIZE, Encoding.writeKey(bb, expected));
    bb.flip();

    var actual = Encoding.readKey(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testTombstoneKey_ByteBuf() {
    var expected = TestData.TOMBSTONE_1_T5.key();

    var bb = newByteBuf();
    assertEquals(Encoding.KEY_SIZE, Encoding.writeKey(bb, expected));

    var actual = Encoding.readKey(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testStats() {
    var expected = TestData.ZERO_STATS;

    var bb = newByteBuffer();
    assertEquals(Encoding.STATS_SIZE, Encoding.writeStats(bb, expected));
    bb.flip();

    var actual = Encoding.readStats(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testStats_ByteBuf() {
    var expected = TestData.ZERO_STATS;

    var bb = newByteBuf();
    assertEquals(Encoding.STATS_SIZE, Encoding.writeStats(bb, expected));

    var actual = Encoding.readStats(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testFooter() {
    var expected = ImmutableEncoding.Footer.builder()
        .maxId(123)
        .pendingCount(456)
        .tombstoneCount(789)
        .build();

    var bb = newByteBuffer();
    assertEquals(Encoding.FOOTER_SIZE, Encoding.writeFooter(bb, expected));
    bb.flip();

    var actual = Encoding.readFooter(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testPendingEntry_ByteBuf() {
    var expected = TestData.PENDING_1_T5;

    var bb = newByteBuf();
    Encoding.writeEntry(bb, expected);

    var actual = Encoding.readEntry(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testTombstoneEntry_ByteBuf() {
    var expected = TestData.TOMBSTONE_1_T5;

    var bb = newByteBuf();
    Encoding.writeEntry(bb, expected);

    var actual = Encoding.readEntry(bb);
    assertEquals(expected, actual);
  }

  @Test
  public void testPendingEntrySize() {
    assertEquals(
        TestData.VALUE.readableBytes() + Encoding.KEY_SIZE + Encoding.STATS_SIZE + 4,
        Encoding.entrySize(TestData.PENDING_1_T5));
  }

  @Test
  public void testTombstoneEntrySize() {
    assertEquals(
        Encoding.KEY_SIZE,
        Encoding.entrySize(TestData.TOMBSTONE_1_T5));
  }

  private static ByteBuffer newByteBuffer() {
    return ByteBuffer.allocate(64);
  }

  private static ByteBuf newByteBuf() {
    return Unpooled.buffer();
  }
}