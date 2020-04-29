package com.brewtab.queue.server;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Test;

public class EncodingTest {

  @Test
  public void testPendingKey() {
    Random random = new Random();
    var expected = ImmutableEntry.Key.builder()
        .deadline(ImmutableTimestamp.of(currentTimeMillis()))
        .id(random.nextLong())
        .entryType(Entry.Type.PENDING)
        .build();
    var bb = ByteBuffer.allocate(Encoding.KEY_SIZE);
    assertEquals(Encoding.KEY_SIZE, Encoding.writeKey(bb, expected));
    bb.flip();

    var actual = Encoding.readKey(bb);
    assertEquals(expected, actual);
  }

}