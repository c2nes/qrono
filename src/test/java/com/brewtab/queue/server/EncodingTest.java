package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;

import com.brewtab.queue.server.Encoding.Key;
import com.brewtab.queue.server.Encoding.Key.Type;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Test;

public class EncodingTest {

  @Test
  public void testPendingKey() {
    Random random = new Random();
    var expected = new Key(System.currentTimeMillis(), random.nextLong(), Type.PENDING);
    var bb = ByteBuffer.allocate(Key.SIZE);
    expected.write(bb);
    bb.flip();

    var actual = Key.read(bb);
    assertEquals(expected.id, actual.id);
    assertEquals(expected.deadline, actual.deadline);
    assertEquals(expected.type, actual.type);
  }

}