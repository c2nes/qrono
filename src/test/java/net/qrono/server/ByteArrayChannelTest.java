package net.qrono.server;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import org.junit.Test;

public class ByteArrayChannelTest {
  @Test
  public void testRead() {
    var ch = new ByteArrayChannel(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
    var buf = ByteBuffer.allocate(4);
    assertEquals(0, ch.position());

    assertEquals(4, ch.read(buf));
    assertEquals(4, ch.position());
    assertEquals(4, buf.position());
    assertEquals(1, buf.get(0));
    assertEquals(2, buf.get(1));
    assertEquals(3, buf.get(2));
    assertEquals(4, buf.get(3));

    assertEquals(0, ch.read(buf));
    buf.position(0).limit(buf.capacity());

    assertEquals(4, ch.read(buf));
    assertEquals(8, ch.position());
    assertEquals(4, buf.position());
    assertEquals(5, buf.get(0));
    assertEquals(6, buf.get(1));
    assertEquals(7, buf.get(2));
    assertEquals(8, buf.get(3));
  }
}