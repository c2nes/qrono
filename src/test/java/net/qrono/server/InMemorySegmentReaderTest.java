package net.qrono.server;

import static net.qrono.server.TestData.PENDING_1_T5;
import static net.qrono.server.TestData.PENDING_2_T0;
import static net.qrono.server.TestData.PENDING_3_T10;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class InMemorySegmentReaderTest {
  @Test
  public void testPeek() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    assertEquals(PENDING_2_T0.key(), reader.peek());
  }

  @Test
  public void testPeek_initiallyEmpty() {
    InMemorySegmentReader reader = new InMemorySegmentReader();
    assertNull(reader.peek());
  }

  @Test
  public void testNext_initiallyEmpty() {
    InMemorySegmentReader reader = new InMemorySegmentReader();
    assertNull(reader.next());
  }

  @Test
  public void testNext() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    assertEquals(PENDING_2_T0, reader.next());
  }

  @Test
  public void testPeek_afterNext() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    reader.next();
    assertEquals(PENDING_1_T5.key(), reader.peek());
  }

  @Test(expected = IllegalStateException.class)
  public void testPeek_afterClose() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    reader.close();
    reader.peek();
  }

  @Test(expected = IllegalStateException.class)
  public void testNext_afterClose() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    reader.close();
    reader.next();
  }

  @Test
  public void testPeek_noMoreEntries() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    assertNotNull(reader.next());
    assertNotNull(reader.next());
    assertNotNull(reader.next());
    assertNull(reader.peek());
  }

  @Test
  public void testNext_noMoreEntries() {
    InMemorySegmentReader reader = new InMemorySegmentReader(PENDING_1_T5, PENDING_2_T0,
        PENDING_3_T10);
    assertNotNull(reader.next());
    assertNotNull(reader.next());
    assertNotNull(reader.next());
    assertNull(reader.peek());
  }
}