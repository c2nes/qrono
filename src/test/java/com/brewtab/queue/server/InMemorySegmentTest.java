package com.brewtab.queue.server;

import static com.brewtab.queue.server.TestData.BASE_TIME;
import static com.brewtab.queue.server.TestData.PENDING_1_T5;
import static com.brewtab.queue.server.TestData.PENDING_2_T0;
import static com.brewtab.queue.server.TestData.TOMBSTONE_3_T10;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.server.data.Entry.Type;
import com.brewtab.queue.server.data.ImmutableEntry.Key;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import java.io.IOException;
import java.util.List;
import org.junit.Test;

public class InMemorySegmentTest {
  @Test
  public void testSize() {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));
    assertEquals(3, segment.size());
  }

  @Test
  public void testSize_empty() {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of());
    assertEquals(0, segment.size());
  }

  @Test
  public void testMetadata() {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));

    var expected = ImmutableSegmentMetadata.builder()
        .maxId(1003)
        .pendingCount(2)
        .tombstoneCount(1)
        .build();

    assertEquals(expected, segment.metadata());
  }

  @Test
  public void testMetadata_empty() {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of());

    var expected = ImmutableSegmentMetadata.builder()
        .maxId(0)
        .pendingCount(0)
        .tombstoneCount(0)
        .build();

    assertEquals(expected, segment.metadata());
  }

  @Test
  public void testName() {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of());
    assertEquals(name, segment.name());
  }

  @Test
  public void testNewReader() throws IOException {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));
    var reader = segment.newReader();
    assertNotNull(reader);
    assertEquals(PENDING_2_T0.key(), reader.peek());
  }

  @Test
  public void testNewReader_offsetEqualsKey() throws IOException {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));

    var reader = segment.newReader(PENDING_2_T0.key());
    assertNotNull(reader);
    assertEquals(PENDING_1_T5.key(), reader.peek());
  }

  @Test
  public void testNewReader_offsetBetweenKeys() throws IOException {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));

    var reader = segment.newReader(Key.builder()
        .entryType(Type.PENDING)
        .deadline(ImmutableTimestamp.of(BASE_TIME + 2))
        .id(1999)
        .build());

    assertNotNull(reader);
    assertEquals(PENDING_1_T5.key(), reader.peek());
  }

  @Test
  public void testNewReader_offsetBeforeFirstKey() throws IOException {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));

    var reader = segment.newReader(Key.builder()
        .entryType(Type.PENDING)
        .deadline(ImmutableTimestamp.of(BASE_TIME - 1))
        .id(1999)
        .build());

    assertNotNull(reader);
    assertEquals(PENDING_2_T0.key(), reader.peek());
  }

  @Test
  public void testNewReader_offsetEqualsLastKey() throws IOException {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));

    var reader = segment.newReader(TOMBSTONE_3_T10.key());

    assertNotNull(reader);
    assertNull(reader.peek());
  }

  @Test
  public void testNewReader_offsetAfterLastKey() throws IOException {
    var name = new SegmentName(0, 1);
    var segment = new InMemorySegment(name, List.of(PENDING_1_T5, PENDING_2_T0, TOMBSTONE_3_T10));

    var reader = segment.newReader(Key.builder()
        .entryType(Type.PENDING)
        .deadline(ImmutableTimestamp.of(BASE_TIME + 15))
        .id(1999)
        .build());

    assertNotNull(reader);
    assertNull(reader.peek());
  }
}
