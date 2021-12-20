package net.qrono.server;

import static net.qrono.server.TestData.PENDING_1_T5;
import static net.qrono.server.TestData.PENDING_2_T0;
import static net.qrono.server.TestData.PENDING_3_T10;
import static net.qrono.server.TestData.PENDING_4_T15;
import static net.qrono.server.TestData.PENDING_5_T20;
import static net.qrono.server.TestData.TOMBSTONE_1_T5;
import static net.qrono.server.TestData.TOMBSTONE_3_T10;
import static net.qrono.server.TestData.TOMBSTONE_4_T15;
import static net.qrono.server.TestData.TOMBSTONE_5_T20;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import net.qrono.server.data.Entry.Key;
import org.junit.Ignore;
import org.junit.Test;

public class MergedSegmentReaderTest {

  @Test
  public void testEmpty() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    assertNull(reader.peek());
    assertNull(reader.next());
  }

  @Test
  public void testSingleSegment() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0, PENDING_3_T10));

    reader.addSegment(segment, Key.ZERO);
    assertEquals(PENDING_2_T0.key(), reader.peek());
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
  }

  @Test
  public void testSkipTombstonePendingPair() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0, PENDING_3_T10));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(TOMBSTONE_1_T5, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);
    reader.addSegment(segment1, Key.ZERO);

    assertTrue(PENDING_1_T5.mirrors(TOMBSTONE_1_T5));
    assertTrue(TOMBSTONE_1_T5.mirrors(PENDING_1_T5));

    // Pending and tombstone for 1_T5 cancel out and should be skipped.
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testAlternateReads() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_2_T0, PENDING_3_T10));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_1_T5, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);
    reader.addSegment(segment1, Key.ZERO);

    // Entries are interleaved so reads should alternate between the segments
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testContinueAfterExhausted() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_3_T10, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);
    reader.addSegment(segment1, Key.ZERO);

    // Segments are sequential. Reads should continue from segment1 after segment0 is exhausted.
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testAddImmediatelyExhaustedSegment() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_3_T10, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);
    // Add segment1, but position at the end so it is immediately exhausted.
    reader.addSegment(segment1, PENDING_4_T15.key());

    // Should read items from segment0, but we opened segment1 at the end so
    // we should read none of its items.
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testAddNewNextEntry() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_5_T20));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_2_T0, PENDING_3_T10, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);

    // The reader only contains segment0 currently so we should read 1_T5.
    // Peeking should then return 5_T20, but this will change after we added segment1.
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_5_T20.key(), reader.peek());

    // Confirm that after adding segment1 that the head of the reader updates accordingly.
    reader.addSegment(segment1, Key.ZERO);
    assertEquals(PENDING_2_T0.key(), reader.peek());

    // Read the remaining items.
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertEquals(PENDING_5_T20, reader.next());
    assertNull(reader.next());
  }

  // testAddNewNextEntry variant that covers the special handling of buffer, unpaired tombstones.
  @Test
  public void testAddNewNextEntry_NextWasUnpairedTombstone() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, TOMBSTONE_5_T20));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_2_T0, PENDING_3_T10, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);

    // The reader only contains segment0 currently so we should read 1_T5.
    // Peeking should then return 5_T20, but this will change after we added segment1.
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(TOMBSTONE_5_T20.key(), reader.peek());

    // Confirm that after adding segment1 that the head of the reader updates accordingly.
    reader.addSegment(segment1, Key.ZERO);
    assertEquals(PENDING_2_T0.key(), reader.peek());

    // Read the remaining items.
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertEquals(TOMBSTONE_5_T20, reader.next());
    assertNull(reader.next());
  }

  // testAddNewNextEntry variant that covers the special handling of buffer, unpaired tombstones.
  @Test
  @Ignore("FIXME: This test currently fails due to the bug mentioned below")
  public void testReplaceSegments_NextWasUnpairedTombstone() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_2_T0, TOMBSTONE_4_T15));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_1_T5, PENDING_5_T20));

    reader.addSegment(segment0, Key.ZERO);

    // The reader only contains segment0 currently so we should read 1_T5.
    // Peeking should then return 5_T20, but this will change after we added segment1.
    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(TOMBSTONE_4_T15.key(), reader.peek());

    // Confirm that after adding segment1 that the head of the reader updates accordingly.
    reader.replaceSegments(List.of(segment0), segment0, PENDING_2_T0.key());
    reader.addSegment(segment1, PENDING_2_T0.key());
    assertEquals(PENDING_1_T5.key(), reader.peek());

    // Read the remaining items.
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(TOMBSTONE_4_T15, reader.next());
    // BUG! TOMBSTONE_4_15 is returned twice!
    assertEquals(PENDING_5_T20, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testAddMatchingEntryForTombstone() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, TOMBSTONE_3_T10));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_3_T10, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);

    // The reader only contains segment0 currently so TOMBSTONE_3_T10 should not be skipped.
    // Peeking should therefore return TOMBSTONE_3_T10, but this will change when we added segment1.
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(TOMBSTONE_3_T10.key(), reader.peek());

    // Confirm that after adding segment1 that the head of the reader updates accordingly.
    reader.addSegment(segment1, Key.ZERO);
    assertEquals(PENDING_4_T15.key(), reader.peek());

    // Read the remaining items.
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testReplaceCurrentHeadSegment() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_3_T10, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);

    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5.key(), reader.peek());
    reader.replaceSegments(List.of(segment0), segment1, Key.ZERO);
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testReplaceSegments_SomeExhausted() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0));
    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_3_T10, PENDING_4_T15));
    Segment segment2 = new InMemorySegment(
        new SegmentName(0, 2),
        List.of(PENDING_5_T20));

    reader.addSegment(segment0, Key.ZERO);
    reader.addSegment(segment1, Key.ZERO);

    assertEquals(PENDING_2_T0, reader.next());
    assertEquals(PENDING_1_T5, reader.next());
    assertEquals(PENDING_3_T10, reader.next());
    reader.replaceSegments(List.of(segment0, segment1), segment2, Key.ZERO);
    assertEquals(PENDING_5_T20, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testReplaceSegments_ReplacementIsExhausted() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_1_T5, PENDING_2_T0));
    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_3_T10, PENDING_4_T15));
    Segment segment2 = new InMemorySegment(
        new SegmentName(0, 2),
        List.of(PENDING_5_T20));

    reader.addSegment(segment0, Key.ZERO);
    reader.addSegment(segment1, Key.ZERO);

    assertEquals(PENDING_2_T0, reader.next());
    // Here we replace segment0 (which has one value left to read) with segment2.
    // However, we specify a position at the very end of segment2 so there will not be
    // any values to read. Thus, the remaining values we read should all come from segment1.
    reader.replaceSegments(List.of(segment0), segment2, PENDING_5_T20.key());
    assertEquals(PENDING_3_T10, reader.next());
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  @Test
  public void testPeekEntry() throws IOException {
    MergedSegmentReader reader = new MergedSegmentReader();
    Segment segment0 = new InMemorySegment(
        new SegmentName(0, 0),
        List.of(PENDING_2_T0, PENDING_3_T10));

    Segment segment1 = new InMemorySegment(
        new SegmentName(0, 1),
        List.of(PENDING_1_T5, PENDING_4_T15));

    reader.addSegment(segment0, Key.ZERO);
    reader.addSegment(segment1, Key.ZERO);

    // Entries are interleaved so reads should alternate between the segments
    assertEquals(PENDING_2_T0, reader.next());

    assertEquals(PENDING_1_T5, reader.peekEntry());
    assertEquals(PENDING_1_T5.key(), reader.peek());
    assertEquals(PENDING_1_T5, reader.next());

    assertEquals(PENDING_3_T10, reader.next());

    assertEquals(PENDING_4_T15, reader.peekEntry());
    assertEquals(PENDING_4_T15, reader.next());
    assertNull(reader.next());
  }

  // Empty
  // Single segment
  // Matching entries (tombstone and pending)
  // Exhaust segment, move to next
  // Add immediately exhausted segment
  // Alternate reads between segments

  // No segments
  // All segments exhausted

  // Head switches when segment exhausted
  // Head switches when another segment should be head

  // Add segment when there is no head
  // Add segment that should be new head
  // Add segment that should not be new head

  // Replace segment when...
  // ...head is replaced
  // ...head is not replaced
  // ...exhausted segments are replaced

  // Get segments returns exhausted segments
  // Get segments updates when segments are added
  // Get segments updates when segments are replaced

}