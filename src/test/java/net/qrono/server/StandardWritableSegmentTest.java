package net.qrono.server;

import static io.netty.util.ReferenceCountUtil.releaseLater;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.netty.util.ReferenceCountUtil;
import java.util.List;
import net.qrono.server.data.Entry;
import org.junit.Test;

public class StandardWritableSegmentTest {
  @Test
  public void test() throws Exception {
    var name = new SegmentName(0, 0);
    var wal = new MockWriteAheadLog();
    var segment = new StandardWritableSegment(name, wal);

    segment.add(TestData.PENDING_4_T15);
    segment.add(TestData.PENDING_3_T10);
    segment.add(TestData.TOMBSTONE_2_T0);
    segment.add(TestData.TOMBSTONE_1_T5);

    assertEquals(TestData.PENDING_3_T10, releaseLater(segment.next()));
    assertEquals(TestData.PENDING_4_T15, releaseLater(segment.next()));
    assertNull(segment.next());
  }

  private static class MockWriteAheadLog implements WriteAheadLog {
    @Override public void append(Entry entry) { }

    @Override public void append(List<Entry> entries) { }

    @Override public void close() { }
  }
}
