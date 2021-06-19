package net.qrono.server;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import net.qrono.server.data.Entry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.Item;
import net.qrono.server.data.Timestamp;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueueDataTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void tearDown() throws Exception {
    System.gc();
    Thread.sleep(250);
    System.gc();
    Thread.sleep(250);
  }

  private final TaskScheduler ioScheduler = new ExecutorTaskScheduler(directExecutor());
  private final SegmentFlushScheduler segmentFlushScheduler =
      new SegmentFlushScheduler(1024 * 1024);

  @Test
  public void testLoad() throws IOException {
    var n = 128 * 1024;
    var directory = temporaryFolder.getRoot().toPath();
    var writer = new StandardSegmentWriter(directory);
    var data = new QueueData(directory, ioScheduler, writer, segmentFlushScheduler);
    data.startAsync().awaitRunning();
    var item = ImmutableItem.builder()
        .deadline(Timestamp.ZERO)
        .stats(ImmutableItem.Stats.builder()
            .enqueueTime(Timestamp.ZERO)
            .requeueTime(Timestamp.ZERO)
            .dequeueCount(0)
            .build())
        .value(Unpooled.EMPTY_BUFFER);
    for (int i = 0; i < n; i++) {
      data.write(Entry.newPendingEntry(item.id(i).build()));
    }
    data.stopAsync().awaitTerminated();

    // Re-open
    data = new QueueData(directory, ioScheduler, writer, segmentFlushScheduler);
    data.startAsync().awaitRunning();

    for (int i = 0; i < n; i++) {
      assertEquals(i, assertPendingThenRelease(data.next()).id());
    }

    assertNull(data.next());
  }

  @Test
  public void testLoadWithTombstones() throws IOException, InterruptedException {
    var directory = temporaryFolder.getRoot().toPath();
    var writer = new StandardSegmentWriter(directory);
    var data = new QueueData(directory, ioScheduler, writer, segmentFlushScheduler);
    data.startAsync().awaitRunning();

    var item = ImmutableItem.builder()
        .deadline(Timestamp.ZERO)
        .stats(ImmutableItem.Stats.builder()
            .enqueueTime(Timestamp.ZERO)
            .requeueTime(Timestamp.ZERO)
            .dequeueCount(0)
            .build())
        .value(Unpooled.EMPTY_BUFFER);

    for (int i = 0; i < 10; i++) {
      data.write(Entry.newPendingEntry(item.id(i).build()));
    }

    // TODO: Test freezing the current segment after we've dequeued items from it.

    assertEquals(0, assertPendingThenRelease(data.next()).id());

    // Force flush otherwise entry and tombstone will be merged
    // in-memory and not written to disk.
    data.flushCurrentSegment();

    assertEquals(1, assertPendingThenRelease(data.next()).id());
    assertEquals(2, assertPendingThenRelease(data.next()).id());

    var entry = data.next();
    assertEquals(3, assertPendingThenRelease(entry).id());

    // Tombstone entry 3
    data.write(Entry.newTombstoneEntry(entry.key()));

    // Close and re-open
    data.stopAsync().awaitTerminated();
    data = new QueueData(directory, ioScheduler, writer, segmentFlushScheduler);
    data.startAsync().awaitRunning();

    assertEquals(0, assertPendingThenRelease(data.next()).id());
    assertEquals(1, assertPendingThenRelease(data.next()).id());
    assertEquals(2, assertPendingThenRelease(data.next()).id());
    // Item ID 3 was tombstoned and should not appear!
    // assertEquals(3, assertPending(data.next()).id());
    assertEquals(4, assertPendingThenRelease(data.next()).id());
    assertEquals(5, assertPendingThenRelease(data.next()).id());
  }

  static Item assertPendingThenRelease(Entry entry) {
    Item item = entry.item();
    assertNotNull(item);
    assertTrue(entry.isPending());
    ReferenceCountUtil.release(entry);
    return item;
  }
}