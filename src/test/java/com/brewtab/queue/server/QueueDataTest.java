package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.Timestamp;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class QueueDataTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testLoad() throws IOException, InterruptedException {
    var directory = temporaryFolder.getRoot().toPath();
    var writer = new StandardSegmentWriter(directory);
    var data = new QueueData(directory, new DirectIOScheduler(), writer);
    var item = ImmutableItem.builder()
        .deadline(Timestamp.ZERO)
        .stats(ImmutableItem.Stats.builder()
            .enqueueTime(Timestamp.ZERO)
            .requeueTime(Timestamp.ZERO)
            .dequeueCount(0)
            .build())
        .value(ByteString.EMPTY);
    for (int i = 0; i < 10 * 128 * 1024; i++) {
      data.write(Entry.newPendingEntry(item.id(i).build()));
    }
    data.close();

    // Re-open
    data = new QueueData(directory, new DirectIOScheduler(), writer);
    data.load();

    for (int i = 0; i < 10 * 128 * 1024; i++) {
      assertEquals(i, assertPending(data.next()).id());
    }

    assertNull(data.next());
  }

  @Test
  public void testLoadWithTombstons() throws IOException, InterruptedException {
    var directory = temporaryFolder.getRoot().toPath();
    var writer = new StandardSegmentWriter(directory);
    var data = new QueueData(directory, new DirectIOScheduler(), writer);
    var item = ImmutableItem.builder()
        .deadline(Timestamp.ZERO)
        .stats(ImmutableItem.Stats.builder()
            .enqueueTime(Timestamp.ZERO)
            .requeueTime(Timestamp.ZERO)
            .dequeueCount(0)
            .build())
        .value(ByteString.EMPTY);

    for (int i = 0; i < 10; i++) {
      data.write(Entry.newPendingEntry(item.id(i).build()));
    }

    // Force flush otherwise entry and tombstone will be merged
    // in-memory and not written to disk.
    data.flushCurrentSegment();

    assertEquals(0, assertPending(data.next()).id());
    assertEquals(1, assertPending(data.next()).id());
    assertEquals(2, assertPending(data.next()).id());

    var entry = data.next();
    assertEquals(3, assertPending(entry).id());

    // Tombstone entry 3
    data.write(Entry.newTombstoneEntry(entry.key()));

    // Close and re-open
    data.close();
    data = new QueueData(directory, new DirectIOScheduler(), writer);
    data.load();

    assertEquals(0, assertPending(data.next()).id());
    assertEquals(1, assertPending(data.next()).id());
    assertEquals(2, assertPending(data.next()).id());
    // Item ID 3 was tombstoned and should not appear!
    // assertEquals(3, assertPending(data.next()).id());
    assertEquals(4, assertPending(data.next()).id());
    assertEquals(5, assertPending(data.next()).id());
  }

  static Item assertPending(Entry entry) {
    Item item = entry.item();
    assertNotNull(item);
    assertTrue(entry.isPending());
    return item;
  }

  static class DirectIOScheduler implements IOScheduler {
    @Override
    public <V> CompletableFuture<V> schedule(Callable<V> operation, Parameters parameters) {
      try {
        return CompletableFuture.completedFuture(operation.call());
      } catch (Exception e) {
        return CompletableFuture.failedFuture(e);
      }
    }
  }
}