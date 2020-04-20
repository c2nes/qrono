package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;
import static org.junit.Assert.*;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.nio.file.Path;
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
    var item = Item.newBuilder()
        .setDeadline(Timestamps.EPOCH)
        .build();
    for (int i = 0; i < 10 * 128 * 1024; i++) {
      data.write(Entry.newBuilder()
          .setPending(item.toBuilder().setId(i))
          .build());
    }
    data.close();

    // Re-open
    data = new QueueData(directory, new DirectIOScheduler(), writer);
    data.load();

    for (int i = 0; i < 10 * 128 * 1024; i++) {
      assertEquals(i, assertPending(data.next()).getId());
    }

    assertNull(data.next());
  }

  @Test
  public void testLoadWithTombstons() throws IOException, InterruptedException {
    var directory = temporaryFolder.getRoot().toPath();
    var writer = new StandardSegmentWriter(directory);
    var data = new QueueData(directory, new DirectIOScheduler(), writer);
    var item = Item.newBuilder()
        .setDeadline(Timestamps.EPOCH)
        .build();

    for (int i = 0; i < 10; i++) {
      data.write(Entry.newBuilder()
          .setPending(item.toBuilder().setId(i))
          .build());
    }

    // Force flush otherwise entry and tombstone will be merged
    // in-memory and not written to disk.
    data.flushCurrentSegment();

    assertEquals(0, assertPending(data.next()).getId());
    assertEquals(1, assertPending(data.next()).getId());
    assertEquals(2, assertPending(data.next()).getId());

    var entry = data.next();
    assertEquals(3, assertPending(entry).getId());

    // Tombstone entry 3
    data.write(Entry.newBuilder()
        .setTombstone(entryKey(entry))
        .build());

    // Close and re-open
    data.close();
    data = new QueueData(directory, new DirectIOScheduler(), writer);
    data.load();

    assertEquals(0, assertPending(data.next()).getId());
    assertEquals(1, assertPending(data.next()).getId());
    assertEquals(2, assertPending(data.next()).getId());
    // Item ID 3 was tombstoned and should not appear!
    // assertEquals(3, assertPending(data.next()).getId());
    assertEquals(4, assertPending(data.next()).getId());
    assertEquals(5, assertPending(data.next()).getId());
  }

  static Item assertPending(Entry entry) {
    assertEquals(EntryCase.PENDING, entry.getEntryCase());
    return entry.getPending();
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