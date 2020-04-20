package com.brewtab.queue.server;

import static org.junit.Assert.*;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
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
      assertEquals(i, data.next().getPending().getId());
    }

    assertNull(data.next());
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