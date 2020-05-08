package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Item.Stats;
import com.google.protobuf.ByteString;
import java.io.IOException;
import org.junit.Test;

public class ImmutableSegmentTest {
  @Test
  public void test() throws IOException {
    long baseTime = System.currentTimeMillis();
    StandardIdGenerator generator = new StandardIdGenerator(baseTime, 0);

    Stats stats = ImmutableItem.Stats.builder()
        .enqueueTime(ImmutableTimestamp.of(baseTime))
        .requeueTime(ImmutableTimestamp.of(baseTime))
        .dequeueCount(0)
        .build();

    ByteString value = ByteString.copyFromUtf8("Hello, world!");

    Entry entry1 = Entry.newPendingEntry(
        ImmutableItem.builder()
            .deadline(ImmutableTimestamp.of(baseTime))
            .id(generator.generateId())
            .stats(stats)
            .value(value)
            .build());
    Entry entry2 = Entry.newPendingEntry(
        ImmutableItem.builder()
            .deadline(ImmutableTimestamp.of(baseTime - 5))
            .id(generator.generateId())
            .stats(stats)
            .value(value)
            .build());
    Entry entry3 = Entry.newPendingEntry(
        ImmutableItem.builder()
            .deadline(ImmutableTimestamp.of(baseTime + 5))
            .id(generator.generateId())
            .stats(stats)
            .value(value)
            .build());

    InMemorySegmentReader memSegment = new InMemorySegmentReader(entry1, entry2, entry3);

    // Item overhead
    var itemOverhead = Encoding.KEY_SIZE + Encoding.STATS_SIZE + 4;
    var itemSize = itemOverhead + value.size();
    var footerSize = Encoding.FOOTER_SIZE;

    ByteArrayChannel channel = new ByteArrayChannel();
    new ImmutableSegment.Writer(channel, memSegment, () -> Key.ZERO).write();
    assertEquals(itemSize * 3 + footerSize, channel.position());

    channel.position(0);

    ImmutableSegment.Reader reader = new ImmutableSegment.Reader(channel);
    reader.position(0);

    assertEquals(entry2, reader.next());
    assertEquals(entry1, reader.next());
    assertEquals(entry3, reader.next());
    assertNull(reader.next());
  }
}