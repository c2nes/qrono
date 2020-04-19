package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

public class ImmutableSegmentTest {
  @Test
  public void test() throws IOException {
    long baseTime = System.currentTimeMillis();
    StandardIdGenerator generator = new StandardIdGenerator(baseTime, 0);

    Entry entry1 = Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime))
            .setId(generator.generateId()))
        .build();
    Entry entry2 = Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime - 5))
            .setId(generator.generateId()))
        .build();
    Entry entry3 = Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime + 5))
            .setId(generator.generateId()))
        .build();

    InMemorySegment memSegment = new InMemorySegment(entry1, entry2, entry3);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImmutableSegment.write(baos, memSegment);
    ImmutableSegment reader = ImmutableSegment.newReader(
        new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(3, reader.size());
    assertEquals(entry2, reader.next());
    assertEquals(entry1, reader.next());
    assertEquals(entry3, reader.next());
    assertNull(reader.next());
  }
}