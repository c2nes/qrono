package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Pending;
import com.brewtab.queue.server.ImmutableSegment.Reader;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;

public class ImmutableSegmentTest {
  @Test
  public void test() throws IOException {
    long baseTime = System.currentTimeMillis();
    IdGeneratorImpl generator = new IdGeneratorImpl(baseTime);

    Entry entry1 = Entry.newBuilder()
        .setKey(Key.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime))
            .setId(generator.generateId()))
        .setPending(Pending.newBuilder().build())
        .build();
    Entry entry2 = Entry.newBuilder()
        .setKey(Key.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime - 5))
            .setId(generator.generateId()))
        .setPending(Pending.newBuilder().build())
        .build();
    Entry entry3 = Entry.newBuilder()
        .setKey(Key.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime + 5))
            .setId(generator.generateId()))
        .setPending(Pending.newBuilder().build())
        .build();

    InMemorySegment memSegment = new InMemorySegment();
    memSegment.addEntry(entry1);
    memSegment.addEntry(entry2);
    memSegment.addEntry(entry3);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ImmutableSegment.write(baos, memSegment.freeze());
    Reader reader = ImmutableSegment.newReader(new ByteArrayInputStream(baos.toByteArray()));
    assertEquals(entry2, reader.next());
    assertEquals(entry1, reader.next());
    assertEquals(entry3, reader.next());
    assertNull(reader.next());
  }
}