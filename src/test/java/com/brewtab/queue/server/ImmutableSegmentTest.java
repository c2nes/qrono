package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Stats;
import com.brewtab.queue.server.Encoding.Key;
import com.brewtab.queue.server.Encoding.PendingPreamble;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.Comparator;
import org.junit.Test;

public class ImmutableSegmentTest {
  @Test
  public void test() throws IOException {
    long baseTime = System.currentTimeMillis();
    StandardIdGenerator generator = new StandardIdGenerator(baseTime, 0);

    Stats stats = Stats.newBuilder()
        .setEnqueueTime(Timestamps.fromMillis(baseTime))
        .setRequeueTime(Timestamps.fromMillis(baseTime))
        .build();

    Entry entry1 = Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime))
            .setId(generator.generateId())
            .setStats(stats))
        .build();
    Entry entry2 = Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime - 5))
            .setId(generator.generateId())
            .setStats(stats))
        .build();
    Entry entry3 = Entry.newBuilder()
        .setPending(Item.newBuilder()
            .setDeadline(Timestamps.fromMillis(baseTime + 5))
            .setId(generator.generateId())
            .setStats(stats))
        .build();

    InMemorySegment memSegment = new InMemorySegment(entry1, entry2, entry3);

    // Item overhead
    var itemOverhead = Key.SIZE + PendingPreamble.SIZE;
    var footerSize = Encoding.Footer.SIZE;

    ByteArrayChannel channel = new ByteArrayChannel();
    ImmutableSegment.write(channel, memSegment);
    assertEquals(itemOverhead * 3 + footerSize, channel.position());

    channel.position(0);
    ImmutableSegment reader = ImmutableSegment.newReader(channel);
    assertEquals(3, reader.size());
    assertEquals(entry2, reader.next());
    assertEquals(entry1, reader.next());
    assertEquals(entry3, reader.next());
    assertNull(reader.next());
  }
//
//  @Test
//  public void testMaxFooterSize() {
//    Message maxFooter = buildMaximumMessage(Footer.newBuilder(), Footer.getDescriptor());
//    int maxFooterSize = maxFooter.toByteArray().length;
//    assertTrue(maxFooterSize < ImmutableSegment.FOOTER_SIZE);
//    System.out.printf(
//        "maxFooterSize(%d) < FOOTER_SIZE(%d)\n",
//        maxFooterSize,
//        ImmutableSegment.FOOTER_SIZE);
//  }

  private static Message buildMaximumMessage(Builder builder, Descriptor descriptor) {
    for (FieldDescriptor field : descriptor.getFields()) {
      // TODO: Add descriptive message
      assertFalse(field.isMapField());
      assertFalse(field.isRepeated());

      final Object value;
      switch (field.getJavaType()) {
        case INT:
          value = Integer.MAX_VALUE;
          break;

        case LONG:
          value = Long.MAX_VALUE;
          break;

        case FLOAT:
          value = Float.MAX_VALUE;
          break;

        case DOUBLE:
          value = Double.MAX_VALUE;
          break;

        case BOOLEAN:
          value = true;
          break;

        case ENUM:
          value = field.getEnumType().getValues().stream()
              .max(Comparator.comparing(EnumValueDescriptor::getNumber))
              .map(Object.class::cast)
              .orElse(field.getDefaultValue());
          break;

        case MESSAGE:
          value = buildMaximumMessage(builder.getFieldBuilder(field), field.getMessageType());
          break;

        case STRING:
        case BYTE_STRING:
          throw new AssertionError("found variable length string and bytes fields");

        default:
          throw new AssertionError("unsupported field type (did proto add a new field type?)");
      }

      builder.setField(field, value);
    }

    return builder.build();
  }
}