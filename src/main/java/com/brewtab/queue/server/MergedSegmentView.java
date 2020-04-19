package com.brewtab.queue.server;

import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Stream;

public class MergedSegmentView<E extends Segment> implements Segment {
  // TODO: Optimize for segment staying at head and dequeueing multiple elements from it
  private final PriorityQueue<E> segments =
      new PriorityQueue<>(Comparator.comparing(Segment::peek, entryKeyComparator()));

  private final List<E> retired = new ArrayList<>();

  public void addSegment(E segment) {
    // If peek is non-null then reinsert into the heap
    if (segment.peek() != null) {
      segments.add(segment);
    } else {
      retired.add(segment);
    }
  }

  // O(M)
  @Override
  public long size() {
    return segments.stream()
        .map(Segment::size)
        .reduce(0L, Long::sum);
  }

  @Override
  public Key peek() {
    Segment segment = segments.peek();
    return segment == null ? null : segment.peek();
  }

  @Override
  public Entry next() throws IOException {
    var segment = segments.poll();
    if (segment == null) {
      return null;
    }

    var entry = segment.next();
    addSegment(segment);

    return entry;
  }

  @Override
  public Key first() {
    return Stream.concat(segments.stream(), retired.stream())
        .map(Segment::first)
        .min(entryKeyComparator())
        .orElse(null);
  }

  @Override
  public Key last() {
    return Stream.concat(segments.stream(), retired.stream())
        .map(Segment::last)
        .max(entryKeyComparator())
        .orElse(null);
  }

  @Override
  public long getMaxId() {
    return Stream.concat(segments.stream(), retired.stream())
        .mapToLong(Segment::getMaxId)
        .max()
        .orElse(0);
  }
}
