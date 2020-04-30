package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Stream;

public class MergedSegmentView<E extends Segment> implements Segment {
  // TODO: This comparator does not order between tombstone and pending
  //  Should we update the Segment interface so peek returns a new "Entry.Key"?
  private static final Comparator<Segment> COMPARATOR = Comparator.comparing(Segment::peek);

  private final PriorityQueue<E> segments = new PriorityQueue<>(COMPARATOR);
  private final List<E> retired = new ArrayList<>();
  private final LongAdder headSwitches = new LongAdder();

  private E head = null;
  private boolean closed = false;

  private void updateHead() {
    if (head == null) {
      head = segments.poll();
      headSwitches.increment();
    } else if (head.peek() == null) {
      retired.add(head);
      head = segments.poll();
      headSwitches.increment();
    } else {
      var next = segments.peek();
      if (next != null && COMPARATOR.compare(next, head) < 0) {
        segments.add(head);
        head = segments.poll();
        headSwitches.increment();
      }
    }
  }

  public long getHeadSwitchDebugCount() {
    return headSwitches.sum();
  }

  public synchronized void addSegment(E segment) {
    // If peek is non-null then reinsert into the heap
    if (segment.peek() != null) {
      segments.add(segment);
      updateHead();
    } else {
      retired.add(segment);
    }
  }

  public synchronized Collection<E> getSegments() {
    var segments = new ArrayList<E>(this.segments);
    segments.addAll(retired);
    if (head != null) {
      segments.add(head);
    }
    return segments;
  }

  @Override
  public synchronized SegmentMetadata getMetadata() {
    // TODO: This doesn't cancel out tombstones and pending entries
    var maybeHead = Optional.ofNullable(head);
    return Stream.of(segments.stream(), retired.stream(), maybeHead.stream())
        .flatMap(Function.identity())
        .map(Segment::getMetadata)
        .collect(SegmentMetadata.merge());
  }

  @Override
  public synchronized Entry.Key peek() {
    Preconditions.checkState(!closed, "closed");
    return head == null ? null : head.peek();
  }

  @Override
  public synchronized Entry next() throws IOException {
    Preconditions.checkState(!closed, "closed");
    if (head == null) {
      return null;
    }

    var entry = head.next();
    updateHead();

    return entry;
  }

  @Override
  public synchronized void close() throws IOException {
    for (E segment : segments) {
      segment.close();
    }
    for (E segment : retired) {
      segment.close();
    }
    if (head != null) {
      head.close();
    }
    closed = true;
  }
}
