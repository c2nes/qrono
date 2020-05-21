package com.brewtab.queue.server;

import static java.util.Collections.unmodifiableCollection;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.Entry.Type;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class MergedSegmentReader implements SegmentReader {
  private static final Comparator<SegmentReader> COMPARATOR =
      Comparator.comparing(SegmentReader::peek);

  private final Map<SegmentName, Segment> segments = new HashMap<>();
  private final PriorityQueue<SegmentReader> readers = new PriorityQueue<>(COMPARATOR);
  private final Map<SegmentName, SegmentReader> readersByName = new HashMap<>();
  private final LongAdder headSwitches = new LongAdder();

  private SegmentReader head = null;
  private Entry next = null;
  private boolean closed = false;

  public long getHeadSwitchDebugCount() {
    return headSwitches.sum();
  }

  private void updateHead() throws IOException {
    if (head == null) {
      head = readers.poll();
      headSwitches.increment();
    } else if (head.peek() == null) {
      // Note, head will still be in namedReaders
      head.close();
      head = readers.poll();
      headSwitches.increment();
    } else {
      var next = readers.peek();
      if (next != null && COMPARATOR.compare(next, head) < 0) {
        readers.add(head);
        head = readers.poll();
        headSwitches.increment();
      }
    }
  }

  public synchronized void addSegment(Segment segment, Key position) throws IOException {
    var reader = segment.newReader(position);

    // If peek is non-null then reinsert into the heap
    if (reader.peek() != null) {
      segments.put(segment.name(), segment);
      readers.add(reader);
      readersByName.put(segment.name(), reader);

      if (next != null) {
        // We've buffered an entry in `next`, but we can no longer assume
        // it is in fact the next entry. Push it back into readers by
        // wrapping it in a single entry reader.
        readers.add(new SingleEntryReader(next));
        next = null;
      }

      updateHead();
    } else {
      reader.close();
    }
  }

  public synchronized void replaceSegments(
      Collection<Segment> oldSegments,
      Segment newSegment,
      Key position
  ) throws IOException {
    // Put head back into readers so we don't have to handle it separately
    if (head != null) {
      readers.add(head);
      head = null;
    }

    Set<SegmentName> names = oldSegments.stream()
        .map(Segment::name)
        .collect(Collectors.toSet());

    for (SegmentName name : names) {
      segments.remove(name);
      var reader = readersByName.remove(name);
      if (reader != null) {
        if (readers.remove(reader)) {
          reader.close();
        }
      }
    }

    addSegment(newSegment, position);
  }

  public synchronized Collection<Segment> getSegments() {
    return unmodifiableCollection(segments.values());
  }

  private Entry.Key rawPeek() {
    return head == null ? null : head.peek();
  }

  private Entry rawNext() throws IOException {
    if (head == null) {
      return null;
    }

    var entry = head.next();
    updateHead();
    return entry;
  }

  /**
   * Advances to the next unpaired entry. Upon returning, either {@code next} will be populated or
   * the raw reader will be positioned at an unpaired entry.
   */
  private void advanceToNextUnpairedEntry() throws IOException {
    while (next == null) {
      var key = rawPeek();
      if (key == null || key.entryType() == Type.PENDING) {
        return;
      }

      // Remove tombstone so we can check if the key after it matches.
      next = rawNext();
      assert next != null && key.equals(next.key());

      // Check if next entry matches
      var nextKey = rawPeek();
      if (nextKey != null && matches(key, nextKey)) {
        var nextEntry = rawNext();
        assert nextEntry != null && nextKey.equals(nextEntry.key());
        next = null;
      }
    }
  }

  @Override
  public synchronized Entry.Key peek() {
    Preconditions.checkState(!closed, "closed");

    try {
      advanceToNextUnpairedEntry();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return next != null ? next.key() : rawPeek();
  }

  @Override
  public synchronized Entry next() throws IOException {
    Preconditions.checkState(!closed, "closed");

    advanceToNextUnpairedEntry();
    if (next != null) {
      var copy = next;
      next = null;
      return copy;
    } else {
      return rawNext();
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (SegmentReader segment : readers) {
      segment.close();
    }
    if (head != null) {
      head.close();
    }
    closed = true;
  }

  private static class SingleEntryReader implements SegmentReader {
    private Entry entry;

    public SingleEntryReader(Entry entry) {
      this.entry = entry;
    }

    @Override
    public synchronized Key peek() {
      return entry == null ? null : entry.key();
    }

    @Override
    public synchronized Entry next() {
      var copy = entry;
      entry = null;
      return copy;
    }

    @Override
    public synchronized void close() {
      entry = null;
    }
  }

  /**
   * Same ID & deadline, but opposite types (one is a tombstone, the other is pending).
   */
  static boolean matches(Entry.Key k1, Entry.Key k2) {
    return k1.id() == k2.id()
        && k1.deadline().equals(k2.deadline())
        && k1.entryType() != k2.entryType();
  }
}