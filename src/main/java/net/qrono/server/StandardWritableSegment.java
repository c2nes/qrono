package net.qrono.server;

import static com.google.common.collect.Iterables.mergeSorted;
import static java.util.Comparator.naturalOrder;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Ordering;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Item;

public class StandardWritableSegment implements WritableSegment {
  // This is a crude approximation of the amount of overhead required to store an entry in memory.
  // It is added to the value size of pending items and is used as the approximate size of
  // tombstones.
  private static final long ENTRY_OVERHEAD_BYTES = 64;

  private final SegmentName name;
  private final WriteAheadLog wal;

  //private final PriorityQueue<Entry> pending = new PriorityQueue<>();
  //private final TreeMap<Entry.Key, Entry> pending = new TreeMap<>();

  private final TreeSet<Entry> pending = new TreeSet<>();
  private final SortedSet<Entry> tombstones = new TreeSet<>();

  // Pending items are moved here after being returned by next().
  //
  // If a tombstone is added to this segment we first check for a matching entry here.
  // If one exists then it is removed and the tombstone is dropped (i.e. the "removed"
  // pending item and the tombstone cancel one another out). We don't check "entries"
  // because a tombstone should only be produced for items in the "working" state which
  // requires that it was returned by a next() call.
  private final LinkedHashMap<Long, Entry> removed = new LinkedHashMap<>();
  private Item lastRemoved = null;

  // Estimated total size of this segment in bytes
  private long sizeBytes = 0;

  private boolean frozen = false;
  private boolean closed = false;

  public StandardWritableSegment(SegmentName name, WriteAheadLog wal) {
    this.name = name;
    this.wal = wal;
  }

  // QueueData is responsible for upholding this invariant for the queue overall.
  // This check verifies that it is upheld within the current segment.
  private void checkEntryDeadline(Entry entry) {
    var item = entry.item();
    if (item != null) {
      Preconditions.checkArgument(lastRemoved == null || item.compareTo(lastRemoved) > 0,
          "pending item must not precede previously dequeued entries");
    } else {
      var tombstone = entry.key();
      var head = peek();
      Preconditions.checkArgument(head == null || tombstone.compareTo(head) < 0,
          "tombstone key refers to pending (not dequeued) item");
    }
  }

  @Override
  public SegmentName name() {
    return name;
  }

  private void addToInMemoryState(Entry entry) {
    var item = entry.item();
    if (item != null) {
      pending.add(entry);
      //
      sizeBytes += item.value().size() + ENTRY_OVERHEAD_BYTES;
    } else {
      Entry.Key tombstone = entry.key();
      Entry removedEntry = removed.remove(tombstone.id());
      if (removedEntry == null) {
        tombstones.add(entry);
        sizeBytes += ENTRY_OVERHEAD_BYTES;
      } else {
        // The tombstone Entry canceled out a removed Item
        sizeBytes -= (removedEntry.item().value().size() + ENTRY_OVERHEAD_BYTES);
      }
    }
  }

  @Override
  public void add(Entry entry) throws IOException {
    Preconditions.checkState(!frozen, "frozen");
    checkEntryDeadline(entry);
    wal.append(entry);
    addToInMemoryState(entry);
  }

  @Override
  public void addAll(List<Entry> entries) throws IOException {
    Preconditions.checkState(!frozen, "frozen");
    entries.forEach(this::checkEntryDeadline);
    wal.append(entries);
    entries.forEach(this::addToInMemoryState);
  }

  @Override
  public synchronized Segment freeze() throws IOException {
    Preconditions.checkState(!frozen, "already frozen");
    frozen = true;
    wal.close();

    var entries = new ArrayList<Entry>(tombstones.size() + pending.size() + removed.size());
    for (var entry : mergeSorted(List.of(tombstones, pending, removed.values()), naturalOrder())) {
      entries.add(entry);
    }

    if (!Ordering.natural().isOrdered(entries)) {
      System.out.println("Not ordered!!!!");
    }

    var sw = Stopwatch.createStarted();
    try {
      return new InMemorySegment(name, entries);
    } finally {
      System.out.println("InMemorySegment(): " + sw);
    }
  }

  @Override
  public synchronized long pendingCount() {
    return pending.size() + removed.size();
  }

  @Override
  public synchronized long tombstoneCount() {
    return tombstones.size();
  }

  @Override
  public synchronized long sizeBytes() {
    return sizeBytes;
  }

  @Override
  public synchronized Entry peekEntry() {
    Preconditions.checkState(!closed, "closed");
    return pending.isEmpty() ? null : pending.first();
  }

  @Override
  public synchronized Entry next() {
    Preconditions.checkState(!closed, "closed");
    Entry entry = pending.pollFirst();
    if (entry == null) {
      return null;
    }

    lastRemoved = entry.item();
    removed.put(entry.key().id(), entry);
    return entry;
  }

  @Override
  public synchronized void close() throws IOException {
    if (!frozen) {
      freeze();
    }
    closed = true;
  }
}
