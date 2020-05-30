package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.Item;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;

public class StandardWritableSegment implements WritableSegment {
  private final SegmentName name;
  private final WriteAheadLog wal;

  private final PriorityQueue<Item> pending = new PriorityQueue<>();
  private final SortedSet<Entry.Key> tombstones = new TreeSet<>();

  // Pending items are moved here after being returned by next().
  //
  // If a tombstone is added to this segment we first check for a matching entry here.
  // If one exists then it is removed and the tombstone is dropped (i.e. the "removed"
  // pending item and the tombstone cancel one another out). We don't check "entries"
  // because a tombstone should only be produced for items in the "working" state which
  // requires that it was returned by a next() call.
  private final LinkedHashMap<Long, Item> removed = new LinkedHashMap<>();
  private Item lastRemoved = null;

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
      pending.add(item);
    } else {
      Entry.Key tombstone = entry.key();
      if (removed.remove(tombstone.id()) == null) {
        tombstones.add(tombstone);
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
    for (Entry.Key tombstone : tombstones) {
      entries.add(ImmutableEntry.builder().key(tombstone).build());
    }

    for (Item item : pending) {
      entries.add(Entry.newPendingEntry(item));
    }

    for (Item item : removed.values()) {
      entries.add(Entry.newPendingEntry(item));
    }

    return new InMemorySegment(name, entries);
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
  public synchronized long size() {
    return pendingCount() + tombstoneCount();
  }

  @Override
  public synchronized Entry.Key peek() {
    Preconditions.checkState(!closed, "closed");
    Item item = pending.peek();
    return item == null ? null : Entry.newPendingKey(item);
  }

  @Override
  public synchronized Entry next() {
    Preconditions.checkState(!closed, "closed");
    Item item = pending.poll();
    if (item == null) {
      return null;
    }

    lastRemoved = item;
    removed.put(item.id(), item);
    return Entry.newPendingEntry(item);
  }

  @Override
  public synchronized void close() throws IOException {
    if (!frozen) {
      freeze();
    }
    closed = true;
  }
}
