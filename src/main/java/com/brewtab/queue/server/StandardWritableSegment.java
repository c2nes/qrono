package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.itemKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;
import static com.brewtab.queue.server.SegmentEntryComparators.itemComparator;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Preconditions;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;

public class StandardWritableSegment implements WritableSegment {
  private final String name;
  private final WriteAheadLog wal;

  private final PriorityQueue<Item> pending = new PriorityQueue<>(itemComparator());
  private final SortedSet<Key> tombstones = new TreeSet<>(entryKeyComparator());

  // Pending items are moved here after being returned by next().
  //
  // If a tombstone is added to this segment we first check for a matching entry here.
  // If one exists then it is removed and the tombstone is dropped (i.e. the "removed"
  // pending item and the tombstone cancel one another out). We don't check "entries"
  // because a tombstone should only be produced for items in the "working" state which
  // requires that it was returned by a next() call.
  private final LinkedHashMap<Key, Item> removed = new LinkedHashMap<>();
  private Item lastRemoved = null;

  private boolean closed = false;

  public StandardWritableSegment(String name, WriteAheadLog wal) {
    this.name = name;
    this.wal = wal;
  }

  // TODO: Should we take responsibility for moving the deadline forward as needed?
  private void checkEntry(Entry entry) {
    if (entry.getEntryCase() == EntryCase.PENDING && lastRemoved != null) {
      Item item = entry.getPending();
      Preconditions.checkArgument(itemComparator().compare(lastRemoved, item) <= 0,
          "item deadline too far in the past, %s < %s",
          Timestamps.toString(item.getDeadline()),
          Timestamps.toString(lastRemoved.getDeadline()));
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void add(Entry entry) throws IOException {
    Preconditions.checkState(!closed, "closed");
    checkEntry(entry);
    wal.append(entry);

    switch (entry.getEntryCase()) {
      case PENDING:
        pending.add(entry.getPending());
        break;

      case TOMBSTONE:
        Key tombstone = entry.getTombstone();
        if (removed.remove(tombstone) == null) {
          tombstones.add(tombstone);
        }
        break;

      default:
        throw new IllegalArgumentException("invalid entry case: " + entry.getEntryCase());
    }
  }

  // TODO: This name clashes with "close()" in ImmutableSegment which may be confusing.

  @Override
  public void close() throws IOException {
    Preconditions.checkState(!closed, "already closed");
    closed = true;
    wal.close();
  }

  @Override
  public synchronized Collection<Entry> entries() {
    Preconditions.checkState(closed, "must be closed");
    var entries = new ArrayList<Entry>();
    for (Key tombstone : tombstones) {
      entries.add(Entry.newBuilder().setTombstone(tombstone).build());
    }
    for (Item pending : pending) {
      entries.add(Entry.newBuilder().setPending(pending).build());
    }
    for (Item pending : removed.values()) {
      entries.add(Entry.newBuilder().setPending(pending).build());
    }
    return Collections.unmodifiableList(entries);
  }

  @Override
  public long size() {
    return pending.size() + removed.size() + tombstones.size();
  }

  @Override
  public Key peek() {
    Item item = pending.peek();
    return item == null ? null : itemKey(item);
  }

  @Override
  public Entry next() {
    Item item = pending.poll();
    if (item == null) {
      return null;
    }

    lastRemoved = item;
    removed.put(itemKey(item), item);
    return Entry.newBuilder().setPending(item).build();
  }

  @Override
  public Key first() {
    // TODO: Implement
    throw new UnsupportedOperationException();
  }

  @Override
  public Key last() {
    // TODO: Implement
    throw new UnsupportedOperationException();
  }
}

