package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
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

  public StandardWritableSegment(String name, WriteAheadLog wal) {
    this.name = name;
    this.wal = wal;
  }

  // QueueData is responsible for upholding this invariant for the queue overall.
  // This check verifies that it is upheld within the current segment.
  private void checkEntryDeadline(Entry entry) {
    var item = entry.item();
    if (item != null && lastRemoved != null) {
      Preconditions.checkArgument(item.compareTo(lastRemoved) > 0,
          "pending item must not precede previously dequeued entries");
    } else {
      var tombstone = entry.key();
      Preconditions.checkArgument(tombstone.compareTo(peek()) < 0,
          "tombstone key refers pending (not dequeued) item");
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Entry add(Entry entry) throws IOException {
    Preconditions.checkState(!frozen, "frozen");

    checkEntryDeadline(entry);
    wal.append(entry);

    var item = entry.item();
    if (item != null) {
      pending.add(item);
    } else {
      Entry.Key tombstone = entry.key();
      if (removed.remove(tombstone.id()) == null) {
        tombstones.add(tombstone);
      }
    }

    return entry;
  }

  @Override
  public synchronized void freeze() throws IOException {
    Preconditions.checkState(!frozen, "already frozen");
    frozen = true;
    wal.close();
  }

  @Override
  public synchronized Collection<Entry> entries() {
    Preconditions.checkState(frozen, "must be frozen");
    var entries = new ArrayList<Entry>();
    for (Entry.Key tombstone : tombstones) {
      entries.add(ImmutableEntry.builder().key(tombstone).build());
    }

    var pendingAndRemoved = Iterables.concat(pending, removed.values());
    for (Item item : pendingAndRemoved) {
      entries.add(Entry.newPendingEntry(item));
    }

    return Collections.unmodifiableList(entries);
  }

  @Override
  public synchronized SegmentMetadata getMetadata() {
    var allItems = new ArrayList<Item>(pending.size() + removed.size());
    allItems.addAll(pending);
    allItems.addAll(removed.values());

    var allKeys = new ArrayList<Entry.Key>(allItems.size() + tombstones.size());
    allKeys.addAll(tombstones);
    for (Item item : allItems) {
      allKeys.add(Entry.newPendingKey(item));
    }

    if (allKeys.isEmpty()) {
      return null;
    }

    return ImmutableSegmentMetadata.builder()
        .pendingCount(allItems.size())
        .tombstoneCount(tombstones.size())
        .maxId(allItems.stream()
            .mapToLong(Item::id)
            .max()
            .orElse(0))
        .firstKey(Collections.min(allKeys))
        .lastKey(Collections.max(allKeys))
        .build();
  }

  @Override
  public long size() {
    return pending.size() + removed.size() + tombstones.size();
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
