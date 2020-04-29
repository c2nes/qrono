package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.pendingItemKey;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.ImmutableEntry;
import com.brewtab.queue.server.data.ImmutableItem;
import com.brewtab.queue.server.data.ImmutableSegmentMetadata;
import com.brewtab.queue.server.data.Item;
import com.brewtab.queue.server.data.SegmentMetadata;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

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
  private final LinkedHashMap<Entry.Key, Item> removed = new LinkedHashMap<>();
  private Item lastRemoved = null;

  private boolean frozen = false;
  private boolean closed = false;

  public StandardWritableSegment(String name, WriteAheadLog wal) {
    this.name = name;
    this.wal = wal;
  }

  private Entry adjustEntryDeadline(Entry entry) {
    var item = entry.item();
    if (item != null && lastRemoved != null) {
      if (item.compareTo(lastRemoved) < 0) {
        var newDeadline = lastRemoved.deadline();
        var newKey = ImmutableEntry.Key.builder()
            .from(entry.key())
            .deadline(newDeadline)
            .build();

        var newItem = ImmutableItem.builder()
            .from(item)
            .deadline(newDeadline)
            .build();

        // If the item still compares less after adjusting the deadline then the item
        // ID must have gone backwards (which should never happen).
        Verify.verify(lastRemoved.compareTo(newItem) < 0,
            "Pending item ID went backwards! %s < %s",
            newDeadline, lastRemoved.deadline());

        return ImmutableEntry.builder()
            .from(entry)
            .key(newKey)
            .item(newItem)
            .build();
      }
    }

    return entry;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Entry add(Entry originalEntry) throws IOException {
    Preconditions.checkState(!frozen, "frozen");

    var entry = adjustEntryDeadline(originalEntry);
    wal.append(entry);

    var item = entry.item();
    if (item != null) {
      pending.add(item);
    } else {
      Entry.Key tombstone = entry.key();
      if (removed.remove(tombstone) == null) {
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
      entries.add(ImmutableEntry.builder()
          .key(pendingItemKey(item))
          .item(item)
          .build());
    }

    return Collections.unmodifiableList(entries);
  }

  @Override
  public synchronized SegmentMetadata getMetadata() {
    var builder = ImmutableSegmentMetadata.builder();
    builder.pendingCount(pending.size() + removed.size());
    builder.tombstoneCount(tombstones.size());
    builder.maxId(Stream.concat(pending.stream(), removed.values().stream())
        .mapToLong(item -> item.id())
        .max()
        .orElse(0));
    // TODO: Set firstKey & lastKey
    // throw new UnsupportedOperationException();
    return builder.build();
  }

  @Override
  public long size() {
    return pending.size() + removed.size() + tombstones.size();
  }

  @Override
  public synchronized Entry.Key peek() {
    Preconditions.checkState(!closed, "closed");
    Item item = pending.peek();
    return item == null ? null : pendingItemKey(item);
  }

  @Override
  public synchronized Entry next() {
    Preconditions.checkState(!closed, "closed");
    Item item = pending.poll();
    if (item == null) {
      return null;
    }

    lastRemoved = item;
    removed.put(pendingItemKey(item), item);
    return ImmutableEntry.builder()
        .key(pendingItemKey(item))
        .item(item).build();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!frozen) {
      freeze();
    }
    closed = true;
  }
}
