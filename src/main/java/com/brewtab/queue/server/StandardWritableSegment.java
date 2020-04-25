package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.itemKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;
import static com.brewtab.queue.server.SegmentEntryComparators.itemComparator;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Metadata;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
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

  private boolean frozen = false;
  private boolean closed = false;

  public StandardWritableSegment(String name, WriteAheadLog wal) {
    this.name = name;
    this.wal = wal;
  }

  private Entry adjustEntryDeadline(Entry entry) {
    if (entry.getEntryCase() == EntryCase.PENDING && lastRemoved != null) {
      Item item = entry.getPending();
      if (itemComparator().compare(item, lastRemoved) < 0) {
        var withAdjustedDeadline = item.toBuilder()
            .setDeadline(lastRemoved.getDeadline())
            .build();

        // If the item still compares less after adjusting the deadline then the item
        // ID must have gone backwards (which should never happen).
        Verify.verify(itemComparator().compare(lastRemoved, withAdjustedDeadline) < 0,
            "Pending item ID went backwards! %s < %s",
            withAdjustedDeadline.getId(),
            lastRemoved.getId());

        return entry.toBuilder().setPending(item).build();
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
  public synchronized Metadata getMetadata() {
    var builder = Metadata.newBuilder();
    builder.setPendingCount(pending.size() + removed.size());
    builder.setTombstoneCount(tombstones.size());
    builder.setMaxId(Stream.concat(pending.stream(), removed.values().stream())
        .mapToLong(Item::getId)
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
  public synchronized Key peek() {
    Preconditions.checkState(!closed, "closed");
    Item item = pending.peek();
    return item == null ? null : itemKey(item);
  }

  @Override
  public synchronized Entry next() {
    Preconditions.checkState(!closed, "closed");
    Item item = pending.poll();
    if (item == null) {
      return null;
    }

    lastRemoved = item;
    removed.put(itemKey(item), item);
    return Entry.newBuilder().setPending(item).build();
  }

  @Override
  public synchronized void close() throws IOException {
    if (!frozen) {
      freeze();
    }
    closed = true;
  }
}
