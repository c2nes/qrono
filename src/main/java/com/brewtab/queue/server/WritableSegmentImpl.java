package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.itemKey;
import static com.brewtab.queue.server.SegmentEntryComparators.entryComparator;
import static com.brewtab.queue.server.SegmentEntryComparators.entryKeyComparator;
import static com.brewtab.queue.server.SegmentEntryComparators.itemComparator;
import static com.google.common.collect.ImmutableSortedSet.toImmutableSortedSet;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

import com.brewtab.queue.Api.Item;
import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Stream;

// TODO: Are there other kinds of WritableSegments?
public class WritableSegmentImpl implements WritableSegment {
  private final Path directory;
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
  private Segment readerView = new MemoryReaderView();

  public WritableSegmentImpl(Path directory, String name) throws IOException {
    this.directory = directory;
    this.name = name;
    // TODO: How do we inject WAL configuration options (e.g. sync duration).
    // Maybe factor out a WAL factory?
    // Maybe add methods to the WAL interface to handle renames & removal?
    wal = new WriteAheadLog(directory.resolve(name + ".log"));
  }

  // TODO: Should we take responsibility for moving the deadline forward as needed?
  private void checkEntry(Entry entry) {
    if (entry.getEntryCase() == EntryCase.PENDING && lastRemoved != null) {
      Item item = entry.getPending();
      Preconditions.checkArgument(itemComparator().compare(item, lastRemoved) < 0,
          "item deadline too far in the past, %s", item.getDeadline());
    }
  }

  @Override
  public void add(Entry entry) throws IOException {
    Preconditions.checkState(!closed, "closed");
    checkEntry(entry);
    wal.write(entry);

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

  private Path getClosedLogPath() {
    return directory.resolve(name + ".log_closed");
  }

  // TODO: This name clashes with "close()" in ImmutableSegment which may be confusing.
  @Override
  public void close() throws IOException {
    Preconditions.checkState(!closed, "already closed");
    closed = true;
    wal.close();
    Files.move(wal.getPath(), getClosedLogPath(), ATOMIC_MOVE);
  }

  @Override
  public Segment freeze() throws IOException {
    Preconditions.checkState(closed, "must be closed first");
    // TODO: Disallow multiple freezes

    Segment tombstoneSegment;
    Segment pendingSegment;

    synchronized (this) {
      tombstoneSegment = new InMemorySegment(
          tombstones.stream()
              .map(k -> Entry.newBuilder().setTombstone(k).build())
              .collect(toImmutableSortedSet(entryComparator())));

      pendingSegment = new InMemorySegment(
          Stream.concat(pending.stream(), removed.values().stream())
              .map(p -> Entry.newBuilder().setPending(p).build())
              .collect(toImmutableSortedSet(entryComparator())));
    }

    if (tombstoneSegment.size() > 0) {
      var tombstoneIdxPath = directory.resolve(name + "-t.idx");
      ImmutableSegment.write(tombstoneIdxPath, tombstoneSegment);
    }

    var pendingIdxPath = directory.resolve(name + "-p.idx");

    final Map<Key, Long> offsets;
    if (pendingSegment.size() > 0) {
      offsets = ImmutableSegment.writeWithOffsetTracking(pendingIdxPath, pendingSegment);
    } else {
      offsets = Collections.emptyMap();
    }

    // Delete log with segment persisted
    Files.delete(getClosedLogPath());

    // Switch reads to the file atomically
    synchronized (this) {
      Item next = pending.peek();
      if (next == null) {
        readerView = new EmptySegment();
      } else {
        Long offset = offsets.get(itemKey(next));
        Verify.verifyNotNull(offset, "missing offset");
        FileChannel input = FileChannel.open(pendingIdxPath);
        readerView = ImmutableSegment.newReader(input, offset);
      }
    }

    // Allow entries to be GC'ed
    pending.clear();
    removed.clear();
    tombstones.clear();

    return readerView;
  }

  @Override
  public synchronized long size() {
    return readerView.size();
  }

  @Override
  public synchronized Key peek() {
    return readerView.peek();
  }

  @Override
  public synchronized Entry next() throws IOException {
    return readerView.next();
  }

  @Override
  public synchronized Key first() {
    return readerView.first();
  }

  @Override
  public synchronized Key last() {
    return readerView.last();
  }

  private class MemoryReaderView implements Segment {
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
      throw new UnsupportedOperationException();
    }

    @Override
    public Key last() {
      throw new UnsupportedOperationException();
    }
  }
}

