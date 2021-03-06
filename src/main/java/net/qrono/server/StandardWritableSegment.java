package net.qrono.server;

import static io.netty.util.ReferenceCountUtil.retain;

import com.google.common.base.Preconditions;
import io.netty.util.AbstractReferenceCounted;
import io.netty.util.ReferenceCounted;
import java.io.IOException;
import java.util.List;
import java.util.TreeSet;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Entry.Key;

public class StandardWritableSegment implements WritableSegment {
  // This is a crude approximation of the amount of overhead required to store an entry in memory.
  // It is added to the value size of pending items and is used as the approximate size of
  // tombstones.
  private static final long ENTRY_OVERHEAD_BYTES = 64;

  private final SegmentName name;
  private final WriteAheadLog wal;

  // This reference count acts as an indirect reference to the entries in this segment. When
  // readers for a frozen segment are created we increment this reference rather than incrementing
  // the reference count of each entry individually. When this writable segment and all readers of
  // its frozen segment are closed this reference count will reach zero and we will then release
  // each individual entry.
  private final ReferenceCounted refCnt = new AbstractReferenceCounted() {
    @Override
    protected void deallocate() {
      for (var entry : entries) {
        entry.release();
      }
    }

    @Override
    public ReferenceCounted touch(Object hint) {
      return this;
    }
  };

  //private final TreeMap<Entry.Key, Entry> entries = new TreeMap<>();
  private final TreeSet<Entry> entries = new TreeSet<>();
  private int pendingCount = 0;
  private int tombstoneCount = 0;
  //
  private Entry lastRemoved = null;

  // Estimated total size of this segment in bytes
  private long sizeBytes = 0;

  private boolean frozen = false;
  private boolean writerClosed = false;
  private boolean closed = false;

  public StandardWritableSegment(SegmentName name, WriteAheadLog wal) {
    this.name = name;
    this.wal = wal;
  }

  // QueueData is responsible for upholding this invariant for the queue overall.
  // This check verifies that it is upheld within the current segment.
  private void checkEntryDeadline(Entry entry) {
    if (entry.isPending()) {
      Preconditions.checkArgument(lastRemoved == null || entry.compareTo(lastRemoved) > 0,
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
      entries.add(entry.retain());
      pendingCount += 1;
      sizeBytes += item.value().readableBytes() + ENTRY_OVERHEAD_BYTES;
    } else {
      // Tombstone sorts before pending so we need to look at the ceiling for a potential match.
      var maybeMirror = entries.ceiling(entry);
      if (maybeMirror != null && maybeMirror.mirrors(entry)) {
        // Match found. Remove the pending entry which this tombstone cancels out.
        entries.remove(maybeMirror);
        pendingCount -= 1;
        sizeBytes -= (maybeMirror.item().value().readableBytes() + ENTRY_OVERHEAD_BYTES);
        maybeMirror.release();
      } else {
        // No match found. Simply add the tombstone to the entry set.
        entries.add(entry.retain());
        tombstoneCount += 1;
        sizeBytes += ENTRY_OVERHEAD_BYTES;
        // Tombstone entries will always precede pending (not dequeued) entries. We need to
        // therefore ensure these tombstones entries will be skipped by `next()` by updating
        // `lastRemoved` appropriately.
        if (lastRemoved == null || entry.compareTo(lastRemoved) > 0) {
          lastRemoved = entry;
        }
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
    return new InMemorySegment(name, entries, refCnt);
  }

  @Override
  public synchronized void closeWriterIO() throws IOException {
    Preconditions.checkState(!writerClosed, "writer already closed");
    writerClosed = true;
    wal.close();
  }

  @Override
  public synchronized long pendingCount() {
    return pendingCount;
  }

  @Override
  public synchronized long tombstoneCount() {
    return tombstoneCount;
  }

  @Override
  public synchronized long sizeBytes() {
    return sizeBytes;
  }

  private synchronized Entry peekEntryUnretained() {
    Preconditions.checkState(!closed, "closed");

    if (lastRemoved == null) {
      return entries.isEmpty() ? null : entries.first();
    } else {
      return entries.higher(lastRemoved);
    }
  }

  @Override
  public Entry peekEntry() {
    return retain(peekEntryUnretained());
  }

  @Override
  public Key peek() {
    var entry = peekEntryUnretained();
    return entry == null ? null : entry.key();
  }

  @Override
  public synchronized Entry next() {
    Preconditions.checkState(!closed, "closed");
    var entry = peekEntry();
    if (entry != null) {
      lastRemoved = entry;
    }
    // Already retained by peekEntry
    return entry;
  }

  @Override
  public synchronized void close() throws IOException {
    if (!frozen) {
      freeze();
    }
    if (!writerClosed) {
      closeWriterIO();
    }
    closed = true;

    // Release our reference to the entries. One or more InMemorySegmentReaders may
    // still hold a reference.
    refCnt.release();
  }
}
