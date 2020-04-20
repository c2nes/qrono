package com.brewtab.queue.server;

import static com.brewtab.queue.server.Segment.entryKey;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Verify;
import java.io.IOException;
import java.io.UncheckedIOException;

public class TombstoningSegmentView implements Segment {
  private final Segment delegate;
  private Entry next;

  public TombstoningSegmentView(Segment delegate) {
    this.delegate = delegate;
  }

  @Override
  public long size() {
    return delegate.size();
  }

  @Override
  public Key peek() {
    if (next == null) {
      try {
        next = next(delegate);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return next == null ? null : entryKey(next);
  }

  public Entry next() throws IOException {
    Entry entry = next;
    next = next(delegate);
    return entry;
  }

  @Override
  public Key first() {
    return delegate.first();
  }

  @Override
  public Key last() {
    return delegate.last();
  }

  @Override
  public long getMaxId() {
    return delegate.getMaxId();
  }

  private static Entry next(Segment delegate) throws IOException {
    while (true) {
      var entry = delegate.next();
      if (entry == null || !entry.hasTombstone()) {
        return entry;
      }

      var key = entryKey(entry);
      var nextKey = delegate.peek();

      // Next key is different so this must be an unpaired tombstone
      if (!key.equals(nextKey)) {
        return entry;
      }

      // Retrieve and discard the next entry.
      var nextEntry = Verify.verifyNotNull(delegate.next());

      // Next entry should be the pending item our tombstone is shadowing
      Verify.verify(nextEntry.getEntryCase() == EntryCase.PENDING,
          "duplicate non-tombstone segment entries found");

      // At most two entries (a pending item and it's tombstone) may share a pending key.
      // Validate that the next key (if any) is different
      Verify.verifyNotNull(!entryKey(entry).equals(delegate.peek()),
          "duplicate segment entries found; key=");
    }
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
