package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.SegmentMetadata;
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
  public SegmentMetadata getMetadata() {
    // TODO: This lies
    return delegate.getMetadata();
  }

  @Override
  public Entry.Key peek() {
    if (next == null) {
      try {
        next = next(delegate);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return next == null ? null : next.key();
  }

  public Entry next() throws IOException {
    if (next == null) {
      next = next(delegate);
    }

    Entry entry = next;
    next = next(delegate);
    return entry;
  }

  private static Entry next(Segment delegate) throws IOException {
    while (true) {
      var entry = delegate.next();
      if (entry == null || entry.isPending()) {
        return entry;
      }

      var key = entry.key();
      var nextKey = delegate.peek();

      // Next key is different so this must be an unpaired tombstone
      if (!matches(key, nextKey)) {
        return entry;
      }

      // Retrieve and discard the next entry.
      var nextEntry = Verify.verifyNotNull(delegate.next());

      // Next entry should be the pending item our tombstone is shadowing
      Verify.verify(nextEntry.isPending(),
          "duplicate non-tombstone segment entries found");

      // At most two entries (a pending item and it's tombstone) may share a deadline & id
      // Validate that the next key (if any) is different
      var nextNextKey = delegate.peek();
      Verify.verify(nextNextKey == null || !matches(key, nextNextKey),
          "duplicate segment entries found; key=%s", key);
    }
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  static boolean matches(Entry.Key k1, Entry.Key k2) {
    return k1.id() == k2.id()
        && k1.deadline().equals(k2.deadline());
  }
}
