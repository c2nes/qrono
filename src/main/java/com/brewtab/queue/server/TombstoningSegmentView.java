package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.EntryCase;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.google.common.base.Verify;
import java.io.IOException;

public class TombstoningSegmentView /* implements Segment */ {

  private final Segment delegate;

  public TombstoningSegmentView(Segment delegate) {
    this.delegate = delegate;
  }

  public Entry next() throws IOException {
    return next(delegate);
  }

  private static Entry next(Segment delegate) throws IOException {
    while (true) {
      var entry = delegate.next();
      if (entry == null) {
        return null;
      }

      var key = entry.getKey();
      var nextKey = delegate.peek();

      // Next key is different so this is either an unpaired tombstone
      // or a pending entry without a corresponding tombstone in this view.
      if (!key.equals(nextKey)) {
        return entry;
      }

      // Retrieve and discard the next entry.
      var nextEntry = Verify.verifyNotNull(delegate.next());

      // One of the two entries must be a tombstone.
      Verify.verify(entry.getEntryCase() == EntryCase.TOMBSTONE
              || nextEntry.getEntryCase() == EntryCase.TOMBSTONE,
          "duplicate non-tombstone segment entries found");

      // At most two entries (a pending item and it's tombstone) may share a pending key.
      // Validate that the next key (if any) is different
      Verify.verifyNotNull(!entry.getKey().equals(delegate.peek()),
          "duplicate segment entries found; key=");
    }
  }
}
