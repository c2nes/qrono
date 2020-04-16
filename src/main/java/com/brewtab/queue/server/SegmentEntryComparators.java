package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import java.util.Comparator;

public final class SegmentEntryComparators {
  private static final Comparator<Key> KEY_COMPARATOR =
      Comparator.comparing((Key k) -> k.getDeadline().getSeconds())
          .thenComparing(k -> k.getDeadline().getNanos())
          .thenComparing(Key::getId);

  private static final Comparator<Entry> ENTRY_COMPARATOR =
      Comparator.comparing(Entry::getKey, KEY_COMPARATOR);

  private SegmentEntryComparators() {
  }

  public static Comparator<Key> entryKeyComparator() {
    return KEY_COMPARATOR;
  }

  public static Comparator<Entry> entryComparator() {
    return ENTRY_COMPARATOR;
  }
}
