package net.qrono.server;

import com.google.common.collect.ImmutableSortedSet;
import io.netty.util.ReferenceCounted;
import java.util.NavigableSet;
import javax.annotation.Nullable;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Entry.Key;
import net.qrono.server.data.ImmutableSegmentMetadata;
import net.qrono.server.data.Item;
import net.qrono.server.data.SegmentMetadata;

public class InMemorySegment implements Segment {
  private final SegmentName name;
  private final NavigableSet<Entry> entries;
  private final ReferenceCounted owner;

  public InMemorySegment(SegmentName name, Iterable<Entry> entries) {
    this(name, ImmutableSortedSet.copyOf(entries));
  }

  public InMemorySegment(SegmentName name, NavigableSet<Entry> entries) {
    this(name, entries, null);
  }

  public InMemorySegment(SegmentName name, NavigableSet<Entry> entries, ReferenceCounted owner) {
    this.name = name;
    this.entries = entries;
    this.owner = owner;
  }

  @Override
  public SegmentName name() {
    return name;
  }

  @Override
  public SegmentMetadata metadata() {
    var pendingCount = 0;
    var maxId = 0L;
    for (var e : entries) {
      if (e.isPending()) {
        pendingCount++;
      }
      var id = e.key().id();
      if (id > maxId) {
        maxId = id;
      }
    }

    var tombstoneCount = entries.size() - pendingCount;
    return ImmutableSegmentMetadata.builder()
        .pendingCount(pendingCount)
        .tombstoneCount(tombstoneCount)
        .maxId(maxId)
        .build();
  }

  @Override
  public SegmentReader newReader(Key position) {
    return new InMemorySegmentReader(
        entries.tailSet(new KeyOnlyEntry(position), false),
        owner == null ? null : owner.retain());
  }

  /**
   * Entry with only a key. Instances are not valid entries! This class exists so bare keys can be
   * wrapped and used to obtain a tail set in {@link #newReader(Key)}.
   */
  private static class KeyOnlyEntry implements Entry {
    private final Key key;

    private KeyOnlyEntry(Key key) {
      this.key = key;
    }

    @Override
    public Key key() {
      return key;
    }

    @Nullable
    @Override
    public Item item() {
      throw new UnsupportedOperationException();
    }
  }
}
