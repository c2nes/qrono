package net.qrono.server;

import com.google.common.base.Preconditions;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.qrono.server.data.Entry;
import net.qrono.server.data.Entry.Key;
import net.qrono.server.data.Item;

public class InMemoryWorkingSet implements WorkingSet {
  private final ConcurrentMap<Long, Item> items = new ConcurrentHashMap<>();

  @Override
  public void add(Item item) {
    if (items.putIfAbsent(item.id(), item) != null) {
      throw new IllegalStateException("already added");
    }
  }

  @Override
  public long size() {
    return items.size();
  }

  @Override
  public ItemRef get(long id) {
    var item = items.get(id);
    if (item == null) {
      return null;
    }

    return new ItemRef() {
      @Override
      public Key key() {
        Preconditions.checkState(items.containsKey(item.id()), "released");
        return Entry.newTombstoneKey(item);
      }

      @Override
      public Item item() {
        Preconditions.checkState(items.containsKey(item.id()), "released");
        return item;
      }

      @Override
      public void release() {
        if (items.remove(id) == null) {
          throw new IllegalStateException("already released");
        }
      }
    };
  }
}
