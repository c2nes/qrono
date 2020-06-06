package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.Item;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryWorkingSet implements WorkingSet {
  private final ConcurrentMap<Long, Item> items = new ConcurrentHashMap<>();

  @Override
  public void add(Item item) {
    items.put(item.id(), item);
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
        return Entry.newTombstoneKey(item);
      }

      @Override
      public Item item() {
        return item;
      }

      @Override
      public void release() {
        items.remove(id);
      }
    };
  }
}
