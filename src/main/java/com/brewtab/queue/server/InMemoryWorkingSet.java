package com.brewtab.queue.server;

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
  public Item get(long id) {
    return items.get(id);
  }

  @Override
  public Item removeForRequeue(long id) {
    return items.remove(id);
  }
}
