package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.Item;
import java.io.IOException;

public interface WorkingSet {
  void add(Item item) throws IOException;

  long size();

  Item get(long id) throws IOException;

  Item removeForRequeue(long id) throws IOException;

  default Key removeForRelease(long id) throws IOException {
    var item = removeForRequeue(id);
    return item == null ? null : Entry.newTombstoneKey(item);
  }
}
