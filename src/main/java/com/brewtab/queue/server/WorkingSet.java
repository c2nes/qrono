package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry.Key;
import com.brewtab.queue.server.data.Item;
import java.io.IOException;

public interface WorkingSet {
  void add(Item item) throws IOException;

  long size();

  ItemRef get(long id) throws IOException;

  interface ItemRef {
    Key key();

    Item item();

    boolean release();
  }
}
