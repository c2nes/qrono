package net.qrono.server;

import net.qrono.server.data.Entry.Key;
import net.qrono.server.data.Item;
import java.io.IOException;

public interface WorkingSet {
  /**
   * Adds the given item to the working set.
   */
  void add(Item item) throws IOException;

  /**
   * Returns the number of items in the working set.
   */
  long size();

  /**
   * Returns an {@link ItemRef} for the given ID, or null if the ID is not found in the working
   * set.
   */
  ItemRef get(long id) throws IOException;

  /**
   * A reference to a previously {@link #add(Item) added} item.
   */
  interface ItemRef {
    /**
     * Returns a tombstone key for this item.
     *
     * @throws IllegalStateException if this item has been released
     */
    Key key();

    /**
     * Returns the referenced item.
     *
     * @throws IllegalStateException if this item has been released
     */
    Item item();

    /**
     * Releases the item, removing it from the working set.
     *
     * @throws IllegalStateException if this item has already been released
     */
    void release();
  }
}
