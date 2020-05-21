package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A write ahead log. Writes a linear sequence of entries.
 */
public interface WriteAheadLog extends Closeable {
  void append(Entry entry) throws IOException;

  void append(List<Entry> entries) throws IOException;
}
