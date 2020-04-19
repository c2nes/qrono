package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import java.io.Closeable;
import java.io.IOException;

/**
 * A write ahead log. Writes a linear sequence of entries.
 */
public interface WriteAheadLog extends Closeable {
  void append(Entry entry) throws IOException;
}
