package net.qrono.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import net.qrono.server.data.Entry;

/**
 * A write ahead log. Writes a linear sequence of entries.
 */
public interface WriteAheadLog extends Closeable {
  void append(Entry entry) throws IOException;

  void append(List<Entry> entries) throws IOException;
}
