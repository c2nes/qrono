package net.qrono.server;

import io.netty.util.ReferenceCountUtil;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import net.qrono.server.data.Entry;

/**
 * A segment reader. Allows sequential reads of the entries in a segment.
 */
public interface SegmentReader extends Closeable {
  /**
   * Returns the next entry in the segment without advancing the reader position. Returns {@code
   * null} when the end of the segment has been reached.
   */
  Entry peekEntry() throws IOException;

  /**
   * Returns the key for the next entry in the segment without advancing the reader position.
   * Returns {@code null} when the end of the segment has been reached.
   */
  default Entry.Key peek() {
    Entry entry = null;
    try {
      entry = peekEntry();
      return entry == null ? null : entry.key();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      ReferenceCountUtil.release(entry);
    }
  }

  /**
   * Returns the next entry in the segment. Returns {@code null} when the end of the segment has
   * been reached.
   */
  Entry next() throws IOException;
}
