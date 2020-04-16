package com.brewtab.queue.server;

import com.brewtab.queue.Api.Segment.Entry;
import com.brewtab.queue.Api.Segment.Entry.Key;
import com.brewtab.queue.Api.Segment.Header;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public final class ImmutableSegment {
  private ImmutableSegment() {
  }

  public static class Reader implements Segment, Closeable {
    private final InputStream input;
    private final Header header;
    private final Entry.Key firstKey;

    private Entry.Key nextKey;
    private long remaining;
    private boolean closed = false;

    private Reader(InputStream input, Header header, Key firstKey) {
      this.input = input;
      this.header = header;
      this.firstKey = firstKey;
      this.nextKey = firstKey;
      this.remaining = header.getEntryCount();
    }

    @Override
    public long size() {
      return header.getEntryCount();
    }

    @Override
    public Key peek() {
      Preconditions.checkState(!closed, "closed");
      return nextKey;
    }

    @Override
    public Entry next() throws IOException {
      Preconditions.checkState(!closed, "closed");
      if (nextKey == null) {
        return null;
      }

      Entry entry = Entry.parseDelimitedFrom(input).toBuilder()
          .setKey(nextKey)
          .build();

      if (remaining > 0) {
        nextKey = Entry.Key.parseDelimitedFrom(input);
        remaining--;
      } else {
        nextKey = null;
        input.close();
      }

      return entry;
    }

    @Override
    public Key first() {
      return firstKey;
    }

    @Override
    public Key last() {
      return header.getLastKey();
    }

    @Override
    public void close() throws IOException {
      closed = true;
      nextKey = null;
      input.close();
    }
  }

  public static Reader newReader(InputStream input) throws IOException {
    Header header = Header.parseDelimitedFrom(input);
    Key firstKey = Key.parseDelimitedFrom(input);
    return new Reader(input, header, firstKey);
  }

  public static void write(OutputStream output, Segment segment) throws IOException {
    Header.newBuilder()
        .setEntryCount(segment.size())
        .setLastKey(segment.last())
        .build()
        .writeDelimitedTo(output);

    for (Entry entry = segment.next(); entry != null; entry = segment.next()) {
      entry.getKey().writeDelimitedTo(output);
      entry.toBuilder().clearKey().build().writeDelimitedTo(output);
    }
  }
}
