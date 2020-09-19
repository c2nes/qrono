package net.qrono.server;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;

public class ByteArrayChannel implements SeekableByteChannel {
  private byte[] data;
  private int position = 0;

  public ByteArrayChannel() {
    this.data = new byte[0];
  }

  public ByteArrayChannel(byte[] data) {
    this.data = Arrays.copyOf(data, data.length);
  }

  @Override
  public int read(ByteBuffer dst) {
    if (position == data.length) {
      return -1;
    }
    int n = Math.min(data.length - position, dst.remaining());
    dst.put(data, position, n);
    position += n;
    return n;
  }

  @Override
  public int write(ByteBuffer src) {
    int n = src.remaining();
    int nextPosition = position + n;
    if (nextPosition > data.length) {
      data = Arrays.copyOf(data, nextPosition);
    }
    src.get(data, position, n);
    position = nextPosition;
    return n;
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) {
    int intPosition = (int) newPosition;
    if (intPosition != newPosition) {
      throw new IllegalArgumentException("int overflow");
    }
    position = intPosition;
    return this;
  }

  @Override
  public long size() {
    return data.length;
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    int intSize = (int) size;
    if (intSize != size) {
      throw new IllegalArgumentException("int overflow");
    }
    if (intSize < data.length) {
      data = Arrays.copyOf(data, intSize);
    }
    position = Math.min(position, intSize);
    return this;
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void close() {

  }
}
