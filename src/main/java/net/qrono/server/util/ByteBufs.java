package net.qrono.server.util;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;

public final class ByteBufs {
  private ByteBufs() {
  }

  public static void writeBytes(ByteBuf dst, ByteBuffer src, int length) {
    var position = src.position();
    var limit = src.limit();
    src.limit(position + length);
    try {
      dst.writeBytes(src);
    } finally {
      src.limit(limit);
    }
  }

  public static void getBytes(ByteBuf src, ByteBuffer dst, int length) {
    var position = dst.position();
    var limit = dst.limit();
    dst.limit(position + length);
    try {
      src.getBytes(src.readerIndex(), dst);
    } finally {
      dst.limit(limit);
    }
  }
}
