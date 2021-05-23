package net.qrono.server.redis;

import static io.netty.util.CharsetUtil.US_ASCII;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import java.util.ArrayList;
import java.util.List;
import net.qrono.server.redis.RedisChannelInitializer.RedisRequestException;

public class QronoDecoder extends ByteToMessageDecoder {
  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
    /*

    For Simple Strings the first byte of the reply is "+"
    For Errors the first byte of the reply is "-"
    For Integers the first byte of the reply is ":"
    For Bulk Strings the first byte of the reply is "$"
    For Arrays the first byte of the reply is "*"

      +   Simple String   +[string]\r\n
      -   Error           -[string]\r\n
      :   Integer         :[digits]\r\n
      $   Bulk String     $[len]\r\n[bytes]\r\n   (null = "$-1\r\n")
      *   Array           *[len]\r\n[elements]    (null = "*-1\r\n")

    A client sends the Redis server a RESP Array consisting of just Bulk Strings.
     */
    try {
      in.markReaderIndex();
      if (!canRead(in)) {
        return;
      }
    } finally {
      in.resetReaderIndex();
    }

    // String
    // Error
    // Long
    // ByteBuf
    // Object[]

    out.add(read(in));
  }

  RedisMessage read(ByteBuf in) throws Exception {
    char type = (char) in.readByte();
    switch (type) {
      case '+':
        return new SimpleStringRedisMessage(readString(in));

      case '-':
        return new ErrorRedisMessage(readString(in));

      case ':':
        return readNumber(in);

      case '$':
        return readBulkString(in);

      case '*':
        return readArray(in);

      default:
        throw new RedisRequestException("protocol error");
    }
  }

  String readString(ByteBuf in) throws Exception {
    int n = in.bytesBefore((byte) '\r');
    var s = in.toString(in.readerIndex(), n, US_ASCII);
    in.readerIndex(in.readerIndex() + n + 2);
    return s;
  }

  IntegerRedisMessage readNumber(ByteBuf in) throws Exception {
    int len = in.bytesBefore((byte) '\r');
    long n = 0;
    int sign = 1;
    for (int i = 0; i < len; i++) {
      byte c = in.readByte();
      if (i == 0 && c == '-') {
        sign = -1;
        continue;
      }
      if (c < '0' || c > '9') {
        throw new RedisRequestException("protocol error");
      }
      n = 10 * n + (c - '0');
    }

    if (in.readByte() != '\r' || in.readByte() != '\n') {
      throw new RedisRequestException("protocol error");
    }

    return new IntegerRedisMessage(n * sign);
  }

  FullBulkStringRedisMessage readBulkString(ByteBuf in) throws Exception {
    int n = tryReadLength(in);
    if (n == -1) {
      return null;
    }
    var string = in.readRetainedSlice(n);
    if (in.readByte() != '\r' || in.readByte() != '\n') {
      throw new RedisRequestException("protocol error");
    }
    return new FullBulkStringRedisMessage(string);
  }

  ArrayRedisMessage readArray(ByteBuf in) throws Exception {
    int n = tryReadLength(in);
    if (n == -1) {
      return null;
    }
    List<RedisMessage> children = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      children.add(read(in));
    }
    return new ArrayRedisMessage(children);
  }

  boolean canRead(ByteBuf in) throws Exception {
    if (in.readableBytes() < 3) {
      return false;
    }

    switch (in.readByte()) {
      case '+':
      case '-':
      case ':':
        return canReadLine(in);

      case '$':
        return canReadBulkString(in);

      case '*':
        return canReadArray(in);

      default:
        throw new RedisRequestException("protocol error");
    }
  }

  boolean canReadLine(ByteBuf in) throws Exception {
    int n = in.bytesBefore((byte) '\r');
    if (n < 0) {
      return false;
    }

    if (in.readableBytes() < n + 2) {
      return false;
    }

    in.readerIndex(in.readerIndex() + n);
    if (in.readByte() != '\r' || in.readByte() != '\n') {
      throw new RedisRequestException("protocol error");
    }

    return true;
  }

  boolean canReadBulkString(ByteBuf in) throws Exception {
    int n = tryReadLength(in);
    if (n == -2) {
      return false;
    }
    if (n == -1) {
      return true;
    }
    if (in.readableBytes() < n + 2) {
      return false;
    }
    in.readerIndex(in.readerIndex() + n + 2);
    return true;
  }

  boolean canReadArray(ByteBuf in) throws Exception {
    int n = tryReadLength(in);
    if (n == -2) {
      return false;
    }
    if (n == -1) {
      return true;
    }
    for (int i = 0; i < n; i++) {
      if (!canRead(in)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns the length of the current array or bulk string value. Returns -2 if more data is
   * required.
   */
  int tryReadLength(ByteBuf in) throws Exception {
    int n = in.bytesBefore((byte) '\n');
    if (n < 0) {
      return -2;
    }

    // Need at least one digit and a '\r'
    if (n < 2) {
      throw new RedisRequestException("protocol error");
    }

    int len = 0;
    int sign = 1;
    for (int i = 0; i < n - 1; i++) {
      byte c = in.readByte();
      if (i == 0 && c == '-') {
        sign = -1;
        continue;
      }
      if (c < '0' || c > '9') {
        throw new RedisRequestException("protocol error");
      }
      len = 10 * len + (c - '0');
    }

    len *= sign;

    if (sign == -1 && len != -1) {
      throw new RedisRequestException("protocol error");
    }

    if (in.readByte() != '\r' || in.readByte() != '\n') {
      throw new RedisRequestException("protocol error");
    }

    return len;
  }
}
