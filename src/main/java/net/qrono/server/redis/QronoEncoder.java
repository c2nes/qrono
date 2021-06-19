/*
 * Copyright 2016 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package net.qrono.server.redis;

import com.google.common.base.Utf8;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.redis.ArrayHeaderRedisMessage;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.BulkStringHeaderRedisMessage;
import io.netty.handler.codec.redis.BulkStringRedisContent;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FixedRedisMessagePool;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.InlineCommandRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.LastBulkStringRedisContent;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.RedisMessagePool;
import io.netty.handler.codec.redis.RedisMessageType;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.internal.ObjectUtil;
import io.netty.util.internal.UnstableApi;
import java.util.List;

/**
 * Encodes {@link RedisMessage} into bytes following
 * <a href="http://redis.io/topics/protocol">RESP (REdis Serialization Protocol)</a>.
 */
@SuppressWarnings("UnstableApiUsage")
@UnstableApi
public class QronoEncoder extends MessageToMessageEncoder<RedisMessage> {

  private final RedisMessagePool messagePool;

  /**
   * Creates a new instance with default {@code messagePool}.
   */
  public QronoEncoder() {
    this(FixedRedisMessagePool.INSTANCE);
  }

  /**
   * Creates a new instance.
   *
   * @param messagePool the predefined message pool.
   */
  public QronoEncoder(RedisMessagePool messagePool) {
    this.messagePool = ObjectUtil.checkNotNull(messagePool, "messagePool");
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, RedisMessage msg, List<Object> out)
      throws Exception {
    try {
      ByteBuf buf = ctx.alloc().ioBuffer();
      writeRedisMessage(ctx.alloc(), msg, buf);
      out.add(buf);
    } catch (CodecException e) {
      throw e;
    } catch (Exception e) {
      throw new CodecException(e);
    }
  }

  private void writeRedisMessage(ByteBufAllocator allocator, RedisMessage msg, ByteBuf out) {
    if (msg instanceof InlineCommandRedisMessage) {
      writeInlineCommandMessage(allocator, (InlineCommandRedisMessage) msg, out);
    } else if (msg instanceof SimpleStringRedisMessage) {
      writeSimpleStringMessage(allocator, (SimpleStringRedisMessage) msg, out);
    } else if (msg instanceof ErrorRedisMessage) {
      writeErrorMessage(allocator, (ErrorRedisMessage) msg, out);
    } else if (msg instanceof IntegerRedisMessage) {
      writeIntegerMessage(allocator, (IntegerRedisMessage) msg, out);
    } else if (msg instanceof FullBulkStringRedisMessage) {
      writeFullBulkStringMessage(allocator, (FullBulkStringRedisMessage) msg, out);
    } else if (msg instanceof BulkStringRedisContent) {
      writeBulkStringContent(allocator, (BulkStringRedisContent) msg, out);
    } else if (msg instanceof BulkStringHeaderRedisMessage) {
      writeBulkStringHeader(allocator, (BulkStringHeaderRedisMessage) msg, out);
    } else if (msg instanceof ArrayHeaderRedisMessage) {
      writeArrayHeader(allocator, (ArrayHeaderRedisMessage) msg, out);
    } else if (msg instanceof ArrayRedisMessage) {
      writeArrayMessage(allocator, (ArrayRedisMessage) msg, out);
    } else {
      throw new CodecException("unknown message type: " + msg);
    }
  }

  private static void writeInlineCommandMessage(ByteBufAllocator allocator,
      InlineCommandRedisMessage msg,
      ByteBuf out) {

    writeString(allocator, RedisMessageType.INLINE_COMMAND, msg.content(), out);
  }

  private static void writeSimpleStringMessage(ByteBufAllocator allocator,
      SimpleStringRedisMessage msg,
      ByteBuf out) {
    writeString(allocator, RedisMessageType.SIMPLE_STRING, msg.content(), out);
  }

  private static void writeErrorMessage(ByteBufAllocator allocator, ErrorRedisMessage msg,
      ByteBuf out) {
    writeString(allocator, RedisMessageType.ERROR, msg.content(), out);
  }

  private static void writeString(ByteBufAllocator allocator, RedisMessageType type, String content,
      ByteBuf buf) {
    buf.ensureWritable(type.length() + ByteBufUtil.utf8MaxBytes(content) +
        RedisConstants.EOL_LENGTH);
    type.writeTo(buf);
    ByteBufUtil.writeUtf8(buf, content);
    buf.writeShort(RedisConstants.EOL_SHORT);
  }

  private void writeIntegerMessage(ByteBufAllocator allocator, IntegerRedisMessage msg,
      ByteBuf buf) {
    buf.ensureWritable(RedisConstants.TYPE_LENGTH + RedisConstants.LONG_MAX_LENGTH +
        RedisConstants.EOL_LENGTH);
    RedisMessageType.INTEGER.writeTo(buf);
    buf.writeBytes(numberToBytes(msg.value()));
    buf.writeShort(RedisConstants.EOL_SHORT);
  }

  private void writeBulkStringHeader(ByteBufAllocator allocator, BulkStringHeaderRedisMessage msg,
      ByteBuf buf) {
    buf.ensureWritable(RedisConstants.TYPE_LENGTH +
        (msg.isNull() ? RedisConstants.NULL_LENGTH :
            RedisConstants.LONG_MAX_LENGTH + RedisConstants.EOL_LENGTH));
    RedisMessageType.BULK_STRING.writeTo(buf);
    if (msg.isNull()) {
      buf.writeShort(RedisConstants.NULL_SHORT);
    } else {
      buf.writeBytes(numberToBytes(msg.bulkStringLength()));
      buf.writeShort(RedisConstants.EOL_SHORT);
    }
  }

  private static void writeBulkStringContent(ByteBufAllocator allocator, BulkStringRedisContent msg,
      ByteBuf buf) {
    buf.writeBytes(msg.content());
    if (msg instanceof LastBulkStringRedisContent) {
      buf.writeShort(RedisConstants.EOL_SHORT);
    }
  }

  private void writeFullBulkStringMessage(ByteBufAllocator allocator,
      FullBulkStringRedisMessage msg,
      ByteBuf buf) {
    if (msg.isNull()) {
      buf.ensureWritable(RedisConstants.TYPE_LENGTH + RedisConstants.NULL_LENGTH +
          RedisConstants.EOL_LENGTH);
      RedisMessageType.BULK_STRING.writeTo(buf);
      buf.writeShort(RedisConstants.NULL_SHORT);
      buf.writeShort(RedisConstants.EOL_SHORT);
    } else {
      buf.ensureWritable(RedisConstants.TYPE_LENGTH + RedisConstants.LONG_MAX_LENGTH +
          RedisConstants.EOL_LENGTH);
      RedisMessageType.BULK_STRING.writeTo(buf);
      buf.writeBytes(numberToBytes(msg.content().readableBytes()));
      buf.writeShort(RedisConstants.EOL_SHORT);
      buf.writeBytes(msg.content());
      buf.writeShort(RedisConstants.EOL_SHORT);
    }
  }

  /**
   * Write array header only without body. Use this if you want to write arrays as streaming.
   */
  private void writeArrayHeader(ByteBufAllocator allocator, ArrayHeaderRedisMessage msg,
      ByteBuf out) {
    writeArrayHeader(allocator, msg.isNull(), msg.length(), out);
  }

  /**
   * Write full constructed array message.
   */
  private void writeArrayMessage(ByteBufAllocator allocator, ArrayRedisMessage msg,
      ByteBuf out) {
    if (msg.isNull()) {
      writeArrayHeader(allocator, msg.isNull(), RedisConstants.NULL_VALUE, out);
    } else {
      writeArrayHeader(allocator, msg.isNull(), msg.children().size(), out);
      for (RedisMessage child : msg.children()) {
        writeRedisMessage(allocator, child, out);
      }
    }
  }

  private void writeArrayHeader(ByteBufAllocator allocator, boolean isNull, long length,
      ByteBuf buf) {
    if (isNull) {
      buf.ensureWritable(RedisConstants.TYPE_LENGTH + RedisConstants.NULL_LENGTH +
          RedisConstants.EOL_LENGTH);
      RedisMessageType.ARRAY_HEADER.writeTo(buf);
      buf.writeShort(RedisConstants.NULL_SHORT);
      buf.writeShort(RedisConstants.EOL_SHORT);
    } else {
      buf.ensureWritable(RedisConstants.TYPE_LENGTH + RedisConstants.LONG_MAX_LENGTH +
          RedisConstants.EOL_LENGTH);
      RedisMessageType.ARRAY_HEADER.writeTo(buf);
      buf.writeBytes(numberToBytes(length));
      buf.writeShort(RedisConstants.EOL_SHORT);
    }
  }

  // ----------------------------------------------------------------------------------


  private int lenRedisMessage(RedisMessage msg) {
    if (msg instanceof InlineCommandRedisMessage) {
      return lenInlineCommandMessage((InlineCommandRedisMessage) msg);
    } else if (msg instanceof SimpleStringRedisMessage) {
      return lenSimpleStringMessage((SimpleStringRedisMessage) msg);
    } else if (msg instanceof ErrorRedisMessage) {
      return lenErrorMessage((ErrorRedisMessage) msg);
    } else if (msg instanceof IntegerRedisMessage) {
      return lenIntegerMessage((IntegerRedisMessage) msg);
    } else if (msg instanceof FullBulkStringRedisMessage) {
      return lenFullBulkStringMessage((FullBulkStringRedisMessage) msg);
    } else if (msg instanceof BulkStringRedisContent) {
      return lenBulkStringContent((BulkStringRedisContent) msg);
    } else if (msg instanceof BulkStringHeaderRedisMessage) {
      return lenBulkStringHeader((BulkStringHeaderRedisMessage) msg);
    } else if (msg instanceof ArrayHeaderRedisMessage) {
      return lenArrayHeader((ArrayHeaderRedisMessage) msg);
    } else if (msg instanceof ArrayRedisMessage) {
      return lenArrayMessage((ArrayRedisMessage) msg);
    } else {
      throw new CodecException("unknown message type: " + msg);
    }
  }

  private static int lenInlineCommandMessage(InlineCommandRedisMessage msg) {
    return lenString(RedisMessageType.INLINE_COMMAND, msg.content());
  }

  private static int lenSimpleStringMessage(SimpleStringRedisMessage msg) {
    return lenString(RedisMessageType.SIMPLE_STRING, msg.content());
  }

  private static int lenErrorMessage(ErrorRedisMessage msg) {
    return lenString(RedisMessageType.ERROR, msg.content());
  }

  private static int lenString(RedisMessageType type, String content) {
    return type.length() + Utf8.encodedLength(content) + RedisConstants.EOL_LENGTH;
  }

  private int lenIntegerMessage(IntegerRedisMessage msg) {
    return RedisConstants.TYPE_LENGTH + RedisCodecUtil.longStrLen(msg.value()) +
        RedisConstants.EOL_LENGTH;
  }

  private int lenBulkStringHeader(BulkStringHeaderRedisMessage msg) {
    return RedisConstants.TYPE_LENGTH +
        (msg.isNull() ? RedisConstants.NULL_LENGTH :
            RedisCodecUtil.longStrLen(msg.bulkStringLength()) + RedisConstants.EOL_LENGTH);
  }

  private static int lenBulkStringContent(BulkStringRedisContent msg) {
    int n = msg.content().readableBytes();
    if (msg instanceof LastBulkStringRedisContent) {
      n += RedisConstants.EOL_LENGTH;
    }
    return n;
  }

  private int lenFullBulkStringMessage(FullBulkStringRedisMessage msg) {
    if (msg.isNull()) {
      return RedisConstants.TYPE_LENGTH + RedisConstants.NULL_LENGTH +
          RedisConstants.EOL_LENGTH;
    } else {
      int n = msg.content().readableBytes();
      return RedisConstants.TYPE_LENGTH
          + RedisCodecUtil.longStrLen(n)
          + RedisConstants.EOL_LENGTH
          + n
          + RedisConstants.EOL_LENGTH;
    }
  }

  /**
   * Write array header only without body. Use this if you want to write arrays as streaming.
   */
  private int lenArrayHeader(ArrayHeaderRedisMessage msg) {
    return lenArrayHeader(msg.isNull(), msg.length());
  }

  /**
   * Write full constructed array message.
   */
  private int lenArrayMessage(ArrayRedisMessage msg) {
    if (msg.isNull()) {
      return lenArrayHeader(msg.isNull(), RedisConstants.NULL_VALUE);
    } else {
      int n = lenArrayHeader(msg.isNull(), msg.children().size());
      for (RedisMessage child : msg.children()) {
        n += lenRedisMessage(child);
      }
      return n;
    }
  }

  private int lenArrayHeader(boolean isNull, long length) {
    if (isNull) {
      return RedisConstants.TYPE_LENGTH + RedisConstants.NULL_LENGTH + RedisConstants.EOL_LENGTH;
    } else {
      return RedisConstants.TYPE_LENGTH + RedisCodecUtil.longStrLen(length)
          + RedisConstants.EOL_LENGTH;
    }
  }

  // ----------------------------------------------------------------------------


  private byte[] numberToBytes(long value) {
    byte[] bytes = messagePool.getByteBufOfInteger(value);
    return bytes != null ? bytes : RedisCodecUtil.longToAsciiBytes(value);
  }
}
