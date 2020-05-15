package com.brewtab.queue.server.redis;

import com.brewtab.queue.server.QueueService;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Timestamp;
import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Sharable
public class RedisRequestHandler extends SimpleChannelInboundHandler<ArrayRedisMessage> {
  private final QueueService service;

  public RedisRequestHandler(QueueService service) {
    this.service = service;
  }

  //
  // https://redis.io/topics/protocol#sending-commands-to-a-redis-server
  //
  //   "A client sends the Redis server a RESP Array consisting of just Bulk Strings."
  //

  private ByteBuf arg(List<RedisMessage> args, int idx) {
    var arg = args.get(idx);
    if (arg instanceof FullBulkStringRedisMessage) {
      return ((FullBulkStringRedisMessage) arg).content();
    }

    throw RedisRequestException.protocolError();
  }

  // ENQUEUE queue value [DEADLINE milliseconds-timestamp]
  //   (integer) id
  //   (integer) deadline
  private void handleEnqueue(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    if (args.size() != 3 && args.size() != 5) {
      throw RedisRequestException.wrongNumberOfArguments();
    }

    // TODO: What characters are allowed in a queue name?
    var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
    var value = arg(args, 2);
    Timestamp deadline = null;

    if (args.size() == 5) {
      var deadlineKeyword = arg(args, 3)
          .toString(StandardCharsets.US_ASCII)
          .toLowerCase();
      if (!deadlineKeyword.equals("deadline")) {
        throw RedisRequestException.syntaxError();
      }

      var deadlineMillisString = arg(args, 4)
          .toString(StandardCharsets.US_ASCII);

      try {
        deadline = ImmutableTimestamp.of(Long.parseLong(deadlineMillisString));
      } catch (NumberFormatException e) {
        throw RedisRequestException.syntaxError();
      }
    }

    var queue = service.getOrCreateQueue(queueName);
    var item = queue.enqueue(ByteString.copyFrom(value.nioBuffer()), deadline);
    var response = new ArrayRedisMessage(List.of(
        new IntegerRedisMessage(item.id()),
        new IntegerRedisMessage(item.deadline().millis())));

    ctx.writeAndFlush(response);
  }

  // DEQUEUE queue
  //   (integer) id
  //   (integer) deadline
  //   (integer) enqueue-time
  //   (integer) requeue-time
  //   (integer) dequeue-count
  //   (bulk) value
  private void handleDequeue(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    if (args.size() != 2) {
      throw RedisRequestException.wrongNumberOfArguments();
    }

    var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
    var queue = service.getQueue(queueName);
    if (queue == null) {
      ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
      return;
    }

    var item = queue.dequeue();
    if (item == null) {
      ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
      return;
    }

    // TODO: Remove gRPC from Queue
    var response = new ArrayRedisMessage(List.of(
        new IntegerRedisMessage(item.id()),
        new IntegerRedisMessage(item.deadline().millis()),
        new IntegerRedisMessage(item.stats().enqueueTime().millis()),
        new IntegerRedisMessage(item.stats().requeueTime().millis()),
        new IntegerRedisMessage(item.stats().dequeueCount()),
        new FullBulkStringRedisMessage(
            Unpooled.wrappedBuffer(item.value().asReadOnlyByteBuffer()))
    ));

    ctx.writeAndFlush(response);
  }

  // REQUEUE queue id [DEADLINE milliseconds-timestamp]
  //   OK
  private void handleRequeue(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    if (args.size() != 3 && args.size() != 5) {
      throw RedisRequestException.wrongNumberOfArguments();
    }

    var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
    var idString = arg(args, 2).toString(StandardCharsets.US_ASCII);
    long id;
    try {
      id = Long.parseLong(idString);
    } catch (NumberFormatException e) {
      throw RedisRequestException.syntaxError();
    }

    Timestamp deadline = null;
    if (args.size() == 5) {
      var deadlineKeyword = arg(args, 3)
          .toString(StandardCharsets.US_ASCII)
          .toLowerCase();
      if (!deadlineKeyword.equals("deadline")) {
        throw RedisRequestException.syntaxError();
      }

      var deadlineMillisString = arg(args, 4)
          .toString(StandardCharsets.US_ASCII);

      try {
        deadline = ImmutableTimestamp.of(Long.parseLong(deadlineMillisString));
      } catch (NumberFormatException e) {
        throw RedisRequestException.syntaxError();
      }
    }

    var queue = service.getQueue(queueName);
    if (queue == null) {
      ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
      return;
    }

    deadline = queue.requeue(id, deadline);
    ctx.writeAndFlush(new IntegerRedisMessage(deadline.millis()));
  }

  // RELEASE queue id
  //   OK
  private void handleRelease(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    if (args.size() != 3) {
      throw RedisRequestException.wrongNumberOfArguments();
    }

    var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
    var idString = arg(args, 2).toString(StandardCharsets.US_ASCII);
    long id;
    try {
      id = Long.parseLong(idString);
    } catch (NumberFormatException e) {
      throw RedisRequestException.syntaxError();
    }

    var queue = service.getQueue(queueName);
    if (queue == null) {
      ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
      return;
    }

    queue.release(id);

    ctx.writeAndFlush(new SimpleStringRedisMessage("OK"));
  }

  // INFO queue
  //   (integer) pending-count
  //   (integer) dequeued-count
  private void handleInfo(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    if (args.size() != 2) {
      throw RedisRequestException.wrongNumberOfArguments();
    }

    var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
    var queue = service.getQueue(queueName);
    if (queue == null) {
      ctx.writeAndFlush(FullBulkStringRedisMessage.NULL_INSTANCE);
      return;
    }

    var info = queue.getQueueInfo();
    var response = new ArrayRedisMessage(List.of(
        new IntegerRedisMessage(info.pendingCount()),
        new IntegerRedisMessage(info.dequeuedCount())));

    ctx.writeAndFlush(response);
  }

  private void handlePing(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    switch (args.size()) {
      case 1:
        ctx.writeAndFlush(new SimpleStringRedisMessage("PONG"));
        break;

      case 2:
        ctx.writeAndFlush(args.get(1));
        break;

      default:
        throw RedisRequestException.wrongNumberOfArguments();
    }
  }

  private void handleMessage(ChannelHandlerContext ctx, List<RedisMessage> args) throws Exception {
    if (args.size() < 1) {
      throw RedisRequestException.protocolError();
    }

    var cmdName = arg(args, 0)
        .toString(StandardCharsets.US_ASCII)
        .toLowerCase();

    switch (cmdName) {
      case "enqueue":
        handleEnqueue(ctx, args);
        break;

      case "dequeue":
        handleDequeue(ctx, args);
        break;

      case "requeue":
        handleRequeue(ctx, args);
        break;

      case "release":
        handleRelease(ctx, args);
        break;

      case "info":
        handleInfo(ctx, args);
        break;

      // For compatibility with Redis
      case "ping":
        handlePing(ctx, args);
        break;

      default:
        throw RedisRequestException.unknownCommand();
    }
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ArrayRedisMessage msg) throws Exception {
    try {
      handleMessage(ctx, msg.children());
    } catch (RedisRequestException e) {
      ctx.writeAndFlush(new ErrorRedisMessage(e.getMessage()));
    } catch (StatusRuntimeException e) {
      // TODO: Should we StatusRuntimeException outside gRPC?
      ctx.writeAndFlush(new ErrorRedisMessage(e.getMessage()));
    }
  }

  static class RedisRequestException extends RuntimeException {
    public RedisRequestException(String message) {
      super(message);
    }

    static RedisRequestException protocolError() {
      return new RedisRequestException("ERR Protocol error");
    }

    static RedisRequestException unknownCommand() {
      return new RedisRequestException("ERR unknown command");
    }

    public static RedisRequestException wrongNumberOfArguments() {
      return new RedisRequestException("ERR wrong number of arguments");
    }

    public static RedisRequestException syntaxError() {
      return new RedisRequestException("ERR syntax error");
    }
  }
}
