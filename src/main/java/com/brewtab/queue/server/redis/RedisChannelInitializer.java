package com.brewtab.queue.server.redis;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.brewtab.queue.server.QueueService;
import com.brewtab.queue.server.data.ImmutableTimestamp;
import com.brewtab.queue.server.data.Timestamp;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.IntegerRedisMessage;
import io.netty.handler.codec.redis.RedisArrayAggregator;
import io.netty.handler.codec.redis.RedisBulkStringAggregator;
import io.netty.handler.codec.redis.RedisDecoder;
import io.netty.handler.codec.redis.RedisEncoder;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger log = LoggerFactory.getLogger(RedisChannelInitializer.class);

  private final QueueService service;

  public RedisChannelInitializer(QueueService service) {
    this.service = service;
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    ch.pipeline().addLast(
        new RedisEncoder(),
        new RedisDecoder(),
        new RedisBulkStringAggregator(),
        new RedisArrayAggregator(),
        new RequestHandler(),
        new ErrorHandler());
  }

  private class RequestHandler extends SimpleChannelInboundHandler<ArrayRedisMessage> {
    private final ArrayDeque<CompletableFuture<RedisMessage>> responses = new ArrayDeque<>();

    public RequestHandler() {
      super(false);
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
    private CompletableFuture<RedisMessage> handleEnqueue(List<RedisMessage> args) {
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
      var itemFuture = queue.enqueueAsync(ByteString.copyFrom(value.nioBuffer()), deadline);

      return itemFuture.thenApply(item -> new ArrayRedisMessage(List.of(
          new IntegerRedisMessage(item.id()),
          new IntegerRedisMessage(item.deadline().millis()))));
    }

    // DEQUEUE queue
    //   (integer) id
    //   (integer) deadline
    //   (integer) enqueue-time
    //   (integer) requeue-time
    //   (integer) dequeue-count
    //   (bulk) value
    private CompletableFuture<RedisMessage> handleDequeue(List<RedisMessage> args)
        throws Exception {
      if (args.size() != 2) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
      var queue = service.getQueue(queueName);
      if (queue == null) {
        return completedFuture(FullBulkStringRedisMessage.NULL_INSTANCE);
      }

      var item = queue.dequeue();
      if (item == null) {
        return completedFuture(FullBulkStringRedisMessage.NULL_INSTANCE);
      }

      var response = new ArrayRedisMessage(List.of(
          new IntegerRedisMessage(item.id()),
          new IntegerRedisMessage(item.deadline().millis()),
          new IntegerRedisMessage(item.stats().enqueueTime().millis()),
          new IntegerRedisMessage(item.stats().requeueTime().millis()),
          new IntegerRedisMessage(item.stats().dequeueCount()),
          new FullBulkStringRedisMessage(
              Unpooled.wrappedBuffer(item.value().asReadOnlyByteBuffer()))
      ));

      return completedFuture(response);
    }

    // REQUEUE queue id [DEADLINE milliseconds-timestamp]
    //   OK
    private CompletableFuture<RedisMessage> handleRequeue(List<RedisMessage> args)
        throws Exception {
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
        return completedFuture(FullBulkStringRedisMessage.NULL_INSTANCE);
      }

      try {
        deadline = queue.requeue(id, deadline);
      } catch (IllegalStateException e) {
        return completedFuture(new ErrorRedisMessage("ERR item not dequeued"));
      }

      return completedFuture(new IntegerRedisMessage(deadline.millis()));
    }

    // RELEASE queue id
    //   OK
    private CompletableFuture<RedisMessage> handleRelease(List<RedisMessage> args)
        throws Exception {
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
        return completedFuture(FullBulkStringRedisMessage.NULL_INSTANCE);
      }

      try {
        queue.release(id);
      } catch (IllegalStateException e) {
        return completedFuture(new ErrorRedisMessage("ERR item not dequeued"));
      }

      return completedFuture(new SimpleStringRedisMessage("OK"));
    }

    // INFO queue
    //   (integer) pending-count
    //   (integer) dequeued-count
    private CompletableFuture<RedisMessage> handleInfo(List<RedisMessage> args) {
      if (args.size() != 2) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
      var queue = service.getQueue(queueName);
      if (queue == null) {
        return completedFuture(FullBulkStringRedisMessage.NULL_INSTANCE);
      }

      var info = queue.getQueueInfo();
      var response = new ArrayRedisMessage(List.of(
          new IntegerRedisMessage(info.pendingCount()),
          new IntegerRedisMessage(info.dequeuedCount())));

      return completedFuture(response);
    }

    private CompletableFuture<RedisMessage> handlePing(List<RedisMessage> args) {
      switch (args.size()) {
        case 1:
          return completedFuture(new SimpleStringRedisMessage("PONG"));

        case 2:
          return completedFuture(args.get(1));

        default:
          throw RedisRequestException.wrongNumberOfArguments();
      }
    }

    private CompletableFuture<RedisMessage> handleMessage(ArrayRedisMessage msg) throws Exception {
      List<RedisMessage> args = msg.children();

      if (args.size() < 1) {
        throw RedisRequestException.protocolError();
      }

      var cmdName = arg(args, 0)
          .toString(StandardCharsets.US_ASCII)
          .toLowerCase();

      switch (cmdName) {
        case "enqueue":
          return handleEnqueue(args);

        case "dequeue":
          return handleDequeue(args);

        case "requeue":
          return handleRequeue(args);

        case "release":
          return handleRelease(args);

        case "info":
          return handleInfo(args);

        // For compatibility with Redis
        case "ping":
          return handlePing(args);
      }

      throw RedisRequestException.unknownCommand();
    }

    private synchronized void flushCompletedResponses(ChannelHandlerContext ctx) {
      var doFlush = false;

      for (var head = responses.peek();
          head != null && head.isDone();
          head = responses.peekFirst()) {

        try {
          ctx.write(head.join());
          doFlush = true;
        } catch (CompletionException e) {
          try {
            // TODO: This is gross. Maybe we can handle this in the future chain with a function,
            //  <X extends Throwable> handleException(Class<X> cls, Function<? super X, T> fn)
            throwIfInstanceOf(e.getCause(), RedisRequestException.class);
            ctx.fireExceptionCaught(e.getCause());
          } catch (RedisRequestException rre) {
            ctx.write(new ErrorRedisMessage(rre.getMessage()));
            doFlush = true;
          }
        }

        // Remove from dequeue
        responses.removeFirst();
      }
      if (doFlush) {
        ctx.flush();
      }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ArrayRedisMessage msg) throws Exception {
      var release = true;
      try {
        var future = handleMessage(msg);

        synchronized (this) {
          responses.add(future);
        }

        // When complete release message and flush unblocked responses in the pipeline
        future.thenRun(() -> flushCompletedResponses(ctx))
            .whenComplete((result, e) -> ReferenceCountUtil.release(msg));

        // Release will happen on future completion
        release = false;
      } catch (RedisRequestException e) {
        ctx.writeAndFlush(new ErrorRedisMessage(e.getMessage()));
      } finally {
        if (release) {
          ReferenceCountUtil.release(msg);
        }
      }
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

  static class ErrorHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      try {
        ctx.writeAndFlush(new ErrorRedisMessage("ERR unknown command"));
      } finally {
        ReferenceCountUtil.release(msg);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof DecoderException) {
        ctx.writeAndFlush(new ErrorRedisMessage("ERR protocol error"))
            .addListener(ChannelFutureListener.CLOSE);
      } else if (!ctx.channel().isActive()) {
        // TODO: This isn't working
        // Connection closed, log quietly?
        log.debug("Caught exception for closed channel", cause);
        ctx.close();
      } else {
        ctx.fireExceptionCaught(cause);
      }
    }
  }
}
