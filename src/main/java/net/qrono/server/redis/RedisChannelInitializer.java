package net.qrono.server.redis;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.google.common.base.Ascii;
import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
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
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import net.qrono.server.Queue;
import net.qrono.server.QueueManager;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Timestamp;
import net.qrono.server.exceptions.QronoException;
import net.qrono.server.exceptions.QueueNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Redis protocol (RESP) interface to the Queue server.
 */
public class RedisChannelInitializer extends ChannelInitializer<SocketChannel> {
  private static final Logger log = LoggerFactory.getLogger(RedisChannelInitializer.class);

  private final QueueManager manager;

  public RedisChannelInitializer(QueueManager manager) {
    this.manager = manager;
  }

  @Override
  protected void initChannel(SocketChannel ch) {
    ch.pipeline().addLast(
        new QronoEncoder(),
        new QronoDecoder(),
        new RequestHandler(),
        new ErrorHandler());
  }

  private class RequestHandler extends SimpleChannelInboundHandler<ArrayRedisMessage> {
    // Response pipeline. Used to ensure responses are returned in the same order as requests are
    // made. As a result, slow requests at the head of the pipeline may block complete responses.
    private final ArrayDeque<CompletableFuture<RedisMessage>> responses = new ArrayDeque<>();

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
      Timestamp deadline;

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
      } else {
        deadline = null;
      }

      var itemFuture = manager.withQueueAsync(queueName,
          queue -> queue.enqueueAsync(value, deadline));

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
      var futureItem = manager.withExistingQueueAsync(queueName, Queue::dequeueAsync);
      return futureItem.thenApply(item -> {
        if (item == null) {
          return FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        return new ArrayRedisMessage(List.of(
            new IntegerRedisMessage(item.id()),
            new IntegerRedisMessage(item.deadline().millis()),
            new IntegerRedisMessage(item.stats().enqueueTime().millis()),
            new IntegerRedisMessage(item.stats().requeueTime().millis()),
            new IntegerRedisMessage(item.stats().dequeueCount()),
            new FullBulkStringRedisMessage(item.value())
        ));
      });
    }

    // PEEK queue
    //   (integer) id
    //   (integer) deadline
    //   (integer) enqueue-time
    //   (integer) requeue-time
    //   (integer) dequeue-count
    //   (bulk) value
    private CompletableFuture<RedisMessage> handlePeek(List<RedisMessage> args) {
      if (args.size() != 2) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
      var futureItem = manager.withExistingQueueAsync(queueName, Queue::peekAsync);
      return futureItem.thenApply(item -> {
        if (item == null) {
          return FullBulkStringRedisMessage.NULL_INSTANCE;
        }

        return new ArrayRedisMessage(List.of(
            new IntegerRedisMessage(item.id()),
            new IntegerRedisMessage(item.deadline().millis()),
            new IntegerRedisMessage(item.stats().enqueueTime().millis()),
            new IntegerRedisMessage(item.stats().requeueTime().millis()),
            new IntegerRedisMessage(item.stats().dequeueCount()),
            new FullBulkStringRedisMessage(item.value())
        ));
      });
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

      Timestamp deadline;
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
      } else {
        deadline = null;
      }

      var future = manager.withExistingQueueAsync(
          queueName, queue -> queue.requeueAsync(id, deadline));

      return future.handle((updatedDeadline, ex) -> {
        if (ex == null) {
          return new IntegerRedisMessage(updatedDeadline.millis());
        }

        if (unwrapCompletionException(ex) instanceof IllegalStateException) {
          return new ErrorRedisMessage("ERR item not dequeued");
        }

        throw propagateCompletionException(ex);
      });
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

      var future = manager.withExistingQueueAsync(queueName, queue -> queue.releaseAsync(id));
      return future.handle((_result, ex) -> {
        if (ex == null) {
          return new SimpleStringRedisMessage("OK");
        }

        if (unwrapCompletionException(ex) instanceof IllegalStateException) {
          return new ErrorRedisMessage("ERR item not dequeued");
        }

        throw propagateCompletionException(ex);
      });
    }

    // STAT queue
    //   (integer) pending-count
    //   (integer) dequeued-count
    private CompletableFuture<RedisMessage> handleStat(List<RedisMessage> args) {
      if (args.size() != 2) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
      var future = manager.withExistingQueueAsync(queueName, Queue::getQueueInfoAsync);
      return future.thenApply(info -> new ArrayRedisMessage(List.of(
          new IntegerRedisMessage(info.pendingCount()),
          new IntegerRedisMessage(info.dequeuedCount()))));
    }

    // DEL(ETE) queue
    //   OK
    private CompletableFuture<RedisMessage> handleDelete(List<RedisMessage> args) {
      if (args.size() != 2) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      var queueName = arg(args, 1).toString(StandardCharsets.US_ASCII);
      return CompletableFuture.supplyAsync(() -> {
        try {
          manager.deleteQueue(queueName);
          return new SimpleStringRedisMessage("OK");
        } catch (IOException e) {
          throw new CompletionException(e);
        }
      });
    }

    private CompletableFuture<RedisMessage> handlePing(List<RedisMessage> args) {
      switch (args.size()) {
        case 1:
          return completedFuture(new SimpleStringRedisMessage("PONG"));

        case 2:
          return completedFuture(ReferenceCountUtil.retain(args.get(1))); // released by Netty

        default:
          throw RedisRequestException.wrongNumberOfArguments();
      }
    }

    private CompletableFuture<RedisMessage> handleConfig(List<RedisMessage> args) {
      if (args.size() < 2) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      if (!Keywords.GET.matches(arg(args, 1))) {
        throw RedisRequestException.unknownSubcommand();
      }

      if (args.size() < 3) {
        throw RedisRequestException.wrongNumberOfArguments();
      }

      var optionName = arg(args, 2).toString(StandardCharsets.US_ASCII).toLowerCase();
      return switch (optionName) {
        case "save" -> completedFuture(new ArrayRedisMessage(List.of(
            new FullBulkStringRedisMessage(copiedBuffer("save", StandardCharsets.US_ASCII)),
            new FullBulkStringRedisMessage(copiedBuffer("", StandardCharsets.US_ASCII))
        )));
        case "appendonly" -> completedFuture(new ArrayRedisMessage(List.of(
            new FullBulkStringRedisMessage(copiedBuffer("appendonly", StandardCharsets.US_ASCII)),
            new FullBulkStringRedisMessage(copiedBuffer("no", StandardCharsets.US_ASCII))
        )));
        default -> completedFuture(ArrayRedisMessage.EMPTY_INSTANCE);
      };
    }

    private CompletableFuture<RedisMessage> handleMessage(ArrayRedisMessage msg) throws Exception {
      List<RedisMessage> args = msg.children();

      if (args.size() < 1) {
        throw RedisRequestException.protocolError();
      }

      var cmdNameBytes = arg(args, 0);
      var cmdName = cmdNameBytes.getCharSequence(
          cmdNameBytes.readerIndex(),
          cmdNameBytes.readableBytes(),
          StandardCharsets.US_ASCII);

      if (Ascii.equalsIgnoreCase(cmdName, "enqueue")) {
        return handleEnqueue(args);
      }

      switch (cmdName.toString().toLowerCase()) {
        case "enqueue":
          return handleEnqueue(args);

        case "dequeue":
          return handleDequeue(args);

        case "requeue":
          return handleRequeue(args);

        case "release":
          return handleRelease(args);

        case "peek":
          return handlePeek(args);

        case "stat":
          return handleStat(args);

        case "del":
        case "delete":
          return handleDelete(args);

        // For compatibility with Redis
        case "ping":
          return handlePing(args);

        // For compatibility with redis-benchmark
        case "config":
          return handleConfig(args);
      }

      throw RedisRequestException.unknownCommand();
    }

    private synchronized void flushCompletedResponses(ChannelHandlerContext ctx) {
      var doFlush = false;

      for (var head = responses.peek();
          head != null && head.isDone();
          head = responses.peek()) {

        try {
          ctx.write(head.join());
          doFlush = true;
        } catch (CompletionException e) {
          if (e.getCause() instanceof RedisRequestException) {
            var rre = (RedisRequestException) e.getCause();
            ctx.write(new ErrorRedisMessage(rre.getMessage()));
            doFlush = true;
          } else if (e.getCause() instanceof QueueNotFoundException) {
            ctx.write(FullBulkStringRedisMessage.NULL_INSTANCE);
            doFlush = true;
          } else if (e.getCause() instanceof QronoException) {
            var qe = (QronoException) e.getCause();
            ctx.write(new ErrorRedisMessage(qe.getMessage()));
            doFlush = true;
          } else {
            log.error("Unhandled server error. Closing connection", e);
            ctx.writeAndFlush(new ErrorRedisMessage("ERR server error"))
                .addListener(ChannelFutureListener.CLOSE);
          }
        }

        // Remove from dequeue
        responses.remove();
      }
      if (doFlush) {
        ctx.flush();
      }
    }

    private CompletableFuture<RedisMessage> handleMessageSafe(ArrayRedisMessage msg) {
      try {
        return handleMessage(msg);
      } catch (RedisRequestException e) {
        return completedFuture(new ErrorRedisMessage(e.getMessage()));
      } catch (Exception e) {
        return failedFuture(e);
      }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ArrayRedisMessage msg) {
      var future = handleMessageSafe(msg);

      synchronized (this) {
        responses.add(future);
      }

      // When complete, flush unblocked responses in the pipeline
      future.whenCompleteAsync((result, ex) -> flushCompletedResponses(ctx), ctx.executor());
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

    public static RedisRequestException unknownSubcommand() {
      return new RedisRequestException("ERR unknown subcommand");
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

  private static Throwable unwrapCompletionException(Throwable ex) {
    if (ex instanceof CompletionException) {
      return ex.getCause();
    }

    return ex;
  }

  private static RuntimeException propagateCompletionException(Throwable ex) {
    if (ex instanceof CompletionException) {
      throw (CompletionException) ex;
    }

    throw new CompletionException(ex);
  }

  enum Keywords {
    ENQUEUE("ENQUEUE"),
    GET("GET");

    private final byte[] name;

    Keywords(String name) {
      Preconditions.checkArgument(name.matches("[A-Za-z]+"));
      this.name = name.toLowerCase().getBytes(StandardCharsets.US_ASCII);
    }

    boolean matches(ByteBuf arg) {
      if (arg.readableBytes() != name.length) {
        return false;
      }
      final int base = arg.readerIndex();
      for (int i = 0; i < name.length; i++) {
        var b0 = name[i];
        var b1 = arg.getByte(base + i);

        // https://en.wikipedia.org/wiki/ASCII#Printable_characters
        // 'A' = 100 0001
        // 'a' = 110 0001
        if (b0 != (b1 | 0b010_0000)) {
          return false;
        }
      }
      return true;
    }
  }
}
