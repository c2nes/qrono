package net.qrono.server;

import static java.lang.Math.toIntExact;

import com.google.common.net.HostAndPort;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Item;
import net.qrono.server.data.Timestamp;
import net.qrono.server.grpc.QueueServerService;
import net.qrono.server.redis.RedisChannelInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    var config = Config.load();
    log.info("Config {}", config);

    Path root = config.dataRoot();
    Files.createDirectories(root);

    StaticIOWorkerPool ioScheduler = new StaticIOWorkerPool(4);
    ioScheduler.startAsync().awaitRunning();

    Path queuesDirectory = root.resolve(config.dataQueuesDir());
    Files.createDirectories(queuesDirectory);

    var idGenerator = new PersistentIdGenerator(root.resolve("last-id"));
    idGenerator.startAsync().awaitRunning();

    Path workingSetDirectory = root.resolve(config.dataWorkingSetDir());

    var workingSet = new DiskBackedWorkingSet(
        workingSetDirectory,
        toIntExact(config.dataWorkingSetMappedFileSize().bytes()));

    workingSet.startAsync().awaitRunning();

    QueueFactory queueFactory = new QueueFactory(
        queuesDirectory,
        idGenerator,
        ioScheduler,
        workingSet);
    Map<String, Queue> queues = new HashMap<>();
    Files.list(queuesDirectory).forEach(entry -> {
      if (Files.isDirectory(entry)) {
        String queueName = entry.getFileName().toString();
        queues.put(queueName, queueFactory.createQueue(queueName));
      }
    });

    var queueService = new QueueService(queueFactory, queues);
    QueueServerService service = new QueueServerService(queueService);

    Server server =
        NettyServerBuilder.forAddress(toSocketAddress(config.netListenGrpc()))
            .addService(service)
            .build();

    // -----------------------------------------------------------------------
    // Vert.x Web
    // -----------------------------------------------------------------------

    // /v1/queues/{queueName}:{enqueue|dequeue|requeue|release}
    Supplier<Verticle> verticleFactory = () -> new AbstractVerticle() {
      @Override
      public void start() throws Exception {
        var httpServer = vertx.createHttpServer();
        var router = Router.router(vertx);
        var queuePath = "/v1/queues/(?<queueName>[^/]+)";

        router.routeWithRegex(HttpMethod.POST, queuePath + ":enqueue")
            .handler(BodyHandler.create())
            .blockingHandler(ctx -> {
              var json = ctx.getBodyAsJson();
              var queue = queueService.getOrCreateQueue(ctx.pathParam("queueName"));
              var value = ByteString.copyFrom(json.getBinary("value"));
              Timestamp deadline = null;
              var deadlineString = json.getString("deadline");
              if (deadlineString != null) {
                long millis;
                try {
                  millis = Long.parseLong(deadlineString);
                } catch (NumberFormatException e) {
                  millis = Instant.parse(deadlineString).toEpochMilli();
                }
                deadline = ImmutableTimestamp.of(millis);
              }

              Item item;
              try {
                item = queue.enqueue(value, deadline);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
              String encoded = Json.encode(Map.of(
                  "id", item.id(),
                  "deadline", Instant.ofEpochMilli(item.deadline().millis()).toString()
              ));
              ctx.response().putHeader("Content-Length", Integer.toString(encoded.length()));
              ctx.response().write(encoded);
              ctx.response().end();
            });

        httpServer.requestHandler(router).listen(
            config.netListenHttp().getPort(),
            config.netListenHttp().getHost());
        super.start();
      }
    };

    var vertx = Vertx.vertx(new VertxOptions().setPreferNativeTransport(true));
    vertx.exceptionHandler(e -> log.error("Uncaught exception", e));
    vertx.deployVerticle(verticleFactory, new DeploymentOptions().setInstances(2));

    // -----------------------------------------------------------------------
    // Netty Redis
    // -----------------------------------------------------------------------

    var parentGroup = new EpollEventLoopGroup();
    var childGroup = new EpollEventLoopGroup();
    var redisServer = new ServerBootstrap()
        .group(parentGroup, childGroup)
        .channel(EpollServerSocketChannel.class)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new RedisChannelInitializer(queueService));

    var channelFuture = redisServer
        .bind(toSocketAddress(config.netListenResp()))
        .sync();

    // -----------------------------------------------------------------------

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        server.shutdown().awaitTermination();
      } catch (InterruptedException e) {
        // TODO: Log and bail
      }
    }));

    server.start().awaitTermination();
  }

  private static SocketAddress toSocketAddress(HostAndPort hostAndPort) {
    return new InetSocketAddress(hostAndPort.getHost(), hostAndPort.getPort());
  }
}
