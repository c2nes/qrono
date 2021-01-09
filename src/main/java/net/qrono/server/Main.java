package net.qrono.server;

import static java.lang.Math.toIntExact;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import net.qrono.server.grpc.QueueServerService;
import net.qrono.server.redis.RedisChannelInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws IOException, InterruptedException {
    Thread.setDefaultUncaughtExceptionHandler((thread, ex) -> {
      log.error("Caught unhandled exception. Terminating; thread={}", thread, ex);
      System.exit(1);
    });

    var config = Config.load();
    log.info("Config {}", config);

    Path root = config.dataRoot();
    Files.createDirectories(root);

    ExecutorIOScheduler ioScheduler = new ExecutorIOScheduler(
        Executors.newFixedThreadPool(4, new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("Qrono-IOWorker-%d")
            .build()));

    Path queuesDirectory = root.resolve(config.dataQueuesDir());
    Files.createDirectories(queuesDirectory);

    var idGenerator = new PersistentIdGenerator(root.resolve("last-id"));
    idGenerator.startAsync().awaitRunning();

    Path workingSetDirectory = root.resolve(config.dataWorkingSetDir());

    var workingSet = new DiskBackedWorkingSet(
        workingSetDirectory,
        toIntExact(config.dataWorkingSetMappedFileSize().bytes()),
        ioScheduler);

    workingSet.startAsync().awaitRunning();

    var segmentFlushScheduler = new SegmentFlushScheduler(config.segmentFlushThreshold().bytes());
    var queueFactory = new QueueFactory(
        queuesDirectory,
        idGenerator,
        ioScheduler,
        workingSet,
        segmentFlushScheduler);
    var queueManager = new QueueManager(queuesDirectory, queueFactory);
    queueManager.startAsync().awaitRunning();

    QueueServerService service = new QueueServerService(queueManager);

    Server server =
        NettyServerBuilder.forAddress(toSocketAddress(config.netGrpcListen()))
            .addService(service)
            .build();

    // -----------------------------------------------------------------------
    // Netty Redis
    // -----------------------------------------------------------------------

    var parentGroup = new EpollEventLoopGroup();
    var childGroup = new EpollEventLoopGroup();
    var redisServer = new ServerBootstrap()
        .group(parentGroup, childGroup)
        .channel(EpollServerSocketChannel.class)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new RedisChannelInitializer(queueManager));

    var channelFuture = redisServer
        .bind(toSocketAddress(config.netRespListen()))
        .sync();

    // -----------------------------------------------------------------------
    // gRPC Gateway (HTTP interface)
    // -----------------------------------------------------------------------

    if (config.netHttpGatewayPath().isPresent()) {
      var gatewayPath = config.netHttpGatewayPath().get();
      if (Files.isExecutable(gatewayPath)) {
        var cmd = List.of(
            gatewayPath.toString(),
            "-target", config.netGrpcListen().toString(),
            "-listen", config.netHttpListen().toString()
        );

        new ProcessBuilder(cmd).inheritIO().start();
      }
    }

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
