package net.qrono.server;

import static java.lang.Math.toIntExact;

import com.google.common.net.HostAndPort;
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

    var queueManager = new QueueManager(queuesDirectory, idGenerator, ioScheduler, workingSet);
    queueManager.startAsync().awaitRunning();

    QueueServerService service = new QueueServerService(queueManager);

    Server server =
        NettyServerBuilder.forAddress(toSocketAddress(config.netListenGrpc()))
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
