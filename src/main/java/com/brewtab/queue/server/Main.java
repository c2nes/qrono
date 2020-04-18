package com.brewtab.queue.server;

import io.grpc.netty.NettyServerBuilder;
import java.nio.file.Path;

public class Main {
  public static void main(String[] args) {
    Path root = Path.of("/tmp/queue-server-test");
    new IdGeneratorImpl();
    new QueueFactory(root, )
    new QueueServerService()
    NettyServerBuilder.forPort(8080);
  }
}
