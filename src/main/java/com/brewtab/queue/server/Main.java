package com.brewtab.queue.server;

import io.grpc.netty.NettyServerBuilder;
import java.nio.file.Path;

public class Main {
  public static void main(String[] args) {
    Path root = Path.of("/tmp/queue-server-test");

    NettyServerBuilder.forPort(8080);
  }
}
