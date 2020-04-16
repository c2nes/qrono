package com.brewtab.queue.server;

import io.grpc.netty.NettyServerBuilder;

public class Main {
  public static void main(String[] args) {
    NettyServerBuilder.forPort(8080);
  }
}
