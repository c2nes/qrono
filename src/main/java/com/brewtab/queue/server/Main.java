package com.brewtab.queue.server;

import com.brewtab.queue.Api.GlobalState;
import com.google.protobuf.util.Timestamps;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
  public static void main(String[] args) throws IOException, InterruptedException {
    Clock clock = Clock.systemUTC();

    // TODO: This should be configurable...
    Path root = Path.of("/tmp/queue-server-test");

    Path globalStatePath = root.resolve("state.bin");
    GlobalState globalState;
    if (Files.exists(globalStatePath)) {
      globalState = GlobalState.parseFrom(Files.readAllBytes(globalStatePath));
    } else {
      globalState = GlobalState.newBuilder()
          .setEpoch(Timestamps.fromMillis(clock.millis()))
          .build();
      Files.write(globalStatePath, globalState.toByteArray());
    }

    StaticIOWorkerPool ioScheduler = new StaticIOWorkerPool(4);
    ioScheduler.startAsync().awaitRunning();

    Path queuesDirectory = root.resolve("queues");
    Files.createDirectories(queuesDirectory);

    Map<Path, QueueData> queueData = new HashMap<>();
    List<QueueLoadSummary> loadSummaries = new ArrayList<>();
    Files.list(queuesDirectory).forEach(entry -> {
      if (Files.isDirectory(entry)) {
        var writer = new StandardSegmentWriter(entry);
        var data = new QueueData(entry, ioScheduler, writer);
        try {
          loadSummaries.add(data.load());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        queueData.put(entry, data);
      }
    });

    long maxId = loadSummaries.stream()
        .mapToLong(QueueLoadSummary::getMaxId)
        .max()
        .orElse(0);

    long epoch = Timestamps.toMillis(globalState.getEpoch());
    IdGenerator idGenerator = new StandardIdGenerator(clock, epoch, maxId);

    QueueFactory queueFactory = new QueueFactory(queuesDirectory, idGenerator, ioScheduler);
    Map<String, Queue> queues = new HashMap<>();
    queueData.forEach((path, data) -> {
      String queueName = path.getFileName().toString();
      Queue queue = new Queue(data, idGenerator, clock);
      queues.put(queueName, queue);
    });

    QueueServerService service = new QueueServerService(queueFactory, queues);
    Server server = NettyServerBuilder.forPort(8080)
        .addService(service)
        .build();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        server.shutdown().awaitTermination();
      } catch (InterruptedException e) {
        // TODO: Log and bail
      }
    }));

    server.start().awaitTermination();
  }
}
