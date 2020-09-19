package net.qrono.server;

import java.nio.file.Path;
import java.time.Clock;

public class QueueFactory {
  private final Path directory;
  private final IdGenerator idGenerator;
  private final IOScheduler ioScheduler;
  private final WorkingSet workingSet;

  public QueueFactory(
      Path directory,
      IdGenerator idGenerator,
      IOScheduler ioScheduler,
      WorkingSet workingSet
  ) {
    this.directory = directory;
    this.idGenerator = idGenerator;
    this.ioScheduler = ioScheduler;
    this.workingSet = workingSet;
  }

  public Queue createQueue(String name) {
    var queueDirectory = directory.resolve(name);
    var segmentWriter = new StandardSegmentWriter(queueDirectory);

    var queueData = new QueueData(queueDirectory, ioScheduler, segmentWriter);
    queueData.startAsync().awaitRunning();

    return createQueue(queueData);
  }

  public Queue createQueue(QueueData data) {
    var queue = new Queue(data, idGenerator, Clock.systemUTC(), workingSet);
    queue.startAsync().awaitRunning();

    return queue;
  }
}
