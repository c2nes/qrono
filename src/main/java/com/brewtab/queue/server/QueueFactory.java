package com.brewtab.queue.server;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;

public class QueueFactory {
  private final Path directory;
  private final IdGenerator idGenerator;
  private final IOScheduler ioScheduler;

  public QueueFactory(
      Path directory,
      IdGenerator idGenerator,
      IOScheduler ioScheduler) {
    this.directory = directory;
    this.idGenerator = idGenerator;
    this.ioScheduler = ioScheduler;
  }

  public Queue createQueue(String name) throws IOException {
    Path queueDirectory = directory.resolve(name);
    StandardSegmentFreezer segmentFreezer = new StandardSegmentFreezer(queueDirectory);
    QueueData queueData = new QueueData(queueDirectory, ioScheduler, segmentFreezer);
    return new Queue(queueData, idGenerator, Clock.systemUTC());
  }
}
