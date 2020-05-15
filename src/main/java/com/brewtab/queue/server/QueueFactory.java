package com.brewtab.queue.server;

import java.io.IOException;
import java.io.UncheckedIOException;
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
    Path queueDirectory = directory.resolve(name);
    StandardSegmentWriter segmentWriter = new StandardSegmentWriter(queueDirectory);
    QueueData queueData = new QueueData(queueDirectory, ioScheduler, segmentWriter);
    // TODO: This shouldn't be here and in Main
    try {
      queueData.load();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return new Queue(queueData, idGenerator, Clock.systemUTC(), workingSet);
  }

  public Queue createQueue(QueueData data) {
    return new Queue(data, idGenerator, Clock.systemUTC(), workingSet);
  }
}
