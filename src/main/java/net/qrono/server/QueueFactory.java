package net.qrono.server;

import java.nio.file.Path;
import java.time.Clock;

public class QueueFactory {
  private final Path directory;
  private final IdGenerator idGenerator;
  private final IOScheduler ioScheduler;
  private final WorkingSet workingSet;
  private final SegmentFlushScheduler segmentFlushScheduler;

  public QueueFactory(Path directory, IdGenerator idGenerator,
      IOScheduler ioScheduler, WorkingSet workingSet,
      SegmentFlushScheduler segmentFlushScheduler) {
    this.directory = directory;
    this.idGenerator = idGenerator;
    this.ioScheduler = ioScheduler;
    this.workingSet = workingSet;
    this.segmentFlushScheduler = segmentFlushScheduler;
  }

  public Queue createQueue(String name) {
    var queueDirectory = directory.resolve(name);
    var segmentWriter = new StandardSegmentWriter(queueDirectory);

    var queueData = new QueueData(
        queueDirectory,
        ioScheduler,
        segmentWriter,
        segmentFlushScheduler);

    var queue = new Queue(queueData, idGenerator, Clock.systemUTC(), workingSet);
    queue.startAsync().awaitRunning();

    return queue;
  }
}
