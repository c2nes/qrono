package net.qrono.server;

import java.nio.file.Path;
import java.time.Clock;

public class QueueFactory {
  private final Path directory;
  private final IdGenerator idGenerator;
  private final TaskScheduler ioScheduler;
  private final TaskScheduler cpuScheduler;
  private final WorkingSet workingSet;
  private final SegmentFlushScheduler segmentFlushScheduler;

  public QueueFactory(
      Path directory,
      IdGenerator idGenerator,
      TaskScheduler ioScheduler,
      TaskScheduler cpuScheduler,
      WorkingSet workingSet,
      SegmentFlushScheduler segmentFlushScheduler) {
    this.directory = directory;
    this.idGenerator = idGenerator;
    this.ioScheduler = ioScheduler;
    this.cpuScheduler = cpuScheduler;
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

    var queue = new Queue(queueData, idGenerator, Clock.systemUTC(), workingSet, cpuScheduler);
    queue.startAsync().awaitRunning();

    return queue;
  }
}
