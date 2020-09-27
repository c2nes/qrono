package net.qrono.server;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Comparator.comparing;

import com.google.common.util.concurrent.AbstractScheduledService;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueManager extends AbstractScheduledService {
  private static final Logger log = LoggerFactory.getLogger(QueueManager.class);

  private final Path directory;
  private final IdGenerator idGenerator;
  private final IOScheduler ioScheduler;
  private final WorkingSet workingSet;

  private final Map<String, Queue> queues = new HashMap<>();

  public QueueManager(
      Path directory,
      IdGenerator idGenerator,
      IOScheduler ioScheduler,
      WorkingSet workingSet
  ) {
    this.directory = directory;
    this.idGenerator = idGenerator;
    this.ioScheduler = ioScheduler;
    this.workingSet = workingSet;

    addListener(new Listener() {
      @Override
      public void failed(State from, Throwable failure) {
        log.error("QueueManager failed (was {}). Compactions will not run.", from, failure);
      }
    }, directExecutor());
  }

  @Override
  protected synchronized void startUp() throws Exception {
    Files.list(directory).forEach(entry -> {
      if (Files.isDirectory(entry)) {
        var queueName = entry.getFileName().toString();
        queues.put(queueName, createQueue(queueName));
      }
    });
  }

  public synchronized Queue getQueue(String queueName) {
    return queues.get(queueName);
  }

  public synchronized Queue getOrCreateQueue(String queueName) {
    Queue queue = queues.get(queueName);
    if (queue == null) {
      queue = createQueue(queueName);
      queues.put(queueName, queue);
    }
    return queue;
  }

  private Queue createQueue(String name) {
    var queueDirectory = directory.resolve(name);
    var segmentWriter = new StandardSegmentWriter(queueDirectory);

    var queueData = new QueueData(queueDirectory, ioScheduler, segmentWriter);
    queueData.startAsync().awaitRunning();

    var queue = new Queue(queueData, idGenerator, Clock.systemUTC(), workingSet);
    queue.startAsync().awaitRunning();

    return queue;
  }

  private static long compactionScore(Queue queue) {
    return queue.getQueueInfo().storageStats().persistedTombstoneCount();
  }

  @Override
  protected void runOneIteration() throws Exception {
    var maybeQueue = queues.values().stream().max(comparing(QueueManager::compactionScore));
    if (maybeQueue.isPresent()) {
      var queue = maybeQueue.get();
      queue.runTestCompaction();
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        Duration.ofMinutes(1),
        Duration.ofMinutes(1));
  }
}
