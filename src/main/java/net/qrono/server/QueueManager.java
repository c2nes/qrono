package net.qrono.server;

import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Comparator.comparing;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.google.common.util.concurrent.AbstractScheduledService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import net.qrono.server.exceptions.QueueNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueManager extends AbstractScheduledService {
  private static final Logger log = LoggerFactory.getLogger(QueueManager.class);

  private final Path directory;
  private final QueueFactory factory;
  private final Map<String, QueueWrapper> queues = new HashMap<>();

  public QueueManager(Path directory, QueueFactory factory) {
    this.directory = directory;
    this.factory = factory;

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
        queues.put(queueName, new QueueWrapper(queueName, factory.createQueue(queueName)));
      }
    });
  }

  @Override
  protected void runOneIteration() throws Exception {
    List<QueueWrapper> queuesCopy;
    synchronized (this) {
      queuesCopy = List.copyOf(queues.values());
    }

    var maybeQueue = queuesCopy.stream()
        .max(comparing(QueueWrapper::compactionScore))
        .map(QueueWrapper::getQueue);

    if (maybeQueue.isPresent()) {
      var queue = maybeQueue.get();
      queue.compact();
    }
  }

  private synchronized QueueWrapper getQueue(String queueName) {
    return queues.get(queueName);
  }

  private synchronized QueueWrapper getOrCreateQueue(String queueName) {
    QueueWrapper queue = queues.get(queueName);
    if (queue == null) {
      queue = new QueueWrapper(queueName, factory.createQueue(queueName));
      queues.put(queueName, queue);
    }
    return queue;
  }

  public <T> CompletableFuture<T> withExistingQueueAsync(
      String queueName, Function<Queue, CompletableFuture<T>> operation
  ) {
    while (true) {
      QueueWrapper wrapper = getQueue(queueName);
      if (wrapper == null) {
        return failedFuture(new QueueNotFoundException());
      }

      var result = wrapper.apply(operation);
      if (result.isPresent()) {
        return result.get();
      }
    }
  }

  public <T> T withExistingQueue(String queueName, Function<Queue, CompletableFuture<T>> operation)
      throws IOException {
    try {
      return withExistingQueueAsync(queueName, operation).join();
    } catch (CompletionException e) {
      var cause = e.getCause();
      // Includes QronoException
      propagateIfPossible(cause, IOException.class, RuntimeException.class);
      throw new RuntimeException(cause);
    }
  }

  public <T> CompletableFuture<T> withQueueAsync(
      String queueName, Function<Queue, CompletableFuture<T>> operation
  ) {
    while (true) {
      var result = getOrCreateQueue(queueName).apply(operation);
      if (result.isPresent()) {
        return result.get();
      }
    }
  }

  public <T> T withQueue(String queueName, Function<Queue, CompletableFuture<T>> operation)
      throws IOException {
    try {
      return withQueueAsync(queueName, operation).join();
    } catch (CompletionException e) {
      var cause = e.getCause();
      // Includes QronoException
      propagateIfPossible(cause, IOException.class, RuntimeException.class);
      throw new RuntimeException(cause);
    }
  }

  public void deleteQueue(String queueName) throws IOException {
    var queue = getQueue(queueName);
    if (queue != null) {
      // delete() will handle removing the queue from this.queues.
      queue.delete();
    }
  }

  private class QueueWrapper {
    private final String queueName;
    private final Queue queue;
    private boolean deleted = false;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private QueueWrapper(String queueName, Queue queue) {
      this.queueName = queueName;
      this.queue = queue;
    }

    public Queue getQueue() {
      return queue;
    }

    public <T> Optional<T> apply(Function<Queue, T> f) {
      lock.readLock().lock();
      try {
        if (deleted) {
          return Optional.empty();
        }
        return Optional.of(f.apply(queue));
      } finally {
        lock.readLock().unlock();
      }
    }

    public long compactionScore() {
      return apply(Queue::getQueueInfoAsync)
          .map(CompletableFuture::join)
          .map(info -> info.storageStats().persistedTombstoneCount())
          .orElse(0L);
    }

    public void delete() throws IOException {
      lock.writeLock().lock();
      try {
        queue.stopAsync().awaitTerminated();
        queue.delete();
        deleted = true;
        synchronized (QueueManager.this) {
          queues.remove(queueName, this);
        }
      } finally {
        lock.writeLock().unlock();
      }
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(
        Duration.ofMinutes(1),
        Duration.ofMinutes(1));
  }
}
