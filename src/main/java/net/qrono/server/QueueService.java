package net.qrono.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueService {
  private static final Logger logger = LoggerFactory.getLogger(QueueService.class);
  private final QueueFactory queueFactory;
  private final Map<String, Queue> queues;

  public QueueService(QueueFactory queueFactory) {
    this(queueFactory, Collections.emptyMap());
  }

  public QueueService(QueueFactory queueFactory, Map<String, Queue> initialQueues) {
    this.queueFactory = queueFactory;
    this.queues = new HashMap<>(initialQueues);
  }

  public synchronized Queue getQueue(String queueName) {
    return queues.get(queueName);
  }

  public synchronized Queue getOrCreateQueue(String queueName) {
    Queue queue = queues.get(queueName);
    if (queue == null) {
      queue = queueFactory.createQueue(queueName);
      queues.put(queueName, queue);
    }
    return queue;
  }
}
