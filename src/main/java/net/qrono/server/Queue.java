package net.qrono.server;

import static net.qrono.server.Encoding.entrySize;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;
import io.prometheus.client.Histogram;
import java.io.IOException;
import java.time.Clock;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import net.qrono.server.data.Entry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableQueueInfo;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Item;
import net.qrono.server.data.MutableEntry;
import net.qrono.server.data.QueueInfo;
import net.qrono.server.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: When we "drop" a queue we'll need to ensure we clear its entries from the
//   working set. Alternatively, we could that no items are dequeued when dropping
//   a queue (i.e. make it the user's responsibility).
public class Queue extends AbstractIdleService {
  private static final Logger log = LoggerFactory.getLogger(Queue.class);

  private static final Histogram opBatchSize = Histogram.build()
      .name("queue_op_batch_size")
      .help("size of flushed operation batches")
      .exponentialBuckets(1, 2, 20)
      .register();

  private static final Histogram enqueueTime = Histogram.build()
      .name("queue_enqueue_time")
      .help("time to enqueue a value")
      .buckets(
          10e-6, 25e-6, 50e-6, 75e-6,
          100e-6, 250e-6, 500e-6, 750e-6,
          1e-3, 2.5e-3, 5e-3, 7.5e-3,
          10e-3, 25e-3, 50e-3, 75e-3,
          100e-3, 250e-3, 500e-3, 750e-3,
          1, 2.5, 5.0, 7.5,
          10
      )
      .register();

  @VisibleForTesting
  final QueueData data;

  private final IdGenerator idGenerator;
  private final Clock clock;

  private final WorkingSet workingSet;
  private final Set<Long> dequeuedIds = new LinkedHashSet<>();

  private static final int OP_BUF_SIZE = 8 * 1024;
  private final OpBuf opBuffer = new OpBuf(OP_BUF_SIZE);
  private final List<Entry> entries = new ArrayList<>(OP_BUF_SIZE);
  private final TaskScheduler.Handle opBufferConsumer;

  public Queue(
      QueueData queueData,
      IdGenerator idGenerator,
      Clock clock,
      WorkingSet workingSet,
      TaskScheduler scheduler
  ) {
    this.data = queueData;
    this.idGenerator = idGenerator;
    this.clock = clock;
    this.workingSet = workingSet;
    opBufferConsumer = scheduler.register(this::processOpBuffer);
  }

  @Override
  protected void startUp() {
    data.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws InterruptedException {
    opBufferConsumer.cancel().join();
    data.stopAsync().awaitTerminated();
  }

  public synchronized void delete() throws IOException {
    Preconditions.checkState(state() == State.TERMINATED);

    // Remove all entries from the working set
    for (Long id : dequeuedIds) {
      workingSet.get(id).release();
    }

    data.delete();
  }

  public CompletableFuture<Item> enqueueAsync(ByteBuf value, @Nullable Timestamp deadline) {
    Preconditions.checkState(isRunning());
    var start = System.nanoTime();
    var enqueueTime = clock.millis();
    var result = new CompletableFuture<Item>();

    synchronized (opBuffer) {
      var event = opBuffer.acquire();
      event.op = event.enqueue;
      event.enqueue.result = result;
      event.enqueue.start = start;

      var entry = event.enqueue.entry;
      entry.deadlineMillis = enqueueTime;
      entry.dequeueCount = 0;
      entry.enqueueTimeMillis = enqueueTime;
      entry.requeueTimeMillis = 0;
      // Ownership transfer in Enqueue#prepareRequest, and then released after the batch write.
      entry.value = ReferenceCountUtil.retain(value);
    }

    opBufferConsumer.schedule();
    return result;
  }

  public Item enqueue(ByteBuf value, @Nullable Timestamp deadline) throws IOException {
    try {
      return enqueueAsync(value, deadline).join();
    } catch (CompletionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<Item> dequeueAsync() {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<Item>();
    synchronized (opBuffer) {
      var event = opBuffer.acquire();
      event.set(event.dequeue, result);
    }
    opBufferConsumer.schedule();
    return result;
  }

  public Item dequeue() throws IOException {
    try {
      return dequeueAsync().join();
    } catch (CompletionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<Void> releaseAsync(long id) {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<Void>();
    synchronized (opBuffer) {
      var event = opBuffer.acquire();
      var op = event.set(event.release, result);
      op.id = id;
    }
    opBufferConsumer.schedule();
    return result;
  }

  public void release(long id) throws IOException {
    try {
      releaseAsync(id).join();
    } catch (CompletionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<Timestamp> requeueAsync(long id, @Nullable Timestamp deadline) {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<Timestamp>();
    var requeueTime = ImmutableTimestamp.of(clock.millis());

    synchronized (opBuffer) {
      var event = opBuffer.acquire();
      var op = event.set(event.requeue, result);
      op.result = result;
      op.id = id;
      op.requeueTime = requeueTime;
      op.deadline = deadline == null ? requeueTime : deadline;
    }
    opBufferConsumer.schedule();
    return result;
  }

  public Timestamp requeue(long id, @Nullable Timestamp deadline) throws IOException {
    try {
      return requeueAsync(id, deadline).join();
    } catch (CompletionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<Item> peekAsync() {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<Item>();
    synchronized (opBuffer) {
      var event = opBuffer.acquire();
      event.set(event.peek, result);
    }
    opBufferConsumer.schedule();
    return result;
  }

  public Item peek() throws IOException {
    try {
      return peekAsync().join();
    } catch (CompletionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<QueueInfo> getQueueInfoAsync() {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<QueueInfo>();
    synchronized (opBuffer) {
      var event = opBuffer.acquire();
      event.set(event.getQueueInfo, result);
    }
    opBufferConsumer.schedule();
    return result;
  }

  public QueueInfo getQueueInfo() {
    try {
      return getQueueInfoAsync().join();
    } catch (CompletionException e) {
      Throwables.throwIfUnchecked(e.getCause());
      throw new RuntimeException(e);
    }
  }

  private boolean processOpBuffer() {
    var ops = opBuffer.swap();

    if (ops == null) {
      return false;
    }

    for (OpHolder op : ops) {
      op.prepareRequest(entries);
    }

    if (log.isTraceEnabled()) {
      log.trace("Flushing batch; opsSize={}, entriesSize={}", ops.size(), entries.size());
    }

    opBatchSize.observe(ops.size());

    try {
      var results = data.write(entries);
      for (OpHolder op : ops) {
        op.takeResult(results);
      }
    } catch (Exception e) {
      for (OpHolder op : ops) {
        op.takeResult(e);
      }
    } finally {
      for (OpHolder op : ops) {
        op.reset();
      }
      for (Entry entry : entries) {
        entry.release();
      }
      entries.clear();
    }

    return !opBuffer.isEmpty();
  }

  public void compact() throws IOException {
    data.compact();
  }

  public void compact(boolean force) throws IOException {
    data.compact(force);
  }

  class OpHolder implements Op {
    private final Enqueue enqueue = new Enqueue();
    private final Dequeue dequeue = new Dequeue();
    private final Release release = new Release();
    private final Requeue requeue = new Requeue();
    private final Peek peek = new Peek();
    private final GetQueueInfo getQueueInfo = new GetQueueInfo();

    private Op op = null;

    public <V, O extends AbstractOp<V>> O set(O op, CompletableFuture<V> result) {
      this.op = op;
      op.result = result;
      return op;
    }

    @Override
    public void prepareRequest(List<? super Entry> batch) {
      op.prepareRequest(batch);
    }

    @Override
    public void takeResult(List<? extends Entry> results) {
      op.takeResult(results);
    }

    @Override
    public void takeResult(Throwable throwable) {
      op.takeResult(throwable);
    }

    @Override
    public int sizeBytes() {
      return op.sizeBytes();
    }

    @Override
    public void reset() {
      op.reset();
      op = null;
    }

    abstract class AbstractOp<V> implements Op {
      CompletableFuture<V> result;

      @Override
      public void reset() {
        result = null;
      }
    }

    abstract class AbstractEntryOp<V> extends AbstractOp<V> {
      // For internal use
      int _resultIdx = -1;
      int _size = 0;
      // Error to be set on the result future _unless_ the batch write fails.
      // We defer sending this error because it may be inaccurate if previous requests
      // within the same batch fail. For instance, if a batch contains duplicate release
      // operations then the first should succeed while the second should return an
      // "item not dequeued" error. If the batch write fails then this error message may
      // be misleading.
      Throwable _deferredError = null;

      protected abstract V convertResultEntry(Entry entry);

      @Override
      public final void takeResult(List<? extends Entry> results) {
        if (_deferredError != null) {
          takeResult(_deferredError);
        } else {
          if (_resultIdx >= 0) {
            result.complete(convertResultEntry(results.get(_resultIdx)));
          }
          onSuccess();
        }
      }

      @Override
      public final void takeResult(Throwable ex) {
        result.completeExceptionally(ex);
        onFailure();
      }

      void onSuccess() {
      }

      void onFailure() {
      }

      @Override
      public final int sizeBytes() {
        return _size;
      }

      @Override
      public void reset() {
        super.reset();
        _resultIdx = -1;
        _size = 0;
        _deferredError = null;
      }
    }

    class Enqueue extends AbstractEntryOp<Item> {
      private final MutableEntry entry = new MutableEntry();
      private long start;

      @Override
      public void prepareRequest(List<? super Entry> batch) {
        entry.id = idGenerator.generateId();
        batch.add(new MutableEntry(entry));
        _resultIdx = batch.size() - 1;
        _size = entrySize(entry);
      }

      @Override
      protected Item convertResultEntry(Entry entry) {
        return entry.item();
      }

      @Override
      void onSuccess() {
        enqueueTime.observe(1e-9 * (System.nanoTime() - start));
        super.onSuccess();
      }

      @Override
      void onFailure() {
        enqueueTime.observe(1e-9 * (System.nanoTime() - start));
        super.onFailure();
      }

      @Override
      public void reset() {
        super.reset();
        entry.reset();
        start = 0;
      }
    }

    class Release extends AbstractEntryOp<Void> {
      private long id;

      // For internal use
      private WorkingSet.ItemRef _item = null;

      @Override
      protected Void convertResultEntry(Entry entry) {
        return null;
      }

      @Override
      void onSuccess() {
        if (_item != null) {
          _item.release();
        }
      }

      @Override
      void onFailure() {
        if (_item != null) {
          dequeuedIds.add(id);
        }
      }

      @Override
      public void prepareRequest(List<? super Entry> batch) {
        try {
          // -1 means "release any"
          if (id == -1 && !dequeuedIds.isEmpty()) {
            id = dequeuedIds.iterator().next();
          }

          var item = workingSet.get(id);
          if (item == null || !dequeuedIds.remove(id)) {
            // TODO: Consider using a more specific exception here
            _deferredError = new IllegalStateException("item not dequeued");
            return;
          }

          var tombstone = Entry.newTombstoneEntry(item.key());
          batch.add(tombstone);
          _resultIdx = batch.size() - 1;
          _size = entrySize(tombstone);
          _item = item;
        } catch (IOException e) {
          // TODO: Re-add id to dequeuedIds?
          result.completeExceptionally(e);
        }
      }

      @Override
      public void reset() {
        super.reset();
        id = 0;
        _item = null;
      }
    }

    class Requeue extends AbstractEntryOp<Timestamp> {
      private long id;
      private Timestamp requeueTime;
      private Timestamp deadline;

      // For internal use
      private WorkingSet.ItemRef _item = null;

      @Override
      protected Timestamp convertResultEntry(Entry entry) {
        return entry.key().deadline();
      }

      @Override
      void onSuccess() {
        if (_item != null) {
          _item.release();
        }
      }

      @Override
      void onFailure() {
        if (_item != null) {
          dequeuedIds.add(id);
        }
      }

      @Override
      public void prepareRequest(List<? super Entry> batch) {
        try {
          // -1 means "requeue any"
          if (id == -1 && !dequeuedIds.isEmpty()) {
            id = dequeuedIds.iterator().next();
          }

          var itemRef = workingSet.get(id);
          if (itemRef == null || !dequeuedIds.remove(id)) {
            // TODO: Consider using a more specific exception here
            _deferredError = new IllegalStateException("item not dequeued");
            return;
          }

          var item = itemRef.item();
          var tombstone = Entry.newTombstoneEntry(itemRef.key());
          var requeue = Entry.newPendingEntry(ImmutableItem.builder()
              .from(item)
              .deadline(deadline)
              .stats(ImmutableItem.Stats.builder()
                  .from(item.stats())
                  .requeueTime(requeueTime)
                  .build())
              .build());

          batch.add(requeue);
          batch.add(tombstone);
          _resultIdx = batch.size() - 2;
          _size = entrySize(requeue) + entrySize(tombstone);
          _item = itemRef;
        } catch (IOException e) {
          // TODO: Re-add id to dequeuedIds?
          result.completeExceptionally(e);
        }
      }

      @Override
      public void reset() {
        super.reset();
        id = 0;
        requeueTime = null;
        deadline = null;
        _item = null;
      }
    }

    abstract class AbstractCallableOp<V> extends AbstractOp<V> {
      abstract V call() throws Exception;

      @Override
      public void prepareRequest(List<? super Entry> batch) {
      }

      @Override
      public void takeResult(List<? extends Entry> results) {
      }

      @Override
      public void takeResult(Throwable throwable) {
      }

      protected final void callAndSetResult() {
        try {
          result.complete(call());
        } catch (Exception e) {
          result.completeExceptionally(e);
        }
      }

      @Override
      public int sizeBytes() {
        return 0;
      }
    }

    abstract class PostBatchWriteOp<V> extends AbstractCallableOp<V> {
      @Override
      public void takeResult(List<? extends Entry> results) {
        callAndSetResult();
      }

      @Override
      public void takeResult(Throwable throwable) {
        callAndSetResult();
      }
    }

    abstract class PreBatchWriteOp<V> extends AbstractCallableOp<V> {
      @Override
      public void prepareRequest(List<? super Entry> batch) {
        callAndSetResult();
      }
    }

    class Dequeue extends PostBatchWriteOp<Item> {
      @Override
      public Item call() throws Exception {
        var key = data.peek();
        if (key == null) {
          return null;
        }

        long now = clock.millis();
        long deadline = key.deadline().millis();
        if (now < deadline) {
          // Deadline is in the future.
          return null;
        }

        var entry = data.next();
        var item = entry.item();
        if (item == null) {
          throw new UnsupportedOperationException("tombstone dequeue handling not implemented");
        }

        // Increment dequeue count
        item = ImmutableItem.builder()
            .from(item)
            .stats(ImmutableItem.Stats.builder()
                .from(item.stats())
                .dequeueCount(item.stats().dequeueCount() + 1)
                .build())
            .build();

        workingSet.add(item);
        dequeuedIds.add(item.id());

        return item;
      }
    }

    class Peek extends PreBatchWriteOp<Item> {
      @Override
      Item call() throws IOException {
        var entry = data.peekEntry();
        if (entry == null) {
          return null;
        }
        var item = entry.item();
        if (item == null) {
          throw new UnsupportedOperationException("tombstone dequeue handling not implemented");
        }
        return item;
      }
    }

    class GetQueueInfo extends PreBatchWriteOp<QueueInfo> {
      @Override
      QueueInfo call() {
        var storageStats = data.getStorageStats();
        long dequeuedSize = dequeuedIds.size();
        long pendingSize = storageStats.persistedPendingCount()
            + storageStats.bufferedPendingCount()
            - storageStats.persistedTombstoneCount()
            - storageStats.bufferedTombstoneCount();
        return ImmutableQueueInfo.builder()
            .pendingCount(pendingSize)
            .dequeuedCount(dequeuedSize)
            .storageStats(storageStats)
            .build();
      }
    }
  }

  interface Op {
    /**
     * Prepares the request, adding entries to {@code batch} as required. Implementations should
     * remember the indices of their batch entries as those will be required to retrieve results in
     * {@link #takeResult(List)}
     */
    void prepareRequest(List<? super Entry> batch);

    void takeResult(List<? extends Entry> results);

    void takeResult(Throwable throwable);

    /**
     * Returns the approximate number of bytes this entry will write. This method may throw an
     * exception or return an invalid result if called before {@link #prepareRequest(List)}.
     */
    int sizeBytes();

    void reset();
  }

  class OpBuf {
    private OpHolder[] write;
    private OpHolder[] read;
    private int writePos = 0, readPos = 0;
    private final int size;

    private final List<OpHolder> view = new AbstractList<>() {
      @Override
      public OpHolder get(int index) {
        return read[index];
      }

      @Override
      public int size() {
        return readPos;
      }

      @Override
      public Iterator<OpHolder> iterator() {
        return new Itr();
      }

      class Itr implements Iterator<OpHolder> {
        private int pos = 0;

        @Override
        public boolean hasNext() {
          return pos < readPos;
        }

        @Override
        public OpHolder next() {
          return read[pos++];
        }
      }
    };

    public OpBuf(int size) {
      this.size = size;
      write = new OpHolder[size];
      read = new OpHolder[size];
      for (int i = 0; i < size; i++) {
        write[i] = new OpHolder();
        read[i] = new OpHolder();
      }
    }

    public synchronized List<OpHolder> swap() {
      if (writePos == 0) {
        return null;
      }

      var saved = read;
      read = write;
      write = saved;
      readPos = writePos;
      writePos = 0;

      notifyAll();

      return view;
    }

    @GuardedBy("this")
    public OpHolder acquire() {
      boolean interrupted = false;
      try {
        while (writePos == size) {
          try {
            wait();
          } catch (InterruptedException e) {
            interrupted = true;
          }
        }

        return write[writePos++];
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    public synchronized boolean isEmpty() {
      return writePos == 0;
    }
  }
}
