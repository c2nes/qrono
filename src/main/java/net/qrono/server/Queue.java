package net.qrono.server;

import static net.qrono.server.Encoding.entrySize;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.protobuf.ByteString;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.util.DaemonThreadFactory;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import javax.annotation.Nullable;
import net.qrono.server.data.Entry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableQueueInfo;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Item;
import net.qrono.server.data.QueueInfo;
import net.qrono.server.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: When we "drop" a queue we'll need to ensure we clear its entries from the
//   working set. Alternatively, we could that no items are dequeued when dropping
//   a queue (i.e. make it the user's responsibility).
public class Queue extends AbstractIdleService {
  private static final Logger log = LoggerFactory.getLogger(Queue.class);

  private final QueueData data;
  private final IdGenerator idGenerator;
  private final Clock clock;

  private final WorkingSet workingSet;
  private final LongSet dequeuedIds = new LongOpenHashSet();

  private class BatchEventHandler implements EventHandler<Op> {
    private final List<Op> ops = new ArrayList<>();
    private final List<Entry> entries = new ArrayList<>();
    private int entriesSizeBytes = 0;

    @Override
    public void onEvent(Op op, long sequence, boolean endOfBatch) {
      ops.add(op);
      op.prepareRequest(entries);
      entriesSizeBytes += op.sizeBytes();

      // TODO: Make this configurable
      if (entriesSizeBytes > 1024 * 1024 || endOfBatch) {
        flushBatch();
      }
    }

    private void flushBatch() {
      if (log.isTraceEnabled()) {
        log.trace("Flushing batch; opsSize={}, entriesSize={}, entriesSizeBytes={}",
            ops.size(), entries.size(), entriesSizeBytes);
      }

      try {
        var results = data.write(entries);
        ops.forEach(op -> op.takeResult(results));
      } catch (Exception e) {
        ops.forEach(op -> op.takeResult(e));
      } finally {
        ops.forEach(Op::reset);
        ops.clear();
        entries.clear();
        entriesSizeBytes = 0;
      }
    }
  };

  private final Disruptor<OpHolder> disruptor;
  private final RingBuffer<OpHolder> opBuffer;

  private final EnqueueTranslator enqueueTranslator = new EnqueueTranslator();
  private final ReleaseTranslator releaseTranslator = new ReleaseTranslator();
  private final RequeueTranslator requeueTranslator = new RequeueTranslator();
  private final DequeueTranslator dequeueTranslator = new DequeueTranslator();
  private final GetQueueInfoTranslator getQueueInfoTranslator = new GetQueueInfoTranslator();

  public Queue(QueueData queueData, IdGenerator idGenerator, Clock clock, WorkingSet workingSet) {
    this.data = queueData;
    this.idGenerator = idGenerator;
    this.clock = clock;
    this.workingSet = workingSet;

    disruptor = new Disruptor<>(OpHolder::new, 1024, DaemonThreadFactory.INSTANCE);
    disruptor.handleEventsWith(new BatchEventHandler());
    opBuffer = disruptor.getRingBuffer();
  }

  @Override
  protected void startUp() {
    data.awaitRunning();
    disruptor.start();
  }

  @Override
  protected void shutDown() {
    disruptor.shutdown();
  }

  public CompletableFuture<Item> enqueueAsync(ByteString value, @Nullable Timestamp deadline) {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<Entry>();
    opBuffer.publishEvent(enqueueTranslator, result, value, deadline);
    return result.thenApply(Entry::item);
  }

  public Item enqueue(ByteString value, @Nullable Timestamp deadline) throws IOException {
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
    opBuffer.publishEvent(dequeueTranslator, result);
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
    var result = new CompletableFuture<Entry>();
    opBuffer.publishEvent(releaseTranslator, result, id);

    return result.thenRun(() -> {
      // Convert from CompletableFuture<Entry> to CF<Void>
    });
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
    var result = new CompletableFuture<Entry>();
    opBuffer.publishEvent(requeueTranslator, result, id, deadline);
    return result.thenApply(entry -> entry.key().deadline());
  }

  public Timestamp requeue(long id, @Nullable Timestamp deadline) throws IOException {
    try {
      return requeueAsync(id, deadline).join();
    } catch (CompletionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new RuntimeException(e);
    }
  }

  public CompletableFuture<QueueInfo> getQueueInfoAsync() {
    Preconditions.checkState(isRunning());
    var result = new CompletableFuture<QueueInfo>();
    opBuffer.publishEvent(getQueueInfoTranslator, result);
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

  public void runTestCompaction() throws IOException {
    Preconditions.checkState(isRunning());
    data.runTestCompaction();
  }

  class OpHolder implements Op {
    private final Enqueue enqueue = new Enqueue();
    private final Dequeue dequeue = new Dequeue();
    private final Release release = new Release();
    private final Requeue requeue = new Requeue();
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

    abstract class AbstractEntryOp extends AbstractOp<Entry> {
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

      @Override
      public final void takeResult(List<? extends Entry> results) {
        if (_deferredError != null) {
          takeResult(_deferredError);
        } else {
          if (_resultIdx >= 0) {
            result.complete(results.get(_resultIdx));
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

    class Enqueue extends AbstractEntryOp {
      private final ImmutableItem.Builder pendingBuilder = ImmutableItem.builder();

      @Override
      public void prepareRequest(List<? super Entry> batch) {
        var pending = Entry.newPendingEntry(pendingBuilder
            .id(idGenerator.generateId())
            .build());
        batch.add(pending);
        _resultIdx = batch.size() - 1;
        _size = entrySize(pending);
      }

      @Override
      public void reset() {
        super.reset();
        pendingBuilder.value(ByteString.EMPTY);
      }
    }

    class Release extends AbstractEntryOp {
      private long id;

      // For internal use
      private WorkingSet.ItemRef _item = null;

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

    class Requeue extends AbstractEntryOp {
      private long id;
      private Timestamp requeueTime;
      private Timestamp deadline;

      // For internal use
      private WorkingSet.ItemRef _item = null;

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

  class EnqueueTranslator implements
      EventTranslatorThreeArg<OpHolder, CompletableFuture<Entry>, ByteString, Timestamp> {
    @Override
    public void translateTo(
        OpHolder event,
        long sequence,
        CompletableFuture<Entry> result,
        ByteString value,
        Timestamp deadline
    ) {
      var enqueueTime = ImmutableTimestamp.of(clock.millis());
      event.op = event.enqueue;
      event.enqueue.result = result;
      event.enqueue.pendingBuilder
          .deadline(deadline == null ? enqueueTime : deadline)
          .stats(ImmutableItem.Stats.builder()
              .dequeueCount(0)
              .enqueueTime(enqueueTime)
              .requeueTime(Timestamp.ZERO)
              .build())
          .value(value);
    }
  }

  class ReleaseTranslator implements
      EventTranslatorTwoArg<OpHolder, CompletableFuture<Entry>, Long> {
    @Override
    public void translateTo(
        OpHolder event,
        long sequence,
        CompletableFuture<Entry> result,
        Long id
    ) {
      var op = event.set(event.release, result);
      op.id = id;
    }
  }

  class RequeueTranslator implements
      EventTranslatorThreeArg<OpHolder, CompletableFuture<Entry>, Long, Timestamp> {
    @Override
    public void translateTo(
        OpHolder event,
        long sequence,
        CompletableFuture<Entry> result,
        Long id,
        Timestamp deadline
    ) {
      var requeueTime = ImmutableTimestamp.of(clock.millis());
      var op = event.set(event.requeue, result);
      op.result = result;
      op.id = id;
      op.requeueTime = requeueTime;
      op.deadline = deadline == null ? requeueTime : deadline;
    }
  }

  class DequeueTranslator implements EventTranslatorOneArg<OpHolder, CompletableFuture<Item>> {
    @Override
    public void translateTo(OpHolder event, long sequence, CompletableFuture<Item> result) {
      event.set(event.dequeue, result);
    }
  }

  class GetQueueInfoTranslator implements
      EventTranslatorOneArg<OpHolder, CompletableFuture<QueueInfo>> {
    @Override
    public void translateTo(OpHolder event, long sequence, CompletableFuture<QueueInfo> result) {
      event.set(event.getQueueInfo, result);
    }
  }
}
