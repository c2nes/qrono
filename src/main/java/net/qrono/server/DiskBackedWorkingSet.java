package net.qrono.server;

import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.protobuf.ByteString;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import net.qrono.server.data.Entry.Key;
import net.qrono.server.data.Entry.Type;
import net.qrono.server.data.ImmutableEntry;
import net.qrono.server.data.ImmutableItem;
import net.qrono.server.data.ImmutableTimestamp;
import net.qrono.server.data.Item;
import net.qrono.server.util.LinkedNode;
import net.qrono.server.util.LinkedNodeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Disk backed working set with in-memory caching. While disk backed, this working set
 * implementation is not persistent and the contents will be lost when the process exits.
 */
public class DiskBackedWorkingSet extends AbstractIdleService implements WorkingSet {
  private static final Logger log = LoggerFactory.getLogger(DiskBackedWorkingSet.class);

  @VisibleForTesting
  static final String FILE_SUFFIX = ".tmp";

  private static final byte[] ZERO_BYTE = {0};
  private static final double OCCUPANCY_DRAIN_THRESHOLD = 0.5;

  private final Path directory;
  private final int mappedFileSize;
  private final int offsetMask;
  private final int offsetMaskLen;

  private final Map<Long, WorkingItem> entries = new HashMap<>();
  private final LinkedHashMap<Long, MappedFile> files = new LinkedHashMap<>();
  private final Deque<MappedFile> emptyFiles = new ArrayDeque<>();
  private MappedFile currentFile = null;

  private long totalSize = 0;
  private long totalUsed = 0;

  private final IOScheduler.Handle drainer;

  public DiskBackedWorkingSet(Path directory, int mappedFileSize, IOScheduler ioScheduler) {
    // STATS_SIZE sets a strict lower bound on file size, but actual size must be large
    // enough to hold the largest allowed item value (which is set elsewhere). Generally
    // mappedFileSize should be set to a value at least a few orders of magnitude larger
    // than the average value size.
    Preconditions.checkArgument(mappedFileSize >= Encoding.STATS_SIZE,
        "mappedFileSize must be >= %s", Encoding.STATS_SIZE);

    this.directory = directory;
    this.mappedFileSize = mappedFileSize;

    var maxOffset = mappedFileSize - 1;
    offsetMask = (Integer.highestOneBit(maxOffset) << 1) - 1;
    offsetMaskLen = Integer.bitCount(offsetMask);

    // Register drain task with scheduler
    drainer = ioScheduler.register(() -> {
      var file = getFileToDrain();
      if (file == null) {
        return false;
      }

      drain(file);
      return true;
    });

    // Add listener to log failures
    addListener(new Listener() {
      @Override
      public void failed(State from, Throwable failure) {
        log.error("Worker thread failed! previousState={}", from, failure);
      }
    }, directExecutor());
  }

  @Override
  protected void startUp() throws Exception {
    Files.createDirectories(directory);

    // Clean up any old files
    try (var children = Files.newDirectoryStream(directory)) {
      for (var child : children) {
        if (child.getFileName().toString().endsWith(FILE_SUFFIX)) {
          Files.delete(child);
        }
      }
    }

    // Create first mapped file
    currentFile = new MappedFile(0);
    files.put(currentFile.fileID, currentFile);
  }

  private synchronized MappedFile getFileToDrain() {
    if (!emptyFiles.isEmpty()) {
      return emptyFiles.removeFirst();
    }

    if (isOccupancyLow()) {
      return files.values().iterator().next();
    }

    return null;
  }

  private boolean isOccupancyLow() {
    return files.size() > 1 && ((double) totalUsed) / totalSize < OCCUPANCY_DRAIN_THRESHOLD;
  }

  private void drain(MappedFile file) throws IOException {
    while (isRunning()) {
      synchronized (this) {
        if (!file.isEmpty()) {
          addUnchecked(file.pop());
        } else {
          if (files.containsKey(file.fileID)) {
            file.close();
            files.remove(file.fileID);
            totalSize -= mappedFileSize;
          }
          return;
        }
      }
    }
  }

  @Override
  protected void shutDown() {
    // Shutdown fast; no cleanup
    drainer.cancel();
    entries.clear();
    files.clear();
  }

  @Override
  public synchronized void add(Item item) throws IOException {
    Preconditions.checkState(isRunning());
    Preconditions.checkState(!entries.containsKey(item.id()), "duplicate item");
    addUnchecked(item);
  }

  // Allows duplicates
  private void addUnchecked(Item item) throws IOException {
    var size = itemSize(item);

    Preconditions.checkArgument(size <= mappedFileSize,
        "item size (%s) must be < mappedFileSize (%s)",
        size, mappedFileSize);

    var capacity = currentFile.capacity();
    if (capacity < size) {
      // Remaining capacity is unused, but contributes to total size.
      totalSize += capacity;
      currentFile = new MappedFile(currentFile.fileID + 1);
      files.put(currentFile.fileID, currentFile);
    }

    WorkingItem entry = currentFile.add(item);
    entries.put(entry.id, entry);
  }

  @Override
  public synchronized long size() {
    return entries.size();
  }

  private MappedFile entryFile(WorkingItem entry) {
    var file = files.get(entryFileID(entry));
    if (file == null) {
      throw new IllegalStateException("BUG: entry file not found");
    }
    return file;
  }

  @Override
  public synchronized ItemRef get(long id) {
    return getInternal(id);
  }

  /**
   * Returns internal item ref representation for tests.
   */
  @VisibleForTesting
  synchronized ItemRefImpl getInternal(long id) {
    Preconditions.checkState(isRunning());

    var entry = entries.get(id);
    if (entry == null) {
      return null;
    }

    return new ItemRefImpl(entry);
  }

  private void removeFileEntry(MappedFile file, WorkingItem entry) {
    file.remove(entry);
    if (file.isEmpty()) {
      if (file == currentFile) {
        file.reset();
      } else {
        // Remove in background thread
        emptyFiles.addLast(file);
        drainer.schedule();
      }
    } else if (isOccupancyLow()) {
      drainer.schedule();
    }
  }

  long entryFileID(WorkingItem entry) {
    return entry.location >>> offsetMaskLen;
  }

  int entryOffset(WorkingItem entry) {
    return (int) (entry.location & offsetMask);
  }

  private Item entryItem(WorkingItem entry, MappedFile entryFile) {
    Item item = entry.item.get();
    if (item != null) {
      return item;
    }

    return entryFile.get(entry);
  }

  @VisibleForTesting
  class ItemRefImpl implements ItemRef {
    private WorkingItem entry;
    private MappedFile entryFile;

    private ItemRefImpl(WorkingItem entry) {
      this.entry = entry;
      entryFile = entryFile(entry);
    }

    // Draining involves atomically removing and re-adding items from the working set. This
    // operation causes a new WorkingItem to be created for the item so the copy we have may no
    // longer be valid once we've left a synchronized block.
    //
    // All ItemRef methods must call revalidate after synchronizing on DiskBackedWorkingSet.this
    private void revalidate() {
      if (!entryFile.contains(entry)) {
        var updatedEntry = entries.get(entry.id);
        if (updatedEntry == null) {
          throw new IllegalStateException("released");
        }
        entry = updatedEntry;
        entryFile = entryFile(entry);
      }
    }

    /**
     * Force clear the item SoftReference in WorkingItem so {@link #item()} reads from disk.
     */
    @VisibleForTesting
    void clearItemReferenceForTest() {
      synchronized (DiskBackedWorkingSet.this) {
        entry.item.clear();
      }
    }

    @Override
    public Key key() {
      synchronized (DiskBackedWorkingSet.this) {
        revalidate();
        return entry.toTombstoneKey();
      }
    }

    @Override
    public Item item() {
      synchronized (DiskBackedWorkingSet.this) {
        revalidate();
        return entryItem(entry, entryFile);
      }
    }

    @Override
    public void release() {
      synchronized (DiskBackedWorkingSet.this) {
        revalidate();
        verify(entries.remove(entry.id) != null,
            "BUG: global entries map and mapped file node list out of sync");
        removeFileEntry(entryFile, entry);
      }
    }
  }

  static class WorkingItem extends LinkedNode<WorkingItem> {
    private final long deadline;
    private final long id;
    private final int size;
    private final SoftReference<Item> item;
    private final long location;

    WorkingItem(Item item, long location) {
      deadline = item.deadline().millis();
      id = item.id();
      size = Encoding.STATS_SIZE + item.value().size();
      this.item = new SoftReference<>(item);
      this.location = location;
    }

    Key toTombstoneKey() {
      return ImmutableEntry.Key.builder()
          .deadline(ImmutableTimestamp.of(deadline))
          .id(id)
          .entryType(Type.TOMBSTONE)
          .build();
    }
  }

  static int writeItem(ByteBuffer buffer, Item item) {
    Encoding.writeStats(buffer, item.stats());
    item.value().copyTo(buffer);
    return itemSize(item);
  }

  private static int itemSize(Item item) {
    return Encoding.STATS_SIZE + item.value().size();
  }

  class MappedFile implements Closeable {
    private final long fileID;
    private final MappedByteBuffer buffer;
    private final LinkedNodeList<WorkingItem> entries = new LinkedNodeList<>();
    private int usedBytes = 0;

    MappedFile(long fileID) throws IOException {
      this.fileID = fileID;
      buffer = createMappedBuffer(fileID);
    }

    boolean isEmpty() {
      return usedBytes == 0;
    }

    int capacity() {
      return buffer.remaining();
    }

    WorkingItem add(Item item) {
      var offset = buffer.position();
      var location = (fileID << offsetMaskLen) | offset;
      var entry = new WorkingItem(item, location);

      var n = writeItem(buffer, item);
      usedBytes += n;
      totalSize += n;
      totalUsed += n;
      entries.add(entry);

      return entry;
    }

    boolean contains(WorkingItem entry) {
      return entries.contains(entry);
    }

    void remove(WorkingItem entry) {
      entries.remove(entry);
      usedBytes -= entry.size;
      totalUsed -= entry.size;
    }

    Item get(WorkingItem entry) {
      Preconditions.checkArgument(entryFileID(entry) == fileID, "entry not in this file");

      var bb = buffer.asReadOnlyBuffer();
      bb.position(entryOffset(entry));

      var stats = Encoding.readStats(bb);
      var value = ByteString.copyFrom(bb, entry.size - Encoding.STATS_SIZE);

      return ImmutableItem.builder()
          .deadline(ImmutableTimestamp.of(entry.deadline))
          .id(entry.id)
          .stats(stats)
          .value(value)
          .build();
    }

    Item pop() {
      WorkingItem entry = entries.remove(0);
      usedBytes -= entry.size;
      totalUsed -= entry.size;
      return entryItem(entry, this);
    }

    void reset() {
      totalSize -= buffer.position();
      buffer.clear();
    }

    @Override
    public void close() throws IOException {
      Preconditions.checkArgument(entries.isEmpty());
      Preconditions.checkArgument(usedBytes == 0);
      Files.delete(filePath(fileID));
    }
  }

  private Path filePath(long fileID) {
    return directory.resolve(String.format("%016x", fileID) + FILE_SUFFIX);
  }

  private MappedByteBuffer createMappedBuffer(long fileID) throws IOException {
    try (var channel = FileChannel.open(filePath(fileID), CREATE_NEW, READ, WRITE, SPARSE)) {
      channel.position(mappedFileSize - 1);
      channel.write(ByteBuffer.wrap(ZERO_BYTE));
      return channel.map(MapMode.READ_WRITE, 0, mappedFileSize);
    }
  }
}
