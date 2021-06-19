package net.qrono.server;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.netty.util.ReferenceCountUtil.releaseLater;
import static net.qrono.server.TestData.ITEM_1_T5;
import static net.qrono.server.TestData.withId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Iterables;
import io.netty.util.ReferenceCountUtil;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DiskBackedWorkingSetTest extends WorkingSetTestBase {
  private static final int MAPPED_FILE_SIZE = 1025;

  @Rule
  public TemporaryFolder dir = new TemporaryFolder();

  private TaskScheduler ioScheduler;
  private DiskBackedWorkingSet workingSet;

  @Before
  public void setUp() throws IOException {
    ioScheduler = new ExecutorTaskScheduler(directExecutor());
    workingSet = new DiskBackedWorkingSet(dir.getRoot().toPath(), MAPPED_FILE_SIZE, ioScheduler);
    workingSet.startAsync().awaitRunning();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (workingSet != null) {
      workingSet.stopAsync().awaitTerminated();
    }

    for (int i = 0; i < 2; i++) {
      System.gc();
      Thread.sleep(250);
    }
  }

  @Test
  public void testInitialFileAdded() throws IOException {
    workingSet.add(ITEM_1_T5);
    assertEquals(1, currentFileCount());
  }

  @Test
  public void testGetItem_fromDisk() throws IOException {
    workingSet.add(ITEM_1_T5);
    var itemRef = workingSet.getInternal(1001);
    assertNotNull(itemRef);
    // Force item to be re-read from disk
    itemRef.clearItemReferenceForTest();
    var item = itemRef.item();
    assertEquals(ITEM_1_T5, item);
    ReferenceCountUtil.release(item);
  }

  @Test
  public void testMappedFilePoolGrowsAndShrinks() throws IOException, InterruptedException {
    // Should have exactly one file initially
    assertThat(currentFileCount()).isEqualTo(1);

    // Add "MAPPED_FILE_SIZE" number of entries. Each entry is at least 1 byte
    // so this should cause > 1 mapped file to be opened.
    for (int i = 0; i < MAPPED_FILE_SIZE; i++) {
      workingSet.add(withId(ITEM_1_T5, i));
    }

    // More than one file should now be present
    assertThat(currentFileCount()).isGreaterThan(1);

    // Validate that all items are readable and then delete them
    for (int i = 0; i < MAPPED_FILE_SIZE; i++) {
      var itemRef = workingSet.getInternal(i);
      assertNotNull(itemRef);
      // Force item to be re-read from disk
      itemRef.clearItemReferenceForTest();
      var item = itemRef.item();
      assertEquals(withId(ITEM_1_T5, i), item);
      // Release item
      itemRef.release();
      ReferenceCountUtil.release(item);
    }

    // Empty files should be removed and we should exactly one file again
    assertThat(currentFileCount()).isEqualTo(1);
  }

  @Test
  public void testCleanUpFilesOnStart() throws IOException {
    // Add "MAPPED_FILE_SIZE" number of entries. Each entry is at least 1 byte
    // so this should cause > 1 mapped file to be opened.
    for (int i = 0; i < MAPPED_FILE_SIZE; i++) {
      workingSet.add(withId(ITEM_1_T5, i));
    }

    // More than one file should now be present
    assertThat(currentFileCount()).isGreaterThan(1);

    // Stop and recreate
    workingSet.stopAsync().awaitTerminated();

    // Files are not removed on shutdown
    // TODO: Should files be removed on shutdown? If so we should split this test and ensure
    //  files are cleaned up on startup and on shutdown.
    assertThat(currentFileCount()).isGreaterThan(1);

    workingSet = new DiskBackedWorkingSet(dir.getRoot().toPath(), MAPPED_FILE_SIZE, ioScheduler);
    workingSet.startAsync().awaitRunning();

    // Existing files are removed and a single new file is opened.
    assertThat(currentFileCount()).isEqualTo(1);
  }

  @Test
  public void testDrainerCompactsSparseFiles() throws Exception {
    // Add 10x "MAPPED_FILE_SIZE" number of entries. Each entry is at least 1 byte
    // so this should cause > 10 mapped file to be opened.
    for (int i = 0; i < 10 * MAPPED_FILE_SIZE; i++) {
      workingSet.add(withId(ITEM_1_T5, i));
    }

    int peakFileCount = currentFileCount();
    assertThat(peakFileCount).isGreaterThan(10);

    // Remove 90% of items evenly across all files
    for (int i = 0; i < 10 * MAPPED_FILE_SIZE; i++) {
      if (i % 10 != 0) {
        workingSet.get(i).release();
      }
    }

    // The drainer is configured to continue draining until file utilization is above X%
    // (currently set to 50%).
    //
    //   N = Peak file count (recorded above)
    //   M = Number of files drained
    //
    // We can simplify by normalizing the capacity of each file to 1. The used capacity of
    // each file is therefore 0.1 (we release 90% of items above). When we drain a file
    // we reduce total capacity by 0.9 (we remove a full file and re-write the remaining
    // items using 0.1 capacity). Total usage is 0.1*N and does not change since we are not
    // deleting any items in the process of draining.
    //
    // Thus, after draining M files we have a utilization of 0.1*N/(N - 0.9*M).
    //
    // We can set utilization to X and determine the number of files we need to drain,
    //
    //   0.1*N/(N - 0.9*M) = X
    //   0.1*N             = X*N - X*0.9*M
    //            X*0.9*M  = X*N - 0.1*N
    //                  M  = (X-0.1)*N / (X*0.9)
    //
    // The total number of files after draining M will be,
    //
    //   Files after drain = N-M+0.1*M = N-0.9*M
    //
    // Substituting M from above,
    //
    //   Files after drain = N-0.9*((X-0.1)*N / (X*0.9))
    //                     = N-((X-0.1)*N / X)
    //                     = (X*N-(X-0.1)*N) / X
    //                     = 0.1*N / X
    //                     = N / 10*X
    //
    // We allow a margin of error of +/- 1 to account for integer math.
    // In the current DiskBackedWorkingSet implementation X is hardcoded to 50%
    // Thus, the file count should now be (peakFileCount / 10*0.5) = peakFileCount/5.

    assertThat(currentFileCount()).isCloseTo(peakFileCount / 5, within(1));
  }

  @Test
  public void testItemRefValidAcrossDrain() throws Exception {
    // Add 10x "MAPPED_FILE_SIZE" number of entries. Each entry is at least 1 byte
    // so this should cause > 10 mapped file to be opened.
    for (int i = 0; i < 10 * MAPPED_FILE_SIZE; i++) {
      workingSet.add(withId(ITEM_1_T5, i));
    }

    int peakFileCount = currentFileCount();
    assertThat(peakFileCount).isGreaterThan(10);

    // This ref should still be valid after the drain we're about to trigger.
    var ref = workingSet.get(0);
    var item = ref.item();
    var key = ref.key();

    // Remove 90% of items evenly across all files
    for (int i = 0; i < 10 * MAPPED_FILE_SIZE; i++) {
      if (i % 10 != 0) {
        workingSet.get(i).release();
      }
    }

    assertThat(ref.key()).isEqualTo(key);
    var newItem = ref.item();
    assertThat(newItem).isEqualTo(item);
    ref.release();
    ReferenceCountUtil.release(item);
    ReferenceCountUtil.release(newItem);
  }

  private int currentFileCount() {
    var pattern = "*" + DiskBackedWorkingSet.FILE_SUFFIX;
    try (var children = Files.newDirectoryStream(dir.getRoot().toPath(), pattern)) {
      return Iterables.size(children);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  protected WorkingSet workingSet() {
    return workingSet;
  }
}
