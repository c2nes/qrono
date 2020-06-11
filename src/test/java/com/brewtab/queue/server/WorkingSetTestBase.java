package com.brewtab.queue.server;

import static com.brewtab.queue.server.TestData.ITEM_1_T5;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.brewtab.queue.server.data.Entry;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public abstract class WorkingSetTestBase {
  protected abstract WorkingSet workingSet();
  
  @Test
  public void testAdd() throws IOException {
    workingSet().add(ITEM_1_T5);
    Assert.assertEquals(1, workingSet().size());
    assertNotNull(workingSet().get(1001));
  }

  @Test
  public void testAdd_afterRemovingLastItem() throws IOException {
    workingSet().add(ITEM_1_T5);
    Assert.assertEquals(1, workingSet().size());

    workingSet().get(1001).release();
    Assert.assertEquals(0, workingSet().size());

    // Re-add item
    workingSet().add(ITEM_1_T5);
    Assert.assertEquals(1, workingSet().size());
  }

  @Test
  public void testAdd_duplicate() throws IOException {
    workingSet().add(ITEM_1_T5);
    assertThatThrownBy(() -> workingSet().add(ITEM_1_T5))
        .isInstanceOf(IllegalStateException.class);
    Assert.assertEquals(1, workingSet().size());
  }

  @Test
  public void testGet_missingEntry() throws IOException {
    assertNull(workingSet().get(0xDEADBEEF));
  }

  @Test
  public void testGetKey() throws IOException {
    workingSet().add(ITEM_1_T5);
    var itemRef = workingSet().get(1001);
    assertNotNull(itemRef);
    Assert.assertEquals(Entry.newTombstoneKey(ITEM_1_T5), itemRef.key());
  }

  @Test
  public void testGetKey_afterRelease() throws IOException {
    workingSet().add(ITEM_1_T5);
    var itemRef = workingSet().get(1001);
    assertNotNull(itemRef);
    itemRef.release();

    assertThatThrownBy(itemRef::key)
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testGetItem() throws IOException {
    workingSet().add(ITEM_1_T5);
    var itemRef = workingSet().get(1001);
    assertNotNull(itemRef);
    Assert.assertEquals(ITEM_1_T5, itemRef.item());
  }

  @Test
  public void testGetItem_afterRelease() throws IOException {
    workingSet().add(ITEM_1_T5);
    var itemRef = workingSet().get(1001);
    assertNotNull(itemRef);
    itemRef.release();

    assertThatThrownBy(itemRef::item)
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void testRelease() throws IOException {
    workingSet().add(ITEM_1_T5);
    var itemRef = workingSet().get(1001);
    assertNotNull(itemRef);

    // Silence is success
    itemRef.release();
  }

  @Test
  public void testRelease_twice() throws IOException {
    workingSet().add(ITEM_1_T5);
    var itemRef = workingSet().get(1001);
    assertNotNull(itemRef);

    // Silence is success
    itemRef.release();

    assertThatThrownBy(itemRef::item)
        .isInstanceOf(IllegalStateException.class);
  }
}
