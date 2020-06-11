package com.brewtab.queue.server;

import org.junit.After;
import org.junit.Before;

public class InMemoryWorkingSetTest extends WorkingSetTestBase {
  private InMemoryWorkingSet workingSet;

  @Before
  public void setUp() {
    workingSet = new InMemoryWorkingSet();
  }

  @After
  public void tearDown() {
    workingSet = null;
  }

  @Override
  protected WorkingSet workingSet() {
    return workingSet;
  }
}