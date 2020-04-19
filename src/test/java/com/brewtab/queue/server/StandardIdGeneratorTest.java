package com.brewtab.queue.server;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class StandardIdGeneratorTest {

  @Test
  public void incrementCounter() {
    var clock = new MockClock(Instant.ofEpochMilli(0x40_12_34_56));
    var generator = new StandardIdGenerator(clock, 0x40_00_00_00, 0);
    assertEquals(0x1_23_45_00_00L, generator.generateId());
    assertEquals(0x1_23_45_00_01L, generator.generateId());
    assertEquals(0x1_23_45_00_02L, generator.generateId());
  }

  @Test
  public void doesNotReturnZero() {
    var clock = new MockClock(Instant.ofEpochMilli(0));
    var generator = new StandardIdGenerator(clock, 0, 0);
    assertEquals(0x1, generator.generateId());
    assertEquals(0x2, generator.generateId());
    clock.adjust(0x10, TimeUnit.MILLISECONDS);
    assertEquals(0x1_00_00, generator.generateId());
    assertEquals(0x1_00_01, generator.generateId());
  }

  @Test
  public void counterResetOnNextTick() {
    var clock = new MockClock(Instant.ofEpochMilli(0x40_12_34_56));
    var generator = new StandardIdGenerator(clock, 0x40_00_00_00, 0);
    assertEquals(0x1_23_45_00_00L, generator.generateId());
    clock.adjust(0x10, TimeUnit.MILLISECONDS);
    assertEquals(0x1_23_46_00_00L, generator.generateId());
  }

  @Test
  public void counterNotResetBetweenTicks() {
    var clock = new MockClock(Instant.ofEpochMilli(0x40_12_34_56));
    var generator = new StandardIdGenerator(clock, 0x40_00_00_00, 0);
    assertEquals(0x1_23_45_00_00L, generator.generateId());
    clock.adjust(0x6, TimeUnit.MILLISECONDS);
    assertEquals(0x1_23_45_00_01L, generator.generateId());
  }

  @Test
  public void counterOverflowsToTicks() {
    var clock = new MockClock(Instant.ofEpochMilli(0x40_12_34_56));
    var generator = new StandardIdGenerator(clock, 0x40_00_00_00, 0);
    for (var i = 0; i < 0xff_ff; i++) {
      generator.generateId();
    }
    assertEquals(0x1_23_45_ff_ffL, generator.generateId());
    assertEquals(0x1_23_46_00_00L, generator.generateId());
  }
}
