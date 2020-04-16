package com.brewtab.queue.server;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;

public class MockClock extends Clock {
  private static final Instant DEFAULT_TIME = Instant.parse("2020-02-01T12:30:00Z");
  private Instant time;

  public MockClock() {
    this(DEFAULT_TIME);
  }

  public MockClock(Instant time) {
    this.time = time;
  }

  public MockClock set(Instant time) {
    this.time = time;
    return this;
  }

  public MockClock adjust(TemporalAmount adjustment) {
    time = time.plus(adjustment);
    return this;
  }

  public MockClock adjust(long amount, TemporalUnit unit) {
    return adjust(Duration.of(amount, unit));
  }

  public MockClock adjust(long amount, TimeUnit unit) {
    return adjust(Duration.of(amount, unit.toChronoUnit()));
  }

  @Override
  public ZoneId getZone() {
    return ZoneOffset.UTC;
  }

  @Override
  public Clock withZone(ZoneId zone) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Instant instant() {
    return time;
  }

  @Override
  public String toString() {
    return "MockClock{" +
        "time=" + time +
        '}';
  }
}
