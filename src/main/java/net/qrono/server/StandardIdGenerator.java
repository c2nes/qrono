package net.qrono.server;

import java.time.Clock;

public class StandardIdGenerator implements IdGenerator {
  // ID:       64 bits
  // Ticks:    38 bits, 16 ms step, offset from configured epoch, ~139 years range
  // Counter:  16 bits, rolls over every 16ms, ~4 million IDs/second
  // 38+16 = 54 (12 bits remaining)

  private static final int TICK_BITS = 38;
  private static final long TICK_MAX = (1L << TICK_BITS) - 1;
  private static final int COUNTER_BITS = 16;
  private static final long COUNTER_MAX = (1L << COUNTER_BITS) - 1;

  private static final int MILLIS_PER_TICK = 1 << 4;

  private final Clock clock;
  private final long epoch;

  private long lastTick;
  private long lastCounter;

  public StandardIdGenerator(long epoch, long lastId) {
    this(Clock.systemUTC(), epoch, lastId);
  }

  public StandardIdGenerator(Clock clock, long epoch, long lastId) {
    this.clock = clock;
    this.epoch = epoch;
    lastTick = lastId & COUNTER_MAX;
    lastCounter = (lastId >>> COUNTER_BITS) & TICK_MAX;
  }

  private long tick() {
    var tick = (clock.millis() - epoch) / MILLIS_PER_TICK;
    if (tick > TICK_MAX) {
      throw new IllegalStateException("tick overflow (this should take 43 years)");
    }
    return tick;
  }

  @Override
  public long generateId() {
    var tick = tick();
    long counter;

    synchronized (this) {
      if (tick > lastTick) {
        lastTick = tick;
        lastCounter = 0;
        counter = 0;
      } else if (lastCounter == COUNTER_MAX) {
        // TODO: Log warning about advancing counter
        tick = lastTick + 1;
        lastTick = tick;
        lastCounter = 0;
        counter = 0;
      } else {
        counter = lastCounter + 1;
        lastCounter = counter;
      }
    }

    return (tick << COUNTER_BITS) | counter;
  }
}
