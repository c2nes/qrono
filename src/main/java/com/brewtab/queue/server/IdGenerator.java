package com.brewtab.queue.server;

/**
 * Must never return 0!
 */
public interface IdGenerator {
  long generateId();

  /**
   * Advance the generator past the given ID. All subsequent calls to {@link #generateId()} will
   * return an ID larger than the given ID.
   */
  void advancePastId(long id);
}
