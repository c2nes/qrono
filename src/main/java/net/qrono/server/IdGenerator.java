package net.qrono.server;

/**
 * Must never return 0!
 */
public interface IdGenerator {
  long generateId();
}
