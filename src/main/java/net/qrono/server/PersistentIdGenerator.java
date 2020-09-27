package net.qrono.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A sequential ID generator that persists its state to a file.
 */
public class PersistentIdGenerator extends AbstractExecutionThreadService implements IdGenerator {

  private final Path stateFilePath;
  // Number of IDs to reserve when renewing
  private final long reservationSize;
  // Number of remaining reserved IDs below which a renewal is triggered
  private final long renewalThreshold;

  private volatile long last = 0L;
  private long reserved = 0L;

  private final Lock lock = new ReentrantLock();
  private final Condition renewalRequired = lock.newCondition();
  private final Condition idsAvailable = lock.newCondition();

  public PersistentIdGenerator(Path stateFilePath) {
    this(stateFilePath, 1_000_000, 500_000);
  }

  @VisibleForTesting
  PersistentIdGenerator(Path stateFilePath, long reservationSize, long renewalThreshold) {
    Preconditions.checkArgument(renewalThreshold <= reservationSize);
    this.stateFilePath = stateFilePath;
    this.reservationSize = reservationSize;
    this.renewalThreshold = renewalThreshold;
  }

  @Override
  protected void startUp() throws Exception {
    if (Files.exists(stateFilePath)) {
      var s = Files.readString(stateFilePath);
      last = Long.parseLong(s);
    }
    renew();
  }

  // lock most be held
  private boolean isRenewalRequired() {
    return available() < renewalThreshold;
  }

  private void awaitRenewalRequired() {
    lock.lock();
    try {
      while (isRunning() && !isRenewalRequired()) {
        renewalRequired.awaitUninterruptibly();
      }
    } finally {
      lock.unlock();
    }
  }

  private void renew() throws IOException {
    var newReserved = last + reservationSize;
    var tmpPath = stateFilePath.resolveSibling(stateFilePath.getFileName() + ".tmp");
    Files.writeString(tmpPath, Long.toString(newReserved));
    Files.move(tmpPath, stateFilePath, StandardCopyOption.ATOMIC_MOVE);

    lock.lock();
    try {
      reserved = newReserved;
      if (last < reserved) {
        idsAvailable.signalAll();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      awaitRenewalRequired();

      if (isRunning()) {
        renew();
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    lock.lock();
    try {
      renewalRequired.signal();
    } finally {
      lock.unlock();
    }
  }

  private long available() {
    return reserved - last;
  }

  @Override
  public long generateId() {
    lock.lock();
    try {
      while (last >= reserved) {
        idsAvailable.awaitUninterruptibly();
      }

      var id = last + 1;
      last = id;

      if (isRenewalRequired()) {
        renewalRequired.signal();
      }

      return id;
    } finally {
      lock.unlock();
    }
  }
}
