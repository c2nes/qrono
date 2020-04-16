package com.brewtab.queue.server;

import com.google.protobuf.Message;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;

public class WriteAheadLog {
  private final Path path;
  private final Duration fsyncInterval;
  private FileOutputStream out;
  private Instant fsyncDeadline;

  public WriteAheadLog(Path path) throws IOException {
    this(path, Duration.ofSeconds(1));
  }

  public WriteAheadLog(Path path, Duration fsyncInterval) throws IOException {
    this.path = path;
    this.fsyncInterval = fsyncInterval;
    out = new FileOutputStream(path.toFile());
    fsyncDeadline = Instant.now().plus(fsyncInterval);
  }

  public void write(Message message) throws IOException {
    message.writeDelimitedTo(out);
    Instant now = Instant.now();
    if (now.isAfter(fsyncDeadline)) {
      out.flush();
      out.getFD().sync();
      fsyncDeadline = now.plus(fsyncInterval);
      System.out.println("LogSync: " + Duration.between(now, Instant.now()));
    }
  }
}
