package com.brewtab.queue.server;

import java.io.IOException;
import java.nio.file.Path;

public class QueueFactory {
  private final Path directory;
  private final IdGenerator idGenerator;
  private final SegmentFreezer segmentFreezer;

  public QueueFactory(Path directory, IdGenerator idGenerator, SegmentFreezer segmentFreezer) {
    this.directory = directory;
    this.idGenerator = idGenerator;
    this.segmentFreezer = segmentFreezer;
  }

  public Queue createQueue(String name) throws IOException {
    return new Queue(name, idGenerator, directory.resolve(name), segmentFreezer);
  }

  public Queue createQueue(String name) throws IOException {

  }
}
