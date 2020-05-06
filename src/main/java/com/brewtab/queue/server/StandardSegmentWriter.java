package com.brewtab.queue.server;

import com.brewtab.queue.server.data.Entry.Key;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.function.Supplier;

public class StandardSegmentWriter implements SegmentWriter {
  private final Path directory;

  public StandardSegmentWriter(Path directory) {
    this.directory = directory;
  }

  @Override
  public Opener write(
      SegmentName segmentName,
      Segment source,
      Supplier<Key> liveReaderOffset
  ) throws IOException {
    var path = SegmentFiles.getIndexPath(directory, segmentName);
    var tmpPath = SegmentFiles.getTemporaryPath(path);
    var offset = ImmutableSegment.write(tmpPath, source, liveReaderOffset);
    Files.move(tmpPath, path, StandardCopyOption.ATOMIC_MOVE);

    // TODO: Log how long it takes to do this
    return position -> {
      var segment = ImmutableSegment.open(path);
      if (position.compareTo(offset.getKey()) < 0) {
        // TODO: Log warning
      } else {
        segment.position(offset.getPosition());
      }
      while (segment.peek().compareTo(position) < 0) {
        segment.next();
      }
      return segment;
    };
  }
}
