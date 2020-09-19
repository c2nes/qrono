package net.qrono.server;

import net.qrono.server.data.Entry.Key;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

public class StandardSegmentWriter implements SegmentWriter {
  private final Path directory;

  public StandardSegmentWriter(Path directory) {
    this.directory = directory;
  }

  @Override
  public Segment write(
      SegmentName segmentName,
      SegmentReader source,
      Supplier<Key> liveReaderOffset
  ) throws IOException {
    var path = SegmentFiles.getIndexPath(directory, segmentName);
    return ImmutableSegment.write(path, source, liveReaderOffset);
  }
}
