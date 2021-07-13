package net.qrono.server.data;

import org.immutables.value.Value;

@Value.Immutable
public interface SegmentMetadata {

  long maxId();

  long pendingCount();

  long tombstoneCount();

}