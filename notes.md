
* Primary queue index is simple merge sorted collection
* Stable PK allocated on initial insert

Is a primary key index necessary?

278 bytes per message
1,000,000 messages

Raw size = 265MB
Log size = 329MB
Overhead = 64MB
Per Item = ~67 bytes

Ideas,
* Add an epoch to each segment and store deadlines as offsets
* Delta encode IDs and/or deadlines
* Store IDs as deltas from smallest ID



Enqueue deadlines must be in the future?
Deadline can never be _before_ the oldest dequeued item?

Segments have Entries and Tombstones

How do we know if a WAL entry is within an on-disk segment? Keep WALs and Segments 1-1.

If we assign monotonic IDs to segments then we just need to keep track of the
last segment ID in the WAL?

Each segment can have its own WAL.

*.log
*.idx

In-Memory Segments writes to its WAL
When full (atomically),
- Close old segment WAL
- Rename old segment WAL -> "0000000001.log_ro"
- Open new in-memory segment with new WAL
Write old segment IDX.
Remove old segment WAL.
Segment is frozen.

When freezing segment,
- Some items may have been dequeued
- Keep track of these IDs and skip over them after swapping the in-memory
  segment for the on-disk copy. The segment writer can keep track of the
  file offset for each item. The on-disk segment can then be opened to
  the appropriate offset.

On startup,

Re-execute rewrites of any "*.log_ro" files
- Read file into memory
- Freeze and write segment
- Remove *.log_ro file

Find "*.log" file (there should be at most one)
- Handle corruption at end
- Otherwise handle same as log_ro (i.e. freeze and remove)

Start new segment.

Should you ever see a rogue tombstone (without its corresponding entry?)
Tombstones are added to mark a pending item as deleted.
When a working item is requeued a tombstone is added for the old copy.
When a working item is released a tombstone is added for the item.
When a tombstone is _added_ the corresponding entry has already been seen.
We only need to read tombstones when,
- Loading a WAL
- Merging segments

Its possible we would see rogue tombstones when merging segments. For
example, if we have segments A, B and C and A contains entry "a" and B
contains tombstone "a", and we merge B and C then we will see the rogue
tombstone for "a".

A "dequeue" operation should never see a tombstone though, since we
must always be reading from a merged view of _all_ segments.

When freezing a segment,
- Write separate segments for pending and tombstones
-

# 2020-04-18

TODO,
- [DONE?] Reload state on startup
- [DONE?] Persist queue metadata (definitions, locations, ids)
- [DONE?] Persist ID generator information
- [DONE] Handle deadlines in the past (advance them as needed)
- [DONE] Implement queue size tracking
- [DONE] Add concurrency support
- [DONE] How do we merge segment headers when merging segments? We can't trivially
  combine pending & tombstone counts because they may cancel one another out.
    - Switched from header to footer (track metadata while writing)

- (NEXT) Write segment merger

- IO batching (put lock waiters into queue; lock acquirer operates on batch)
- Online segment merging
- Switch to more efficient segment and log encoding
- Do not keep working item values in memory (just IDs)
    - Add ID lookup index to segments?
- Requeue with new value?
- How do we avoid re-reading large numbers of segment entries to cancel out
  tombstones?

# 2020-04-26

Custom SEGMENT format

```
Entry:
  key          EntryKey(128)
 [Type:Pending]
  stats        Stats(128)
  value_length uint32
  value        bytes(value_length)

EntryKey(128):
  <reserved> bits(14)
  deadline   Timestamp(48)
  id         uint64
  type       bits(2) {00: Pending,
                      01: Reserved,
                      10: Reserved,
                      11: Tombstone}

Timestamp(48):
    48 millis

Stats(128):
  enqueue_time  Timestamp(48)
  requeue_time  Timestamp(48)
  dequeue_count uint32

Footer:
  pending_count   uint64
  tombstone_count uint64
  last_key        EntryKey(128)
  max_id          uint64
```

TODO,
* Clean up Encoding class

