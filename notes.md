
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
- Reload state on startup
- Persist queue metadata (definitions, locations, ids)
- Persist ID generator information
- Handle deadlines in the past (advance them as needed)
- Online segment merging
