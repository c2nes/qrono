use crate::bytes::Bytes;
use crate::data::{Entry, Item, Key, SegmentID, Stats, Timestamp};
use crate::io::ReadVecUninitialized;
use bytes::{Buf, BufMut, BytesMut};
use std::io;
use std::io::Cursor;

// [48:millis]
const TIMESTAMP_SIZE: usize = 6;

// [48:enqueue_time][48:requeue_time][32:dequeue_count]
pub const STATS_SIZE: usize = TIMESTAMP_SIZE + TIMESTAMP_SIZE + 4;

const PENDING: u8 = 0b00;
const TOMBSTONE: u8 = 0b11;

pub fn put_key(buf: &mut BytesMut, key: Key) {
    let deadline = key.deadline().millis() as u128;
    let id = key.id() as u128;
    let entry_type = match key {
        Key::Pending { .. } => PENDING,
        Key::Tombstone { .. } => TOMBSTONE,
    };
    let packed: u128 = deadline << 66 | id << 2 | entry_type as u128;
    buf.put_u128(packed);
}

pub fn put_stats(buf: &mut BytesMut, stats: &Stats) {
    buf.put_int(stats.enqueue_time.millis(), 6);
    buf.put_int(stats.requeue_time.millis(), 6);
    buf.put_u32(stats.dequeue_count);
}

pub fn put_value(buf: &mut BytesMut, val: &[u8]) -> usize {
    buf.put_u32(val.len() as u32);
    buf.put_slice(val);
    4 + val.len()
}

pub fn put_entry(buf: &mut BytesMut, entry: &Entry) -> usize {
    let len_before = buf.len();
    put_key(buf, entry.key());
    buf.put_u64(entry.segment_id());

    if let Entry::Pending(item) = entry {
        put_stats(buf, &item.stats);
        put_value(buf, &item.value);
    }

    buf.len() - len_before
}

pub fn decode_key(buf: [u8; 16]) -> Key {
    let packed = u128::from_be_bytes(buf);
    let entry_type = (packed & 0b11) as u8;
    let id = ((packed >> 2) & ((1 << 64) - 1)) as u64;
    let timestamp = ((packed >> 66) & ((1 << 48) - 1)) as i64;
    let deadline = Timestamp::from_millis(timestamp);
    match entry_type {
        PENDING => Key::Pending { id, deadline },
        TOMBSTONE => Key::Tombstone { id, deadline },
        _ => panic!("unrecognized key type {:?}", entry_type),
    }
}

pub fn get_key<B: Buf>(buf: &mut B) -> Key {
    let mut bytes = [0u8; 16];
    buf.copy_to_slice(&mut bytes);
    decode_key(bytes)
}

pub fn get_stats<B: Buf>(buf: &mut B) -> Stats {
    let enqueue_time = Timestamp::from_millis(buf.get_int(6));
    let requeue_time = Timestamp::from_millis(buf.get_int(6));
    let dequeue_count = buf.get_u32();
    Stats {
        enqueue_time,
        requeue_time,
        dequeue_count,
    }
}

pub fn get_value(buf: &mut Cursor<&[u8]>) -> Bytes {
    let len = buf.get_u32() as usize;
    let pos = buf.position() as usize;
    let res = Bytes::from(&buf.get_ref()[pos..pos + len]);
    buf.advance(len);
    res
}

pub fn get_entry(buf: &mut Cursor<&[u8]>) -> Entry {
    let key = get_key(buf);
    let segment_id = buf.get_u64() as SegmentID;
    match key {
        Key::Tombstone { id, deadline } => Entry::Tombstone {
            id,
            deadline,
            segment_id,
        },
        Key::Pending { id, deadline } => {
            let stats = get_stats(buf);
            let value = get_value(buf);
            Entry::Pending(Item {
                id,
                deadline,
                stats,
                value,
                segment_id,
            })
        }
    }
}

pub fn read_entry_rest<R: ReadVecUninitialized>(
    src: &mut R,
    key: Key,
    segment_id: SegmentID,
) -> io::Result<Entry> {
    match key {
        Key::Tombstone { id, deadline } => Ok(Entry::Tombstone {
            id,
            deadline,
            segment_id,
        }),
        Key::Pending { id, deadline } => {
            let mut buf = [0; 16 + 4]; // 16 bytes for stats, 4 for value length
            src.read_exact(&mut buf)?;
            let mut buf = Cursor::new(&buf);
            let stats = get_stats(&mut buf);
            let mut len = buf.get_u32() as usize;
            let mut value = Vec::with_capacity(len);
            while len > 0 {
                len -= src.read_bytes(&mut value)?;
            }

            Ok(Entry::Pending(Item {
                id,
                deadline,
                stats,
                value: Bytes::from(value),
                segment_id,
            }))
        }
    }
}

pub fn len(entry: &Entry) -> usize {
    match entry {
        Entry::Pending(Item { value, .. }) => 44 + value.len(),
        Entry::Tombstone { .. } => 24,
    }
}

pub trait PutEntry: BufMut {
    fn put_entry(&mut self, entry: &Entry) -> usize;
}

impl PutEntry for BytesMut {
    fn put_entry(&mut self, entry: &Entry) -> usize {
        put_entry(self, entry)
    }
}

pub trait GetEntry: Buf {
    fn get_entry(&mut self) -> Entry;
}

impl GetEntry for Cursor<&[u8]> {
    fn get_entry(&mut self) -> Entry {
        get_entry(self)
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::Bytes;
    use crate::data::{Entry, Item, Key, Stats, Timestamp};
    use crate::encoding::{GetEntry, PutEntry};
    use bytes::BytesMut;
    use std::io::Cursor;
    use std::mem::size_of;

    fn item() -> Item {
        Item {
            id: 1000,
            deadline: Timestamp::from_millis(1),
            stats: Stats {
                enqueue_time: Timestamp::from_millis(2),
                requeue_time: Timestamp::from_millis(3),
                dequeue_count: 0,
            },
            value: Bytes::from("Hello, world!"),
            segment_id: 0,
        }
    }

    #[test]
    fn round_trip_pending() {
        let mut buf = BytesMut::new();
        let entry = Entry::Pending(item());
        buf.put_entry(&entry);
        let actual = Cursor::new(&buf[..]).get_entry();
        assert_eq!(entry, actual);
        assert_eq!(13, actual.item().unwrap().value.len());
    }

    #[test]
    fn round_trip_tombstone() {
        let mut buf = BytesMut::new();
        let item = item();
        let entry = Entry::Tombstone {
            id: item.id,
            deadline: item.deadline,
            segment_id: 0,
        };
        buf.put_entry(&entry);
        let actual = Cursor::new(&buf[..]).get_entry();
        assert_eq!(entry, actual);
        assert!(actual.item().is_none());
    }

    #[test]
    fn round_trip_empty_value() {
        let mut buf = BytesMut::new();
        let mut item = item();
        item.value = Bytes::empty();
        let entry = Entry::Pending(item);

        buf.put_entry(&entry);
        let actual = Cursor::new(&buf[..]).get_entry();

        assert_eq!(entry, actual);
        assert_eq!(0, actual.item().unwrap().value.len());
    }

    #[test]
    fn key_size() {
        println!("Key={}, Entry={}", size_of::<Key>(), size_of::<Entry>())
    }
}
