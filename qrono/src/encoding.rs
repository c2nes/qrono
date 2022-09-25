use crate::bytes::Bytes;
use crate::data::{Entry, Item, Key, SegmentID, Stats, Timestamp, ID};

use bytes::{Buf, BufMut};

// [padding:14][48:deadline][64:id][2:kind]
pub const KEY_SIZE: usize = 16;

// [48:enqueue_time][48:requeue_time][32:dequeue_count]
pub const STATS_SIZE: usize = 16;

// [32:value length]
pub const VALUE_LEN_SIZE: usize = 4;

// [64:segment id]
pub const SEGMENT_ID_SIZE: usize = 8;

const PENDING: u8 = 0b00;
const TOMBSTONE: u8 = 0b11;

macro_rules! pack {
    ($packed:ident,$bits:literal,$val:expr) => {
        $packed = ($packed << $bits) | ($val as u128 & ((1 << $bits) - 1));
    };
}

macro_rules! unpack {
    ($packed:ident,$bits:literal,$cast:ty) => {{
        let s = <$cast>::BITS - $bits;
        // Using a pair of shifts instead of a bit mask ensures
        // correct sign extension for signed types.
        (($packed as $cast) << s) >> s
    }};
}

macro_rules! unpack_shift {
    ($packed:ident,$bits:literal,$cast:ty) => {{
        let r = unpack!($packed, $bits, $cast);
        $packed >>= $bits;
        r
    }};
}

pub trait Encoder: BufMut {
    fn put_key(&mut self, key: Key) {
        let kind = if key.is_pending() { PENDING } else { TOMBSTONE };
        let mut packed = 0;
        pack!(packed, 48, key.deadline().millis());
        pack!(packed, 64, key.id());
        pack!(packed, 2, kind);
        self.put_u128(packed);
    }

    fn put_stats(&mut self, stats: &Stats) {
        self.put_int(stats.enqueue_time.millis(), 6);
        self.put_int(stats.requeue_time.millis(), 6);
        self.put_u32(stats.dequeue_count);
    }

    fn put_value_len(&mut self, len: usize) {
        self.put_u32(len as u32);
    }

    fn put_value(&mut self, val: &[u8]) {
        self.put_value_len(val.len());
        self.put_slice(val);
    }

    fn put_entry(&mut self, entry: &Entry) {
        self.put_key(entry.key());
        self.put_u64(entry.segment_id());

        if let Entry::Pending(item) = entry {
            self.put_stats(&item.stats);
            self.put_value(&item.value);
        }
    }
}

impl<B: BufMut> Encoder for B {}

pub trait Decoder: Buf {
    fn get_key(&mut self) -> Key {
        let mut packed = self.get_u128();
        let kind = unpack_shift!(packed, 2, u8);
        let id = unpack_shift!(packed, 64, ID);
        let deadline_millis = unpack!(packed, 48, i64);
        let deadline = Timestamp::from_millis(deadline_millis);
        match kind as u8 {
            PENDING => Key::Pending { deadline, id },
            TOMBSTONE => Key::Tombstone { deadline, id },
            _ => panic!("unrecognized key type {kind:?}"),
        }
    }

    fn get_stats(&mut self) -> Stats {
        let enqueue_time = Timestamp::from_millis(self.get_int(6));
        let requeue_time = Timestamp::from_millis(self.get_int(6));
        let dequeue_count = self.get_u32();
        Stats {
            enqueue_time,
            requeue_time,
            dequeue_count,
        }
    }

    fn get_value_len(&mut self) -> usize {
        self.get_u32() as usize
    }

    fn get_value(&mut self) -> Bytes {
        let len = self.get_value_len();
        let chunk = self.chunk();
        if len <= chunk.len() {
            let res = Bytes::from(&chunk[..len]);
            self.advance(len);
            res
        } else {
            let mut remaining = len;
            let mut dst = Vec::with_capacity(len);
            while remaining > 0 {
                let chunk = self.chunk();
                let limit = usize::min(remaining, chunk.len());
                dst.copy_from_slice(&chunk[..limit]);
                self.advance(limit);
                remaining -= limit;
            }
            Bytes::from(dst)
        }
    }

    fn get_entry(&mut self) -> Entry {
        let key = self.get_key();
        let segment_id = self.get_u64() as SegmentID;
        match key {
            Key::Tombstone { id, deadline } => Entry::Tombstone {
                id,
                deadline,
                segment_id,
            },
            Key::Pending { id, deadline } => {
                let stats = self.get_stats();
                let value = self.get_value();
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
}

impl<B: Buf> Decoder for B {}

pub fn len(entry: &Entry) -> usize {
    match entry {
        Entry::Pending(Item { value, .. }) => {
            KEY_SIZE + SEGMENT_ID_SIZE + STATS_SIZE + VALUE_LEN_SIZE + value.len()
        }
        Entry::Tombstone { .. } => KEY_SIZE + SEGMENT_ID_SIZE,
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::Bytes;
    use crate::data::{Entry, Item, Key, Stats, Timestamp};
    use crate::encoding::{Decoder, Encoder};
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
        let mut buf = vec![];
        let entry = Entry::Pending(item());
        buf.put_entry(&entry);
        let actual = Cursor::new(&buf[..]).get_entry();
        assert_eq!(entry, actual);
        assert_eq!(13, actual.item().unwrap().value.len());
    }

    #[test]
    fn round_trip_tombstone() {
        let mut buf = vec![];
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
        let mut buf = vec![];
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
