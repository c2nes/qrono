use crate::data::Entry;
use crate::encoding;
use crate::encoding::{Decoder, Encoder};
use crate::hash::murmur3;
use crate::segment::MemorySegment;
use bytes::{Buf, BufMut};
use io::ErrorKind::UnexpectedEof;
use log::trace;
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Error, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use std::{fs, io};

pub struct WriteAheadLog {
    file: Option<File>,
    path: PathBuf,
    last_sync: Instant,
    sync_period: Option<Duration>,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(path: P, sync_period: Option<Duration>) -> WriteAheadLog {
        let last_sync = Instant::now();
        let path = path.as_ref().to_path_buf();
        WriteAheadLog {
            file: None,
            path,
            last_sync,
            sync_period,
        }
    }

    pub fn read<P: AsRef<Path>>(path: P) -> Result<MemorySegment, ReadError> {
        let mut segment = MemorySegment::new(None);
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut buf = vec![0; 8 * 1024];
        loop {
            let mut len_buf = [0; 4];
            let n = {
                let mut pos = 0;
                while pos < len_buf.len() {
                    let n = reader.read(&mut len_buf[pos..])?;
                    if n == 0 {
                        break;
                    }
                    pos += n;
                }
                pos
            };
            if n == 0 {
                break;
            }
            if n < 4 {
                return Err(ReadError::Truncated);
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            if len > buf.len() {
                buf.resize(len, 0)
            }
            reader.read_exact(&mut buf[..len])?;
            let computed_xsum = murmur3(&buf[..len], 0);
            let mut xsum_buf = [0; 4];
            reader.read_exact(&mut xsum_buf)?;
            let actual_xsum = u32::from_be_bytes(xsum_buf);
            if computed_xsum != actual_xsum {
                return Err(ReadError::Checksum);
            }

            let mut cursor = Cursor::new(&buf[..len]);
            while cursor.has_remaining() {
                segment.add(cursor.get_entry()).unwrap();
            }
        }
        Ok(segment)
    }

    pub fn append(&mut self, entries: &[Entry]) -> io::Result<()> {
        // <entries size in bytes "N">     4 bytes
        // <entries...>                    N bytes
        // <block checksum>                4 bytes
        let size: usize = entries.iter().map(encoding::len).sum();
        let mut buf = Vec::with_capacity(size + 8);
        buf.put_u32(size as u32);
        for entry in entries {
            buf.put_entry(entry);
        }
        buf.put_u32(murmur3(&buf[4..], 0));

        if self.file.is_none() {
            self.file = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&self.path)?,
            );
        }

        self.file.as_mut().unwrap().write_all(&buf)?;

        if let Some(sync_period) = self.sync_period {
            if sync_period == Duration::ZERO || self.last_sync.elapsed() > sync_period {
                self.sync()?;
            }
        }
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        if let Some(file) = &self.file {
            if self.sync_period.is_some() {
                let start = Instant::now();
                file.sync_data()?;
                let end = Instant::now();
                trace!("sync_data completed in {:?}", end - start);
                self.last_sync = end;
            }
        }

        Ok(())
    }

    pub fn delete(mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            drop(file);
            fs::remove_file(&self.path)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub enum ReadError {
    Truncated,
    Checksum,
    IO(io::Error),
}

impl From<io::Error> for ReadError {
    fn from(err: Error) -> Self {
        match err.kind() {
            UnexpectedEof => ReadError::Truncated,
            _ => ReadError::IO(err),
        }
    }
}

impl Display for ReadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl std::error::Error for ReadError {}

#[cfg(test)]
mod tests {
    use super::WriteAheadLog;
    use crate::data::generator::EntryGenerator;
    use crate::data::{Entry, Item};
    use crate::wal::ReadError;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use std::fs::File;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempdir;

    #[test]
    pub fn append_default_pending() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = WriteAheadLog::new(&path, None);
        let pending = Entry::Pending(Item {
            id: 0,
            deadline: Default::default(),
            stats: Default::default(),
            value: Default::default(),
            segment_id: 0,
        });

        wal.append(&[pending.clone()]).unwrap();
        drop(wal);

        let entries = WriteAheadLog::read(&path).unwrap().freeze().0.entries();
        assert_eq!(&entries, &[pending]);
    }

    #[test]
    pub fn append_default_tombstone() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = WriteAheadLog::new(&path, None);
        let tombstone = Entry::Tombstone {
            id: 0,
            deadline: Default::default(),
            segment_id: 0,
        };

        wal.append(&[tombstone.clone()]).unwrap();
        drop(wal);

        let entries = WriteAheadLog::read(&path).unwrap().freeze().0.entries();
        assert_eq!(&entries, &[tombstone]);
    }

    #[test]
    pub fn append_empty() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        let mut wal = WriteAheadLog::new(&path, None);
        wal.append(&[]).unwrap();

        let entries = WriteAheadLog::read(&path).unwrap().freeze().0.entries();
        assert_eq!(&entries, &[]);
    }

    #[test]
    pub fn append_many() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        let mut wal = WriteAheadLog::new(&path, None);
        let mut gen = EntryGenerator::default();
        let mut rng = ChaCha20Rng::from_seed([0; 32]);
        let mut expected = vec![];

        for _ in 0..200 {
            let batch_size_base = [1, 10, 100, 500].choose(&mut rng).unwrap();
            let jitter = batch_size_base / 2;
            let batch_size = rng.gen_range(batch_size_base - jitter..=batch_size_base + jitter);
            let batch = (0..batch_size).map(|_| gen.entry()).collect::<Vec<_>>();
            wal.append(&batch).unwrap();
            expected.extend(batch);
        }
        drop(wal);

        let entries = WriteAheadLog::read(&path).unwrap().freeze().0.entries();
        expected.sort();
        assert_eq!(&entries, &expected, "entries do not match");
    }

    #[test]
    pub fn read_truncated() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        let mut wal = WriteAheadLog::new(&path, None);
        let mut gen = EntryGenerator::default();

        for _ in 0..100 {
            let entry = gen.pending();
            wal.append(&[entry.clone()]).unwrap();
        }
        drop(wal);

        let mut wal = File::options().write(true).open(&path).unwrap();
        let new_len = wal.seek(SeekFrom::End(-1)).unwrap();
        wal.set_len(new_len).unwrap();
        drop(wal);

        assert!(matches!(
            WriteAheadLog::read(&path),
            Err(ReadError::Truncated)
        ));
    }

    #[test]
    pub fn read_corrupted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        let mut wal = WriteAheadLog::new(&path, None);
        let mut gen = EntryGenerator::default();

        for _ in 0..100 {
            let entry = gen.pending();
            wal.append(&[entry.clone()]).unwrap();
        }
        drop(wal);

        // Overwrite the checksum
        let mut wal = File::options().write(true).open(&path).unwrap();
        wal.seek(SeekFrom::End(-4)).unwrap();
        wal.write_all(&[0, 0, 0, 0]).unwrap();
        drop(wal);

        assert!(matches!(
            WriteAheadLog::read(&path),
            Err(ReadError::Checksum)
        ));
    }
}
