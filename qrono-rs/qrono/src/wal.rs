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
    file: File,
    path: PathBuf,
    last_sync: Instant,
    sync_period: Option<Duration>,
}

impl WriteAheadLog {
    pub fn new<P: AsRef<Path>>(
        path: P,
        sync_period: Option<Duration>,
    ) -> io::Result<WriteAheadLog> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let last_sync = Instant::now();
        let path = path.as_ref().to_path_buf();
        Ok(WriteAheadLog {
            file,
            path,
            last_sync,
            sync_period,
        })
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

        self.file.write_all(&buf)?;

        if let Some(sync_period) = self.sync_period {
            if sync_period == Duration::ZERO || self.last_sync.elapsed() > sync_period {
                self.sync()?;
            }
        }
        Ok(())
    }

    pub fn sync(&mut self) -> io::Result<()> {
        if self.sync_period.is_some() {
            let start = Instant::now();
            self.file.sync_all()?;
            let end = Instant::now();
            trace!("sync_all completed in {:?}", end - start);
            self.last_sync = end;
        }

        Ok(())
    }

    pub fn delete(self) -> io::Result<()> {
        drop(self.file);
        fs::remove_file(&self.path)
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
