use bytes::{BufMut, BytesMut};
use std::fs::File;
use std::io;
use std::io::{BufReader, Read};
use std::net::TcpStream;
use std::ptr::slice_from_raw_parts_mut;

pub trait ReadBufUninitialized: Read {
    fn read_buf(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
        let chunk = buf.chunk_mut();
        let slice = slice_from_raw_parts_mut(chunk.as_mut_ptr(), chunk.len());
        let ret = unsafe { self.read(&mut *slice) };

        if let Ok(n) = ret {
            unsafe {
                buf.advance_mut(n);
            }
        }

        ret
    }
}

impl ReadBufUninitialized for mio::net::TcpStream {}
impl ReadBufUninitialized for TcpStream {}
impl ReadBufUninitialized for File {}
impl ReadBufUninitialized for BufReader<File> {}
