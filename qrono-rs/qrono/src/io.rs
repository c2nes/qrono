use bytes::{BufMut, BytesMut};
use std::fs::File;
use std::io;
use std::io::{BufReader, Read};
use std::net::TcpStream;
use std::ptr::slice_from_raw_parts_mut;

pub trait ReadBytesMutUninitialized: Read {
    fn read_bytes_mut(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
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

pub trait ReadVecUninitialized: Read {
    fn read_bytes(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
        let pos = buf.len();
        let cap = buf.capacity();
        let ptr = buf.as_mut_ptr();

        unsafe {
            let dst = slice_from_raw_parts_mut(ptr.add(pos), cap - pos);
            let res = self.read(&mut *dst);
            if let Ok(n) = &res {
                buf.set_len(pos + *n);
            }
            res
        }
    }
}

// impl ReadBufUninitialized for mio::net::TcpStream {}
impl ReadBytesMutUninitialized for TcpStream {}
impl ReadBytesMutUninitialized for File {}
impl ReadBytesMutUninitialized for BufReader<File> {}

impl ReadVecUninitialized for TcpStream {}
impl ReadVecUninitialized for File {}
impl ReadVecUninitialized for BufReader<File> {}
