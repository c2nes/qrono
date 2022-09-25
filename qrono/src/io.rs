use bytes::{BufMut, BytesMut};
use std::io;
use std::io::Read;
use std::ptr::slice_from_raw_parts_mut;

pub trait ReadInto<T>: Read {
    fn read_into(&mut self, dest: T) -> io::Result<usize>;

    fn read_exact_into(&mut self, dest: T) -> io::Result<()>;
}

impl<R: Read> ReadInto<&mut BytesMut> for R {
    fn read_into(&mut self, buf: &mut BytesMut) -> io::Result<usize> {
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

    fn read_exact_into(&mut self, buf: &mut BytesMut) -> io::Result<()> {
        let chunk = buf.chunk_mut();
        let len = chunk.len();
        let slice = slice_from_raw_parts_mut(chunk.as_mut_ptr(), chunk.len());
        unsafe {
            self.read_exact(&mut *slice)?;
            buf.advance_mut(len);
        };
        Ok(())
    }
}

impl<R: Read> ReadInto<&mut Vec<u8>> for R {
    fn read_into(&mut self, buf: &mut Vec<u8>) -> io::Result<usize> {
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

    fn read_exact_into(&mut self, buf: &mut Vec<u8>) -> io::Result<()> {
        let pos = buf.len();
        let cap = buf.capacity();
        let ptr = buf.as_mut_ptr();
        let len = cap - pos;

        unsafe {
            let dst = slice_from_raw_parts_mut(ptr.add(pos), len);
            self.read_exact(&mut *dst)?;
            buf.set_len(pos + len);
        }

        Ok(())
    }
}
