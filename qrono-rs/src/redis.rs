use std::num::ParseIntError;
use std::ops::{Range, RangeInclusive};
use std::str;
use std::str::FromStr;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

use crate::redis::Error::{Incomplete, ProtocolError};
use crate::redis::Value::{Integer, SimpleString};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use num::integer::div_rem;

#[derive(Debug)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Bytes),
    NullBulkString,
    Array(Vec<Value>),
    NullArray,
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    ProtocolError(String),
}

impl From<Error> for Value {
    fn from(err: Error) -> Self {
        match err {
            ProtocolError(s) => Value::Error(s),
            Incomplete => panic!("Error::Incomplete is an internal-only error"),
        }
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Self {
        ProtocolError("invalid utf8".to_owned())
    }
}

impl From<Utf8Error> for Error {
    fn from(_: Utf8Error) -> Self {
        ProtocolError("invalid utf8".to_owned())
    }
}

impl From<ParseIntError> for Error {
    fn from(_: ParseIntError) -> Self {
        ProtocolError("invalid integer".to_owned())
    }
}

struct ValueParser<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Buf for ValueParser<'a> {
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn chunk(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    fn advance(&mut self, cnt: usize) {
        self.pos += cnt;
    }
}

impl<'a> ValueParser<'a> {
    fn read_value(&mut self) -> Result<Value, Error> {
        if self.remaining() == 0 {
            return Err(Incomplete);
        }

        match self.get_u8() as char {
            '+' => self.read_simple_string(),
            '-' => self.read_error(),
            ':' => self.read_integer(),
            '$' => self.read_bulk_string(),
            '*' => self.read_array(),
            c => Err(ProtocolError(format!("unexpected character, {:?}", c))),
        }
    }

    fn read_simple_string(&mut self) -> Result<Value, Error> {
        let line = self.read_line()?;
        Ok(SimpleString(line.to_owned()))
    }

    fn read_error(&mut self) -> Result<Value, Error> {
        let line = self.read_line()?;
        Ok(Value::Error(line.to_owned()))
    }

    fn read_integer(&mut self) -> Result<Value, Error> {
        let line = self.read_line()?;
        Ok(Integer(i64::from_str(line)?))
    }

    fn read_bulk_string(&mut self) -> Result<Value, Error> {
        let len = self.read_length()?;
        if len < 0 {
            return Ok(Value::NullBulkString);
        }
        let len = len as usize;
        if self.remaining() < len + 2 {
            return Err(Incomplete);
        }
        let range = self.read_slice(len);
        let value = Bytes::copy_from_slice(&self.buf[range]);
        self.read_crlf()?;
        Ok(Value::BulkString(value))
    }

    fn read_array(&mut self) -> Result<Value, Error> {
        let len = self.read_length()?;
        if len < 0 {
            return Ok(Value::NullArray);
        }
        let len = len as usize;
        let mut array = Vec::with_capacity(len);
        for _ in 0..len {
            array.push(match self.read_value() {
                Ok(val) => val,
                err @ Err(_) => return err,
            })
        }
        Ok(Value::Array(array))
    }

    fn read_line(&mut self) -> Result<&str, Error> {
        for idx in 0..self.remaining() {
            if self.buf[self.pos + idx] == b'\n' {
                let len = idx - 1;
                let range = self.read_slice(len);
                self.read_crlf()?;
                return Ok(str::from_utf8(&self.buf[range])?);
            }
        }
        Err(Incomplete)
    }

    fn read_length(&mut self) -> Result<i64, Error> {
        for idx in 0..self.remaining() {
            if self.buf[self.pos + idx] == b'\n' {
                let len = idx - 1;
                let start = self.pos;
                let end = start + len;
                self.advance(len);
                self.read_crlf()?;
                return parse_signed(&self.buf[start..end]);
            }
        }
        Err(Incomplete)
    }

    #[inline(always)]
    fn read_slice(&mut self, len: usize) -> Range<usize> {
        let start = self.pos;
        let end = start + len;
        self.advance(len);
        start..end
    }

    #[inline(always)]
    fn read_crlf(&mut self) -> Result<(), Error> {
        if self.get_u8() != b'\r' || self.get_u8() != b'\n' {
            return Err(ProtocolError(String::from("missing line terminator")));
        }

        Ok(())
    }
}

struct ValueChecker<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Buf for ValueChecker<'a> {
    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn chunk(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    fn advance(&mut self, cnt: usize) {
        self.pos += cnt;
    }
}

impl<'a> ValueChecker<'a> {
    fn read_value(&mut self) -> Result<(), Error> {
        if self.remaining() == 0 {
            return Err(Incomplete);
        }

        match self.get_u8() as char {
            '+' => self.read_simple_string(),
            '-' => self.read_error(),
            ':' => self.read_integer(),
            '$' => self.read_bulk_string(),
            '*' => self.read_array(),
            c => Err(ProtocolError(format!("unexpected character, {:?}", c))),
        }
    }

    fn read_simple_string(&mut self) -> Result<(), Error> {
        self.read_line()?;
        Ok(())
    }

    fn read_error(&mut self) -> Result<(), Error> {
        self.read_line()?;
        Ok(())
    }

    fn read_integer(&mut self) -> Result<(), Error> {
        let line = self.read_line()?;
        i64::from_str(line)?;
        Ok(())
    }

    fn read_bulk_string(&mut self) -> Result<(), Error> {
        let len = self.read_length()?;
        if len < 0 {
            return Ok(());
        }
        let len = len as usize;
        if self.remaining() < len + 2 {
            return Err(Incomplete);
        }
        self.advance(len);
        self.read_crlf()?;
        Ok(())
    }

    fn read_array(&mut self) -> Result<(), Error> {
        let len = self.read_length()?;
        if len < 0 {
            return Ok(());
        }
        let len = len as usize;
        for _ in 0..len {
            self.read_value()?
        }
        Ok(())
    }

    fn read_line(&mut self) -> Result<&str, Error> {
        for idx in 0..self.remaining() {
            if self.buf[self.pos + idx] == b'\n' {
                let len = idx - 1;
                let start = self.pos;
                let end = start + len;
                self.advance(len);
                self.read_crlf()?;
                return Ok(str::from_utf8(&self.buf[start..end])?);
            }
        }
        Err(Incomplete)
    }

    fn read_length(&mut self) -> Result<i64, Error> {
        for idx in 0..self.remaining() {
            if self.buf[self.pos + idx] == b'\n' {
                let len = idx - 1;
                let start = self.pos;
                let end = start + len;
                self.advance(len);
                self.read_crlf()?;
                return parse_signed(&self.buf[start..end]);
            }
        }
        Err(Incomplete)
    }

    #[inline(always)]
    fn read_crlf(&mut self) -> Result<(), Error> {
        if self.get_u8() != b'\r' || self.get_u8() != b'\n' {
            return Err(ProtocolError(String::from("missing line terminator")));
        }

        Ok(())
    }
}

impl Value {
    pub fn check(buf: &[u8]) -> Result<usize, Error> {
        let mut checker = ValueChecker { buf, pos: 0 };
        checker.read_value()?;
        Ok(checker.pos)
    }

    pub fn from_bytes(buf: &[u8]) -> Result<(Value, usize), Error> {
        let mut parser = ValueParser { buf, pos: 0 };
        let value = parser.read_value()?;
        Ok((value, parser.pos))
    }

    pub fn from_buf(buf: Bytes) -> Result<(Value, usize), Error> {
        let mut parser = ValueParser { buf: &buf, pos: 0 };
        let value = parser.read_value()?;
        Ok((value, parser.pos))
    }

    fn put(&self, mut buf: &mut BytesMut) {
        match self {
            Value::SimpleString(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Value::Error(s) => {
                buf.put_u8(b'-');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Value::Integer(n) => {
                buf.put_u8(b':');
                put_i64(&mut buf, *n);
                buf.put_slice(b"\r\n");
            }
            Value::BulkString(b) => {
                buf.put_u8(b'$');
                put_usize(&mut buf, b.len());
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            Value::Array(vals) => {
                buf.put_u8(b'*');
                put_usize(&mut buf, vals.len());
                buf.put_slice(b"\r\n");
                for val in vals {
                    val.put(buf);
                }
            }
            Value::NullBulkString => buf.put_slice(b"$-1\r\n"),
            Value::NullArray => buf.put_slice(b"*-1\r\n"),
        }
    }

    pub fn encoded_length(&self) -> usize {
        match self {
            Value::SimpleString(s) => s.len() + 3, // +<s>\r\n
            Value::Error(s) => s.len() + 3,        // -<s>\r\n
            Value::Integer(n) => i64_len(*n) + 3,  // :<n>\r\n
            Value::BulkString(b) => usize_len(b.len()) + b.len() + 5, // $<len>\r\n<b>\r\n
            Value::Array(els) => {
                let mut size = usize_len(els.len()) + 3; // *<len>\r\n[elements...]
                for el in els {
                    size += el.encoded_length();
                }
                size
            }
            Value::NullArray => 5,      // *-1\r\n
            Value::NullBulkString => 5, // $-1\r\n
        }
    }
}

pub trait PutValue {
    fn put_redis_value(&mut self, value: Value);
}

impl PutValue for BytesMut {
    fn put_redis_value(&mut self, value: Value) {
        self.reserve(value.encoded_length());
        value.put(self);
    }
}

fn i64_len(mut n: i64) -> usize {
    if n >= 0 {
        if n < 10 {
            return 1;
        }
        if n < 100 {
            return 2;
        }
        if n < 1000 {
            return 3;
        }
        if n < 10000 {
            return 4;
        }
        if n < 100000 {
            return 5;
        }
        if n < 1000000 {
            return 6;
        }
    }
    let mut len = 1;
    if n < 0 {
        len += 1;
        n *= -1;
    }
    while n >= 10 {
        n /= 10;
        len += 1;
    }
    len
}

fn usize_len(mut n: usize) -> usize {
    if n < 10 {
        return 1;
    }
    if n < 100 {
        return 2;
    }
    if n < 1000 {
        return 3;
    }
    if n < 10000 {
        return 4;
    }
    if n < 100000 {
        return 5;
    }
    if n < 1000000 {
        return 6;
    }

    let mut len = 1;
    while n >= 10 {
        n /= 10;
        len += 1;
    }
    len
}

fn put_usize(buf: &mut BytesMut, mut n: usize) {
    let start = buf.len();
    while n >= 10 {
        let (q, r) = div_rem(n, 10);
        buf.put_u8(r as u8 + b'0');
        n = q;
    }
    buf.put_u8(n as u8 + b'0');
    let len = buf.len() - start;
    if len > 1 {
        buf[start..].reverse();
    }
}

fn put_i64(buf: &mut BytesMut, mut n: i64) {
    if n < 0 {
        buf.put_u8(b'-');
        n = -n;
    }

    let start = buf.len();
    while n >= 10 {
        let (q, r) = div_rem(n, 10);
        buf.put_u8(r as u8 + b'0');
        n = q;
    }
    buf.put_u8(n as u8 + b'0');
    let len = buf.len() - start;
    if len > 1 {
        buf[start..].reverse();
    }
}

pub fn parse_signed(src: &[u8]) -> Result<i64, Error> {
    let (sign, src) = if src.len() > 1 && src[0] == b'-' {
        (-1, &src[1..])
    } else {
        (1, src)
    };

    if src.is_empty() {
        return Err(Error::ProtocolError("empty number".to_string()));
    }

    const DIGITS: RangeInclusive<u8> = b'0'..=b'9';
    let mut scale = 0;
    for c in src {
        if !DIGITS.contains(c) {
            return Err(Error::ProtocolError("invalid digit".to_string()));
        }
        let digit = (*c - b'0') as i64;
        scale = 10 * scale + digit;
    }

    Ok(sign * scale)
}

pub fn parse_unsigned(src: &[u8]) -> Result<u64, Error> {
    if src.is_empty() {
        return Err(Error::ProtocolError("empty number".to_string()));
    }

    const DIGITS: RangeInclusive<u8> = b'0'..=b'9';
    let mut scale = 0;
    for c in src {
        if !DIGITS.contains(c) {
            return Err(Error::ProtocolError("invalid digit".to_string()));
        }
        let digit = (*c - b'0') as u64;
        scale = 10 * scale + digit;
    }

    Ok(scale)
}

#[cfg(test)]
mod tests {
    use crate::redis::{put_usize, Error, Value};
    use bytes::BytesMut;

    #[test]
    fn test_put_usize() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"hello world!");
        put_usize(&mut buf, 1);
        println!("{:?}", buf);
    }

    #[test]
    fn incomplete_bulk_string() {
        let input = format!("$100\r\n{}\r\n", "0".repeat(100));
        let input = input.as_bytes();
        for i in 0..input.len() - 1 {
            let res = Value::from_bytes(&input[..i]);
            if let Err(Error::Incomplete) = &res {
                // Okay
                continue;
            }
            panic!("Error::Incomplete expected, found {:?}", res);
        }
        assert!(Value::from_bytes(input).is_ok());
    }
}
