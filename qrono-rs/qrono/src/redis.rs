use std::num::ParseIntError;
use std::ops::{Range, RangeInclusive};
use std::str;
use std::str::FromStr;
use std::str::Utf8Error;
use std::string::FromUtf8Error;

use crate::redis::Error::{Incomplete, ProtocolError};
use crate::redis::Value::{Integer, SimpleString};
use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Debug, Clone)]
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

    pub fn put<B: RedisBuf>(&self, buf: &mut B) {
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
                put_i64(buf, *n);
                buf.put_slice(b"\r\n");
            }
            Value::BulkString(b) => {
                buf.put_u8(b'$');
                // TODO: Overflow check?
                put_u32(buf, b.len() as u32);
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            Value::Array(vals) => {
                buf.put_u8(b'*');
                put_u32(buf, vals.len() as u32);
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
            Value::BulkString(b) => u32_len(b.len() as u32) + b.len() + 5, // $<len>\r\n<b>\r\n
            Value::Array(els) => {
                let mut size = u32_len(els.len() as u32) + 3; // *<len>\r\n[elements...]
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

#[inline]
pub fn i64_len(n: i64) -> usize {
    if n < 0 {
        let n = !(n as u64) + 1;
        1 + u64_len(n)
    } else {
        u64_len(n as u64)
    }
}

// Take from stdlib unstable method
#[inline]
#[allow(clippy::unusual_byte_groupings)]
const fn less_than_5(val: u32) -> u32 {
    // Similar to u8, when adding one of these constants to val,
    // we get two possible bit patterns above the low 17 bits,
    // depending on whether val is below or above the threshold.
    const C1: u32 = 0b011_00000000000000000 - 10; // 393206
    const C2: u32 = 0b100_00000000000000000 - 100; // 524188
    const C3: u32 = 0b111_00000000000000000 - 1000; // 916504
    const C4: u32 = 0b100_00000000000000000 - 10000; // 514288

    // Value of top bits:
    //                +c1  +c2  1&2  +c3  +c4  3&4   ^
    //         0..=9  010  011  010  110  011  010  000 = 0
    //       10..=99  011  011  011  110  011  010  001 = 1
    //     100..=999  011  100  000  110  011  010  010 = 2
    //   1000..=9999  011  100  000  111  011  011  011 = 3
    // 10000..=99999  011  100  000  111  100  100  100 = 4
    (((val + C1) & (val + C2)) ^ ((val + C3) & (val + C4))) >> 17
}

#[inline]
pub const fn log10_u64(mut val: u64) -> u32 {
    let mut log = 0;
    if val >= 10_000_000_000 {
        val /= 10_000_000_000;
        log += 10;
    }
    if val >= 100_000 {
        val /= 100_000;
        log += 5;
    }
    log + less_than_5(val as u32)
}

#[inline]
pub const fn log10_u32(mut val: u32) -> u32 {
    let mut log = 0;
    if val >= 100_000 {
        val /= 100_000;
        log += 5;
    }
    log + less_than_5(val)
}

#[inline]
pub const fn u64_len(val: u64) -> usize {
    (log10_u64(val) + 1) as usize
}

#[inline]
pub const fn u32_len(val: u32) -> usize {
    (log10_u32(val) + 1) as usize
}

macro_rules! digits_fn {
    ($name:ident, $width: literal) => {
        #[inline]
        fn $name(n: usize, dst: &mut [u8]) {
            const MAX: usize = 10usize.pow($width);
            const DIGITS: [u8; $width * MAX] = {
                let mut digits = [b'0'; $width * MAX];
                let mut i = 0;
                while i < MAX {
                    let mut off = ($width * i) + ($width - 1);
                    let mut n = i;
                    while n >= 10 {
                        digits[off] = b'0' + (n % 10) as u8;
                        n /= 10;
                        off -= 1;
                    }
                    digits[off] = b'0' + (n as u8);
                    i += 1;
                }
                digits
            };
            let idx = $width * n;
            let src = &DIGITS[idx..idx + $width];
            dst.copy_from_slice(src);
        }
    };
}

digits_fn!(digits2, 2);
digits_fn!(digits4, 4);

pub trait RedisBuf: Sized {
    fn put_u8(&mut self, val: u8) {
        self.put_slice(&[val]);
    }

    fn put_slice(&mut self, val: &[u8]);

    fn put_value(&mut self, val: &Value) {
        val.put(self)
    }
}

impl RedisBuf for BytesMut {
    fn put_u8(&mut self, val: u8) {
        BufMut::put_u8(self, val)
    }

    fn put_slice(&mut self, val: &[u8]) {
        BufMut::put_slice(self, val)
    }

    fn put_value(&mut self, val: &Value) {
        self.reserve(val.encoded_length());
        val.put(self);
    }
}

impl RedisBuf for Vec<u8> {
    #[inline]
    fn put_u8(&mut self, val: u8) {
        self.push(val);
    }

    #[inline]
    fn put_slice(&mut self, val: &[u8]) {
        self.extend_from_slice(val);
    }
}

#[inline]
pub fn put_u32<B: RedisBuf>(buf: &mut B, n: u32) {
    if n < 10 {
        buf.put_u8(b'0' + n as u8);
    } else {
        put_u32_slow(buf, n)
    }
}

fn put_u32_slow<B: RedisBuf>(buf: &mut B, mut n: u32) {
    const N: u32 = 10_000;
    let mut temp = [0u8; 10];
    let len = u32_len(n);

    let q = n / N;
    let r = n % N;
    digits4(r as usize, &mut temp[6..10]);
    n = q;

    let q = n / N;
    let r = n % N;
    digits4(r as usize, &mut temp[2..6]);
    n = q;

    digits2(n as usize, &mut temp[0..2]);

    buf.put_slice(&temp[temp.len() - len..]);
}

#[inline]
pub fn put_u64<B: RedisBuf>(buf: &mut B, n: u64) {
    if n < 10 {
        buf.put_u8(b'0' + n as u8)
    } else {
        put_u64_slow(buf, n)
    }
}

fn put_u64_slow<B: RedisBuf>(buf: &mut B, mut n: u64) {
    const N: u64 = 10_000;
    let mut temp = [0u8; 20];
    let mut pos = temp.len();
    let len = u64_len(n);
    while pos > 0 {
        let q = n / N;
        let r = n % N;
        digits4(r as usize, &mut temp[pos - 4..pos]);
        n = q;
        pos -= 4;
    }
    buf.put_slice(&temp[temp.len() - len..]);
}

#[inline]
pub fn put_i64<B: RedisBuf>(buf: &mut B, n: i64) {
    if n < 0 {
        buf.put_u8(b'-');
        let n = !(n as u64) + 1;
        put_u64(buf, n);
    } else {
        put_u64(buf, n as u64);
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
    use crate::redis::{put_i64, put_u32, put_u64, Error, Value};

    fn buf() -> Vec<u8> {
        Vec::new()
    }

    #[test]
    fn test_put_u32() {
        let test = |n: u32| {
            let expected = format!("{}", n);
            let mut buf = buf();
            put_u32(&mut buf, n);
            let actual = &buf[..];
            assert_eq!(expected.as_str(), std::str::from_utf8(actual).unwrap());
        };
        for i in 0..=100000 {
            test(i);
        }
        for s in 0..u32::BITS {
            test(1 << s);
        }

        let maxpow = (u32::MAX as f64).log10() as u32;
        for pow in 0..=maxpow {
            let n = 10u32.pow(pow);

            test(n - 1);
            test(n);
            test(n + 1);
        }

        test(u32::MIN);
        test(u32::MAX);
    }

    #[test]
    fn test_put_u64() {
        let test = |n: u64| {
            let expected = format!("{}", n);
            let mut buf = buf();
            put_u64(&mut buf, n);
            let actual = &buf[..];
            assert_eq!(expected.as_str(), std::str::from_utf8(actual).unwrap());
        };
        for i in 0..=100000 {
            test(i);
        }
        for s in 0..u64::BITS {
            test(1 << s);
        }

        let maxpow = (u64::MAX as f64).log10() as u32;
        for pow in 0..=maxpow {
            let n = 10u64.pow(pow);

            test(n - 1);
            test(n);
            test(n + 1);
        }

        test(u64::MIN);
        test(u64::MAX);
    }

    #[test]
    fn test_encode_integer() {
        let test = |n: i64| {
            let expected = format!(":{}\r\n", n);
            let mut buf = buf();
            Value::Integer(n).put(&mut buf);
            let actual = &buf[..];
            assert_eq!(expected.as_str(), std::str::from_utf8(actual).unwrap());
        };
        for i in -100000..=100000 {
            test(i);
        }
        for s in 0..i64::BITS {
            test(1 << s);
        }

        let maxpow = (i64::MAX as f64).log10() as u32;
        for pow in 0..=maxpow {
            let n = 10i64.pow(pow);

            test(n - 1);
            test(n);
            test(n + 1);

            test(-n - 1);
            test(-n);
            test(-n + 1);
        }

        test(i64::MIN);
        test(i64::MAX);
    }

    #[test]
    fn test_put_i64() {
        let test = |n: i64| {
            let expected = format!("{}", n);
            let mut buf = buf();
            put_i64(&mut buf, n);
            let actual = &buf[..];
            assert_eq!(expected.as_str(), std::str::from_utf8(actual).unwrap());
        };
        for i in -100000..=100000 {
            test(i);
        }
        for s in 0..i64::BITS {
            test(1 << s);
        }

        let maxpow = (i64::MAX as f64).log10() as u32;
        for pow in 0..=maxpow {
            let n = 10i64.pow(pow);

            test(n - 1);
            test(n);
            test(n + 1);

            test(-n - 1);
            test(-n);
            test(-n + 1);
        }

        test(i64::MIN);
        test(i64::MAX);
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
