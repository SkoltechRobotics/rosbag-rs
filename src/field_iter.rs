use byteorder::{LE, ByteOrder};
use std::str;
use std::iter::Iterator;
use super::{Result, Error};

pub(crate) struct FieldIterator<'a> {
    buf: &'a [u8],
}

impl<'a> FieldIterator<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }
}

impl<'a> Iterator for FieldIterator<'a> {
    type Item = Result<(&'a str, &'a [u8])>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf.len() < 4 { return Some(Err(Error::InvalidHeader)); }
        let n = LE::read_u32(&self.buf[..4]) as usize;
        self.buf = &self.buf[4..];

        if self.buf.len() < n { return Some(Err(Error::InvalidHeader)); }
        let rec = &self.buf[..n];
        self.buf = &self.buf[n..];

        let mut delim = 0;
        for (i, b) in rec.iter().enumerate() {
            match *b {
                b'=' => {
                    delim = i;
                    break;
                },
                0x20...0x7e => (),
                _ => return Some(Err(Error::InvalidHeader)),
            }
        }
        if delim == 0 { return Some(Err(Error::InvalidHeader)); }
        let name = str::from_utf8(&rec[..delim]).expect("already checked");
        let val = &rec[delim+1..];
        Some(Ok((name, val)))
    }
}
