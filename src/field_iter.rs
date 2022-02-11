use super::Result;
use crate::record_types::utils::read_record;
use std::iter::Iterator;
use std::str;

/// Iterator which goes over record header fields
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
        if self.buf.is_empty() {
            return None;
        }
        let (name, val, leftover) = match read_record(&self.buf) {
            Ok(v) => v,
            Err(err) => return Some(Err(err)),
        };
        self.buf = leftover;
        Some(Ok((name, val)))
    }
}
