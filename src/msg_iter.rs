use std::io::{self, Cursor, Seek, SeekFrom};
use super::{Result, Error};
use record_types::{MessageData, RecordGen};
use byteorder::{LE, ReadBytesExt};

pub struct MessageIterator<'a> {
    buf: Cursor<&'a [u8]>,
}

impl<'a> MessageIterator<'a> {
    pub fn new<S: AsRef<[u8]>>(slice: &'a S) -> Self {
        Self { buf: Cursor::new(slice.as_ref()) }
    }

    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.buf.seek(pos)
    }

    fn left(&self) -> usize {
        self.buf.get_ref().len() - self.buf.position() as usize
    }

    fn next_message(&mut self) -> Result<MessageData> {
        let n = self.buf.read_u32::<LE>()? as usize;
        if n > self.left() { Err(Error::InvalidRecord)? }
        let s = self.buf.position() as usize;
        self.buf.set_position((s + n) as u64);
        let header = &self.buf.get_ref()[s..s + n];
        MessageData::read(header, &mut self.buf)
    }
}

impl<'a> Iterator for MessageIterator<'a> {
    type Item = Result<MessageData>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left() != 0 { Some(self.next_message()) } else { None }

    }
}
