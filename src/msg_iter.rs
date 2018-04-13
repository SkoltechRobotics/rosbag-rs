use std::io::{self, Cursor, Seek, SeekFrom};
use super::{Result, Error};
use record_types::{MessageData, HeaderGen};
use record_types::message_data::MessageDataHeader;
use byteorder::{LE, ReadBytesExt};

/// Non-owning variant of [`MessageData`](record_types/struct.MessageData.html)
/// which borrows data from [`Chunk`](record_types/struct.Chunk.html) buffer
#[derive(Debug, Clone, Default)]
pub struct MessageDataRef<'a> {
    /// ID for connection on which message arrived
    pub conn_id: u32,
    /// time at which the message was received in nanoseconds of UNIX epoch
    pub time: u64,
    /// serialized message data in the ROS serialization format
    pub data: &'a [u8],
}

impl<'a> MessageDataRef<'a> {
    fn new(header: &[u8], data: &'a [u8]) -> Result<Self> {
        let header = MessageDataHeader::read_header(header)?;
        let conn_id = header.conn_id.ok_or(Error::InvalidHeader)?;
        let time = header.time.ok_or(Error::InvalidHeader)?;
        Ok(MessageDataRef { conn_id, time, data })
    }

    /// Convert to the `MessageData` which owns `data` by copying its contents
    /// into a vector
    pub fn to_owned(self) -> MessageData {
        MessageData {
            conn_id: self.conn_id, time: self.time, data: self.data.to_vec()
        }
    }
}

/// Iterator which iterates over messages in the
/// [`Chunk`](../record_types/struct.Chunk.html) from which it was acquired.
///
/// It assumes that only `MessageData` records are stored in the provided
/// `Chunk`.
pub struct ChunkMessagesIterator<'a> {
    buf: Cursor<&'a [u8]>,
}

impl<'a> ChunkMessagesIterator<'a> {
    pub(crate) fn new<S: AsRef<[u8]>>(slice: &'a S) -> Self {
        Self { buf: Cursor::new(slice.as_ref()) }
    }

    /// Seek to an offset, in bytes from the beggining of an internall chunk
    /// buffer.
    ///
    /// Offset values can be taken from `IndexData` records which follow
    /// `Chunk` used for iterator initialization. Be careful though, as
    /// incorrect offset value will lead to errors.
    pub fn seek(&mut self, offset: u32) -> io::Result<u64> {
        self.buf.seek(SeekFrom::Start(offset as u64))
    }

    fn left(&self) -> usize {
        self.buf.get_ref().len() - self.buf.position() as usize
    }

    fn next_chunk(&mut self) -> Result<&'a [u8]> {
        let n = self.buf.read_u32::<LE>()? as usize;
        if n > self.left() { Err(Error::InvalidRecord)? }
        let s = self.buf.position() as usize;
        self.buf.set_position((s + n) as u64);
        Ok(&self.buf.get_ref()[s..s + n])
    }
}

impl<'a> Iterator for ChunkMessagesIterator<'a> {
    type Item = Result<MessageDataRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left() == 0 { return None; }

        let header = match self.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e)),
        };
        let data = match self.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e)),
        };

        Some(MessageDataRef::new(header, data))
    }
}
