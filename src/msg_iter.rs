//! Iterators over content of `Chunk`
use super::Result;
use crate::record_types::{RecordGen, MessageData, Connection, HeaderGen};
use crate::record_types::message_data::MessageDataHeader;
use crate::record_types::connection::ConnectionHeader;


use crate::cursor::Cursor;

/// Record types which can be stored in the `Chunk`
#[derive(Debug, Clone)]
pub enum ChunkRecord<'a> {
    MessageData(MessageData<'a>),
    Connection(Connection<'a>),
}

impl<'a> ChunkRecord<'a> {
    fn new(header: &'a [u8], cursor: &mut Cursor<'a>) -> Result<Self> {
        Ok(match MessageDataHeader::read_header(header) {
            Ok(h) => ChunkRecord::MessageData(MessageData::read_data(cursor, h)?),
            // test if record is `Connection`
            Err(_) => ChunkRecord::Connection(Connection::read(header, cursor)?),
        })
    }

    fn message_only(header: &'a [u8], cursor: &mut Cursor<'a>)
        -> Result<Option<MessageData<'a>>>
    {
        Ok(match MessageDataHeader::read_header(header) {
            Ok(h) => Some(MessageData::read_data(cursor, h)?),
            Err(_) => {
                ConnectionHeader::read_header(header)?;
                cursor.next_chunk()?;
                None
            },
        })
    }
}

/// Iterator which iterates over records stored in the
/// [`Chunk`](../record_types/struct.Chunk.html).
pub struct ChunkRecordsIterator<'a> {
    cursor: Cursor<'a>,
}

impl<'a> ChunkRecordsIterator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        assert!(data.len() <= 1<<32,
            "chunk length should not be bigger than 2^32");
        Self { cursor: Cursor::new(data) }
    }

    /// Seek to an offset, in bytes from the beggining of an internall chunk
    /// buffer.
    ///
    /// Offset values can be taken from `IndexData` records which follow
    /// `Chunk` used for iterator initialization. Be careful though, as
    /// incorrect offset value will lead to errors.
    pub fn seek(&mut self, offset: u32) -> Result<()> {
        Ok(self.cursor.seek(offset as u64)?)
    }
}

impl<'a> Iterator for ChunkRecordsIterator<'a> {
    type Item = Result<ChunkRecord<'a>>;

    fn next(&mut self) -> Option<Result<ChunkRecord<'a>>> {
        if self.cursor.left() == 0 { return None; }

        let header = match self.cursor.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e.into())),
        };

        Some(ChunkRecord::new(header, &mut self.cursor))
    }
}


/// Iterator which iterates over `MessageData` records stored in the
/// [`Chunk`](../record_types/struct.Chunk.html).
///
/// It ignores `Connection` records.
pub struct ChunkMessagesIterator<'a> {
    cursor: Cursor<'a>,
}


impl<'a> ChunkMessagesIterator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { cursor: Cursor::new(data) }
    }

    /// Seek to an offset, in bytes from the beggining of an internall chunk
    /// buffer.
    ///
    /// Offset values can be taken from `IndexData` records which follow
    /// `Chunk` used for iterator initialization. Be careful though, as
    /// incorrect offset value will lead to errors.
    pub fn seek(&mut self, offset: u32) -> Result<()> {
        Ok(self.cursor.seek(offset as u64)?)
    }
}

impl<'a> Iterator for ChunkMessagesIterator<'a> {
    type Item = Result<MessageData<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.left() == 0 { return None; }

        let header = match self.cursor.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e.into())),
        };

        match ChunkRecord::message_only(header, &mut self.cursor) {
            Ok(Some(v)) => Some(Ok(v)),
            // got connection record, ignore
            Ok(None) => self.next(),
            Err(e) => Some(Err(e)),
        }
    }
}
