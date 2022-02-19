use super::{Error, Result};

use crate::cursor::Cursor;

use crate::field_iter::FieldIterator;
use crate::record_types::{
    BagHeader, Chunk, ChunkInfo, Connection, IndexData, MessageData, RecordGen,
};

/// Enum with all possible record variants
#[derive(Debug, Clone)]
pub enum Record<'a> {
    BagHeader(BagHeader),
    Chunk(Chunk<'a>),
    Connection(Connection<'a>),
    MessageData(MessageData<'a>),
    IndexData(IndexData<'a>),
    ChunkInfo(ChunkInfo<'a>),
}

impl<'a> Record<'a> {
    pub(crate) fn next_record(c: &mut Cursor<'a>) -> Result<Self> {
        let header = c.next_chunk()?;

        let mut fi = FieldIterator::new(header);
        let op = loop {
            match fi.next() {
                Some(Ok((name, v))) if name == "op" && v.len() == 1 => break v[0],
                Some(Ok(_)) => (),
                Some(Err(e)) => return Err(e),
                None => return Err(Error::InvalidRecord),
            }
        };

        Ok(match op {
            IndexData::OP => Record::IndexData(IndexData::read(header, c)?),
            Chunk::OP => Record::Chunk(Chunk::read(header, c)?),
            ChunkInfo::OP => Record::ChunkInfo(ChunkInfo::read(header, c)?),
            Connection::OP => Record::Connection(Connection::read(header, c)?),
            MessageData::OP => Record::MessageData(MessageData::read(header, c)?),
            BagHeader::OP => Record::BagHeader(BagHeader::read(header, c)?),
            _ => return Err(Error::InvalidRecord),
        })
    }

    pub fn get_type(&self) -> &'static str {
        match self {
            Record::BagHeader(_) => "BagHeader",
            Record::Chunk(_) => "Chunk",
            Record::Connection(_) => "Connection",
            Record::MessageData(_) => "MessageData",
            Record::IndexData(_) => "IndexData",
            Record::ChunkInfo(_) => "ChunkInfo",
        }
    }
}
