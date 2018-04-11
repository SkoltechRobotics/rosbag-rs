use std::io::Read;
use byteorder::{LE, ReadBytesExt};
use super::{Result, Error};

use record_types::{
    BagHeader, Chunk, Connection, MessageData, IndexData, ChunkInfo,
    RecordGen,
};
use field_iter::FieldIterator;

const BUF_SIZE: usize = 128;

#[derive(Debug, Clone)]
pub enum Record {
    BagHeader(BagHeader),
    Chunk(Chunk),
    // using `Box` results in a double indirection for `Connection`, but allows
    // to reduce size of the `Record` from 128 bytes to 64, considering
    // `Connection` rarity it's a good trade-off
    Connection(Box<Connection>),
    MessageData(MessageData),
    IndexData(IndexData),
    ChunkInfo(ChunkInfo),
}

fn read_opt<R: Read, F, T>(mut r: R, f: F) -> Result<T>
    where F: FnOnce(&[u8], R) -> Result<T>
{
    let n = r.read_u32::<LE>()? as usize;
    Ok(if n <= BUF_SIZE {
        let mut buf_array = [0u8; BUF_SIZE];
        let buf = &mut buf_array[..n];
        r.read_exact(buf)?;
        f(buf, r)?
    } else {
        let mut header = vec![0u8; n];
        r.read_exact(&mut header)?;
        f(&header, r)?
    })
}

impl Record {
    pub fn next_record<R: Read>(mut r: R) -> Result<Self> {
        read_opt(&mut r, |buf, r| {
            let f = FieldIterator::new(buf)
                .find(|v| match v {
                    Ok((name, _)) => name == &"op",
                    Err(_) => false,
                });
            let op = match f {
                Some(Ok((_, val))) if val.len() == 1 => val[0],
                _ => Err(Error::InvalidRecord)?,
            };
            Ok(match op {
                IndexData::OP => Record::IndexData(IndexData::read(buf, r)?),
                Chunk::OP => Record::Chunk(Chunk::read(buf, r)?),
                ChunkInfo::OP => Record::ChunkInfo(ChunkInfo::read(buf, r)?),
                Connection::OP =>
                    Record::Connection(Box::new(Connection::read(buf, r)?)),
                MessageData::OP =>
                    Record::MessageData(MessageData::read(buf, r)?),
                BagHeader::OP => Record::BagHeader(BagHeader::read(buf, r)?),
                _ => Err(Error::InvalidRecord)?
            })
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
