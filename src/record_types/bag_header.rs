use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{unknown_field, set_field_u64, set_field_u32};
use std::io::{Read, SeekFrom, Seek};
use byteorder::{LE, ReadBytesExt};

/// Bag file header record which contains basic information about the file.
#[derive(Debug, Clone)]
pub struct BagHeader {
    /// Offset of first record after the chunk section
    pub index_pos: u64,
    /// Number of unique connections in the file
    pub conn_count: u32,
    /// Number of chunk records in the file
    pub chunk_count: u32,
}

#[derive(Default)]
pub(crate) struct BagHeaderHeader {
    index_pos: Option<u64>,
    conn_count: Option<u32>,
    chunk_count: Option<u32>,
}

impl RecordGen for BagHeader {
    type Header = BagHeaderHeader;

    fn parse_data<R: Read + Seek>(mut r: R, header: Self::Header) -> Result<Self> {
        let index_pos = header.index_pos.ok_or(Error::InvalidHeader)?;
        let conn_count = header.conn_count.ok_or(Error::InvalidHeader)?;
        let chunk_count = header.chunk_count.ok_or(Error::InvalidHeader)?;
        let n = r.read_u32::<LE>()? as i64;
        r.seek(SeekFrom::Current(n))?;
        Ok(BagHeader { index_pos, conn_count, chunk_count } )
    }
}

impl HeaderGen for BagHeaderHeader {
    const OP: u8 = 0x03;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"index_pos" => set_field_u64(&mut self.index_pos, val)?,
            b"conn_count" => set_field_u32(&mut self.conn_count, val)?,
            b"chunk_count" => set_field_u32(&mut self.chunk_count, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
