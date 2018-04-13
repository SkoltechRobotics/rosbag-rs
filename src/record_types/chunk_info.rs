use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{unknown_field, set_field_u32, set_field_u64, set_field_time};
use byteorder::{LE, ReadBytesExt};
use std::io::{Read, Seek};

/// Entry which contains number of records in the `Chunk` for `Connection` with
/// `conn_id` ID.
#[derive(Debug, Clone, Default)]
pub struct ChunkInfoEntry {
    /// Connection id
    pub conn_id: u32,
    /// Number of messages that arrived on this connection in the chunk
    pub count: u32,
}

/// High-level index of `Chunk` records.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    /// Chunk info record version (only version 1 is currently cupported)
    pub ver: u32,
    /// Offset of the chunk record relative to the bag file beginning
    pub chunk_pos: u64,
    /// Timestamp of earliest message in the chunk in nanoseconds of UNIX epoch
    pub start_time: u64,
    /// Timestamp of latest message in the chunk in nanoseconds of UNIX epoch
    pub end_time: u64,
    /// Index entries
    pub data: Vec<ChunkInfoEntry>,
}

#[derive(Default)]
pub(crate) struct ChunkInfoHeader {
    pub ver: Option<u32>,
    pub chunk_pos: Option<u64>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub count: Option<u32>,
}

impl RecordGen for ChunkInfo {
    type Header = ChunkInfoHeader;

    fn parse_data<R: Read + Seek>(mut r: R, header: Self::Header) -> Result<Self> {
        let ver = header.ver.ok_or(Error::InvalidHeader)?;
        let chunk_pos = header.chunk_pos.ok_or(Error::InvalidHeader)?;
        let start_time = header.start_time.ok_or(Error::InvalidHeader)?;
        let end_time = header.end_time.ok_or(Error::InvalidHeader)?;
        let count = header.count.ok_or(Error::InvalidHeader)?;

        if ver != 1 { Err(Error::UnsupportedVersion)? }
        let n = r.read_u32::<LE>()?;
        if n % 8 != 0 { Err(Error::InvalidRecord)? }
        let n = n/8;
        if n != count { Err(Error::InvalidRecord)? }
        let mut data = vec![ChunkInfoEntry::default(); n as usize];
        for e in data.iter_mut() {
            e.conn_id = r.read_u32::<LE>()?;
            e.count = r.read_u32::<LE>()?;
        };
        Ok(Self { ver, chunk_pos, start_time, end_time, data })
    }
}

impl HeaderGen for ChunkInfoHeader {
    const OP: u8 = 0x06;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"ver" => set_field_u32(&mut self.ver, val)?,
            b"chunk_pos" => set_field_u64(&mut self.chunk_pos, val)?,
            b"start_time" => set_field_time(&mut self.start_time, val)?,
            b"end_time" => set_field_time(&mut self.end_time, val)?,
            b"count" => set_field_u32(&mut self.count, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
