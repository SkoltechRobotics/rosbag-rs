use super::utils::{set_field_time, set_field_u32, set_field_u64, unknown_field};
use super::{Error, HeaderGen, RecordGen, Result};

use crate::cursor::Cursor;

/// High-level index of `Chunk` records.
#[derive(Debug, Clone)]
pub struct ChunkInfo<'a> {
    /// Chunk info record version (only version 1 is currently cupported)
    pub ver: u32,
    /// Offset of the chunk record relative to the bag file beginning
    pub chunk_pos: u64,
    /// Timestamp of earliest message in the chunk in nanoseconds of UNIX epoch
    pub start_time: u64,
    /// Timestamp of latest message in the chunk in nanoseconds of UNIX epoch
    pub end_time: u64,
    /// Index entries data
    data: &'a [u8],
}

impl<'a> ChunkInfo<'a> {
    /// Get entries iterator.
    pub fn entries(&'a self) -> ChunkInfoEntriesIterator<'a> {
        ChunkInfoEntriesIterator {
            cursor: Cursor::new(self.data),
        }
    }
}

#[derive(Default)]
pub(crate) struct ChunkInfoHeader {
    pub ver: Option<u32>,
    pub chunk_pos: Option<u64>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub count: Option<u32>,
}

impl<'a> RecordGen<'a> for ChunkInfo<'a> {
    type Header = ChunkInfoHeader;

    fn read_data(c: &mut Cursor<'a>, header: Self::Header) -> Result<Self> {
        let ver = header.ver.ok_or(Error::InvalidHeader)?;
        let chunk_pos = header.chunk_pos.ok_or(Error::InvalidHeader)?;
        let start_time = header.start_time.ok_or(Error::InvalidHeader)?;
        let end_time = header.end_time.ok_or(Error::InvalidHeader)?;
        let count = header.count.ok_or(Error::InvalidHeader)?;

        if ver != 1 {
            return Err(Error::UnsupportedVersion);
        }
        let n = c.next_u32()?;
        if n % 8 != 0 || n / 8 != count {
            return Err(Error::InvalidRecord);
        }
        let data = c.next_bytes(n as u64)?;
        Ok(Self {
            ver,
            chunk_pos,
            start_time,
            end_time,
            data,
        })
    }
}

impl<'a> HeaderGen<'a> for ChunkInfoHeader {
    const OP: u8 = 0x06;

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "ver" => set_field_u32(&mut self.ver, val)?,
            "chunk_pos" => set_field_u64(&mut self.chunk_pos, val)?,
            "start_time" => set_field_time(&mut self.start_time, val)?,
            "end_time" => set_field_time(&mut self.end_time, val)?,
            "count" => set_field_u32(&mut self.count, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}

/// Entry which contains number of records in the `Chunk` for `Connection` with
/// `conn_id` ID.
#[derive(Debug, Clone, Default)]
pub struct ChunkInfoEntry {
    /// Connection id
    pub conn_id: u32,
    /// Number of messages that arrived on this connection in the chunk
    pub count: u32,
}

/// Iterator over `ChunkInfo` entries
pub struct ChunkInfoEntriesIterator<'a> {
    cursor: Cursor<'a>,
}

impl<'a> Iterator for ChunkInfoEntriesIterator<'a> {
    type Item = ChunkInfoEntry;

    fn next(&mut self) -> Option<ChunkInfoEntry> {
        if self.cursor.left() == 0 {
            return None;
        }
        if self.cursor.left() < 8 {
            panic!("unexpected data leftover for entries")
        }
        let conn_id = self.cursor.next_u32().expect("already checked");
        let count = self.cursor.next_u32().expect("already checked");

        Some(ChunkInfoEntry { conn_id, count })
    }
}
