use super::utils::{set_field_u32, unknown_field};
use super::{Error, HeaderGen, RecordGen, Result};

use crate::cursor::Cursor;

/// Index record which describes messages offset for `Connection` with
/// `conn_id` ID in the preceding `Chunk`.
#[derive(Debug, Clone)]
pub struct IndexData<'a> {
    /// Index data record version (only version 1 is currently cupported)
    pub ver: u32,
    /// Connection ID
    pub conn_id: u32,
    /// Occurrences of timestamps, chunk record offsets and message offsets
    data: &'a [u8],
}

impl<'a> IndexData<'a> {
    /// Get entries iterator.
    pub fn entries(&'a self) -> IndexDataEntriesIterator<'a> {
        IndexDataEntriesIterator {
            cursor: Cursor::new(self.data),
        }
    }
}

#[derive(Default)]
pub(crate) struct IndexDataHeader {
    pub ver: Option<u32>,
    pub conn_id: Option<u32>,
    pub count: Option<u32>,
}

impl<'a> RecordGen<'a> for IndexData<'a> {
    type Header = IndexDataHeader;

    fn read_data(c: &mut Cursor<'a>, header: Self::Header) -> Result<Self> {
        let ver = header.ver.ok_or(Error::InvalidHeader)?;
        let conn_id = header.conn_id.ok_or(Error::InvalidHeader)?;
        let count = header.count.ok_or(Error::InvalidHeader)?;

        if ver != 1 {
            return Err(Error::UnsupportedVersion);
        }
        let n = c.next_u32()?;
        if n % 12 != 0 || n / 12 != count {
            return Err(Error::InvalidRecord);
        }
        let data = c.next_bytes(n as u64)?;
        Ok(Self { ver, conn_id, data })
    }
}

impl<'a> HeaderGen<'a> for IndexDataHeader {
    const OP: u8 = 0x04;

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "ver" => set_field_u32(&mut self.ver, val)?,
            "conn" => set_field_u32(&mut self.conn_id, val)?,
            "count" => set_field_u32(&mut self.count, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}

/// Index entry which contains message offset and its timestamp.
#[derive(Debug, Clone, Default)]
pub struct IndexDataEntry {
    /// Time at which the message was received
    pub time: u64,
    /// Offset of message data record in uncompressed chunk data
    pub offset: u32,
}

/// Iterator over `IndexData` entries
pub struct IndexDataEntriesIterator<'a> {
    cursor: Cursor<'a>,
}

impl<'a> Iterator for IndexDataEntriesIterator<'a> {
    type Item = IndexDataEntry;

    fn next(&mut self) -> Option<IndexDataEntry> {
        if self.cursor.left() == 0 {
            return None;
        }
        if self.cursor.left() < 12 {
            panic!("unexpected data leftover for entries")
        }
        let time = self.cursor.next_time().expect("already checked");
        let offset = self.cursor.next_u32().expect("already checked");
        Some(IndexDataEntry { time, offset })
    }
}
