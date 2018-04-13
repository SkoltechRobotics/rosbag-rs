use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{unknown_field, set_field_u32};
use byteorder::{LE, ReadBytesExt};
use std::io::{Read, Seek};

/// Index entry which contains message offset and its timestamp.
#[derive(Debug, Clone, Default)]
pub struct IndexDataEntry {
    /// Time at which the message was received
    pub time: u64,
    /// Offset of message data record in uncompressed chunk data
    pub offset: u32,
}

/// Index record which describes messages offset for `Connection` with
/// `conn_id` ID in the preceding `Chunk`.
#[derive(Debug, Clone)]
pub struct IndexData {
    /// Index data record version (only version 1 is currently cupported)
    pub ver: u32,
    /// Connection ID
    pub conn_id: u32,
    /// Occurrences of timestamps, chunk record offsets and message offsets
    pub data: Vec<IndexDataEntry>
}

#[derive(Default)]
pub(crate) struct IndexDataHeader {
    pub ver: Option<u32>,
    pub conn_id: Option<u32>,
    pub count: Option<u32>,
}

impl RecordGen for IndexData {
    type Header = IndexDataHeader;

    fn parse_data<R: Read + Seek>(mut r: R, header: Self::Header) -> Result<Self> {
        let ver = header.ver.ok_or(Error::InvalidHeader)?;
        let conn_id = header.conn_id.ok_or(Error::InvalidHeader)?;
        let count = header.count.ok_or(Error::InvalidHeader)?;


        if ver != 1 { Err(Error::UnsupportedVersion)? }
        let n = r.read_u32::<LE>()?;
        if n % 12 != 0 { Err(Error::InvalidRecord)? }
        let n = n/12;
        if n != count { Err(Error::InvalidRecord)? }
        let mut data = vec![IndexDataEntry::default(); n as usize];
        for e in data.iter_mut() {
            let s = r.read_u32::<LE>()? as u64;
            let ns = r.read_u32::<LE>()? as u64;
            e.time = 1_000_000_000*s + ns;
            e.offset = r.read_u32::<LE>()?;
        };
        Ok(Self { ver, conn_id, data })
    }
}

impl HeaderGen for IndexDataHeader {
    const OP: u8 = 0x04;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"ver" => set_field_u32(&mut self.ver, val)?,
            b"conn" => set_field_u32(&mut self.conn_id, val)?,
            b"count" => set_field_u32(&mut self.count, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
