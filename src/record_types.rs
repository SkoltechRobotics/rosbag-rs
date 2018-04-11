use std::io::{Read};
use byteorder::{LE, ByteOrder, ReadBytesExt};
use hex::FromHex;
use super::{Result, Error};
use std::str;
use field_iter::FieldIterator;
use msg_iter::MessageIterator;

pub(crate) fn read_record(mut header: &[u8]) -> Result<(&[u8], &[u8], &[u8])> {
    if header.len() < 4 { Err(Error::InvalidHeader)? }
    let n = LE::read_u32(&header[..4]) as usize;
    header = &header[4..];

    if header.len() < n { Err(Error::InvalidHeader)? }
    let rec = &header[..n];
    header = &header[n..];

    let mut delim = 0;
    for (i, b) in rec.iter().enumerate() {
        match *b {
            b'=' => {
                delim = i;
                break;
            },
            0x20...0x7e => (),
            _ => Err(Error::InvalidHeader)?,
        }
    }
    if delim == 0 { Err(Error::InvalidHeader)? }
    let name = &rec[..delim];
    let val = &rec[delim+1..];
    Ok((name, val, header))
}

fn read_to_vec<R: Read>(mut r: R) -> Result<Vec<u8>> {
    let n = r.read_u32::<LE>()? as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn to_string(val: &[u8]) -> Result<String> {
    str::from_utf8(val)
        .map_err(|_| Error::InvalidRecord)
        .map(|v| v.to_string())
}

fn unknown_field(name: &[u8], val: &[u8]) {
    warn!("Unknown header field: {}={:?}",
        str::from_utf8(name).expect("already checked"), val);
}

#[derive(Debug, Clone, Default)]
pub struct BagHeader {
    /// offset of first record after the chunk section
    pub index_pos: u64,
    /// number of unique connections in the file
    pub conn_count: u32,
    /// number of chunk records in the file
    pub chunk_count: u32,
}

impl RecordGen for BagHeader {
    const OP: u8 = 0x03;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"index_pos" => {
                if val.len() != 8 { Err(Error::InvalidRecord)? }
                self.index_pos = LE::read_u64(val);
            },
            b"conn_count" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.conn_count = LE::read_u32(val);
            },
            b"chunk_count" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.chunk_count = LE::read_u32(val);
            },
            b"op" => if val.len() != 1 || val[0] != Self::OP {
                Err(Error::InvalidRecord)?
            },
            _ => unknown_field(name, val),
        }
        Ok(())
    }

    fn parse_data<R: Read>(&mut self, r: R) -> Result<()> {
        // TODO: replace with seek
        read_to_vec(r)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Compression {
    Bzip2,
    None
}

impl Default for Compression {
    fn default() -> Compression { Compression::None }
}


#[derive(Debug, Clone, Default)]
pub struct Chunk {
    /// compression type for the data
    pub compression: Compression,
    /// size in bytes of the uncompressed chunk
    pub size: u32,
    /// message data and connection records, compressed using the method
    /// specified in the `compression` field
    pub data: Vec<u8>,
}

impl Chunk {
    pub fn iter(&self) -> MessageIterator {
        MessageIterator::new(&self.data)
    }
}

impl RecordGen for Chunk {
    const OP: u8 = 0x05;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"compression" => {
                self.compression = match val {
                    b"none" => Compression::None,
                    b"bzip2" => Compression::Bzip2,
                    _ => Err(Error::InvalidRecord)?,
                };
            },
            b"size" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.size = LE::read_u32(val);
            },
            b"op" => if val.len() != 1 || val[0] != Self::OP {
                Err(Error::InvalidRecord)?
            },
            _ => unknown_field(name, val),
        }
        Ok(())
    }

    fn parse_data<R: Read>(&mut self, r: R) -> Result<()> {
        self.data = read_to_vec(r)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct Connection {
    /// unique connection ID
    pub id: u32,
    /// topic on which the messages are stored
    pub storage_topic: String,

    /// name of the topic the subscriber is connecting to
    pub topic: String,
    /// message type
    pub tp: String,
    /// md5sum of the message type
    pub md5sum: [u8; 16],
    /// name of node sending data
    pub caller_id: String,
    /// is publisher in latching mode (i.e. sends the last value published
    /// to new subscribers)
    pub latching: bool,
}

impl RecordGen for Connection {
    const OP: u8 = 0x07;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"topic" => self.storage_topic = to_string(val)?,
            b"conn" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.id = LE::read_u32(val);
            },
            b"op" => if val.len() != 1 || val[0] != Self::OP {
                Err(Error::InvalidRecord)?
            },
            _ => unknown_field(name, val),
        }
        Ok(())
    }

    fn parse_data<R: Read>(&mut self, mut r: R) -> Result<()> {
        let n = r.read_u32::<LE>()? as usize;
        let mut buf = vec![0u8; n];
        println!("{:?}", n);
        r.read_exact(&mut buf)?;

        for field in FieldIterator::new(&buf) {
            let (name, val) = field?;
            match name {
                "topic" => self.topic = to_string(val)?,
                "type" => self.tp = to_string(val)?,
                "md5sum" => self.md5sum = <[u8; 16]>::from_hex(val)
                    .map_err(|_| Error::InvalidRecord)?,
                "callerid" => self.caller_id = to_string(val)?,
                "latching" => self.latching = match val {
                    b"1" => true,
                    b"0" => false,
                    _ => Err(Error::InvalidRecord)?,
                },
                _ => warn!("Unknown field in the connection header: {}", name),
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct MessageData {
    /// ID for connection on which message arrived
    pub conn_id: u32,
    /// time at which the message was received in nanoseconds of UNIX epoch
    pub time: u64,
    /// serialized message data in the ROS serialization format
    pub data: Vec<u8>,
}

impl RecordGen for MessageData {
    const OP: u8 = 0x02;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"conn" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.conn_id = LE::read_u32(val);
            },
            b"time" => {
                if val.len() != 8 { Err(Error::InvalidRecord)? }
                let s = LE::read_u32(&val[..4]) as u64;
                let ns = LE::read_u32(&val[4..]) as u64;
                self.time = 1_000_000_000*s + ns;
            },
            b"op" => if val.len() != 1 || val[0] != Self::OP {
                Err(Error::InvalidRecord)?
            },
            _ => unknown_field(name, val),
        }
        Ok(())
    }

    fn parse_data<R: Read>(&mut self, r: R) -> Result<()> {
        self.data = read_to_vec(r)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct IndexEntry {
    /// time at which the message was received
    pub time: u64,
    /// offset of message data record in uncompressed chunk data
    pub offset: u32,
}

#[derive(Debug, Clone, Default)]
pub struct IndexData {
    /// index data record version
    pub ver: u32,
    /// connection ID
    pub conn_id: u32,
    /// number of messages on conn in the preceding chunk
    pub count: u32,
    /// occurrences of timestamps, chunk record offsets and message offsets
    pub data: Vec<IndexEntry>
}

impl RecordGen for IndexData {
    const OP: u8 = 0x04;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"ver" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.ver = LE::read_u32(val);
            },
            b"conn" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.conn_id = LE::read_u32(val);
            },
            b"count" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.count = LE::read_u32(val);
            },
            b"op" => if val.len() != 1 || val[0] != Self::OP {
                Err(Error::InvalidRecord)?
            },
            _ => unknown_field(name, val),
        }
        Ok(())
    }

    fn parse_data<R: Read>(&mut self, mut r: R) -> Result<()> {
        if self.ver != 1 { Err(Error::InvalidRecord)? }
        let n = r.read_u32::<LE>()? as usize;
        if n % 12 != 0 { Err(Error::InvalidRecord)? }
        self.data = vec![IndexEntry::default(); n/12];
        for e in self.data.iter_mut() {
            let s = r.read_u32::<LE>()? as u64;
            let ns = r.read_u32::<LE>()? as u64;
            e.time = 1_000_000_000*s + ns;
            e.offset = r.read_u32::<LE>()?;
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChunkInfoEntry {
    /// connection id
    pub conn_id: u32,
    /// number of messages that arrived on this connection in the chunk
    pub count: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ChunkInfo {
    /// chunk info record version
    pub ver: u32,
    /// offset of the chunk record
    pub chunk_pos: u64,
    /// timestamp of earliest message in the chunk in nanoseconds of UNIX epoch
    pub start_time: u64,
    /// timestamp of latest message in the chunk in nanoseconds of UNIX epoch
    pub end_time: u64,
    pub data: Vec<ChunkInfoEntry>,
}

impl RecordGen for ChunkInfo {
    const OP: u8 = 0x06;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"ver" => {
                if val.len() != 4 { Err(Error::InvalidRecord)? }
                self.ver = LE::read_u32(val);
            },
            b"chunk_pos" => {
                if val.len() != 8 { Err(Error::InvalidRecord)? }
                self.chunk_pos = LE::read_u64(val);
            },
            b"start_time" => {
                if val.len() != 8 { Err(Error::InvalidRecord)? }
                let s = LE::read_u32(&val[..4]) as u64;
                let ns = LE::read_u32(&val[4..]) as u64;
                self.start_time = 1_000_000_000*s + ns;
            },
            b"end_time" => {
                if val.len() != 8 { Err(Error::InvalidRecord)? }
                let s = LE::read_u32(&val[..4]) as u64;
                let ns = LE::read_u32(&val[4..]) as u64;
                self.end_time = 1_000_000_000*s + ns;
            },
            b"count" => (),
            b"op" => if val.len() != 1 || val[0] != Self::OP {
                Err(Error::InvalidRecord)?
            },
            _ => unknown_field(name, val),
        }
        Ok(())
    }

    fn parse_data<R: Read>(&mut self, mut r: R) -> Result<()> {
        if self.ver != 1 { Err(Error::InvalidRecord)? }
        let n = r.read_u32::<LE>()? as usize;
        if n % 8 != 0 { Err(Error::InvalidRecord)? }
        self.data = vec![ChunkInfoEntry::default(); n/8];
        for e in self.data.iter_mut() {
            e.conn_id = r.read_u32::<LE>()?;
            e.count = r.read_u32::<LE>()?;
        };
        Ok(())
    }
}

pub(crate) trait RecordGen: Sized + Default {
    const OP: u8;

    fn read<R: Read>(mut header: &[u8], r: R) -> Result<Self> {
        let mut rec = Self::default();
        while header.len() != 0 {
            let (name, val, new_header) = read_record(header)?;
            header = new_header;
            rec.process_field(name, val)?
        }
        rec.parse_data(r)?;
        Ok(rec)
    }

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> ;

    fn parse_data<R: Read>(&mut self, r: R)  -> Result<()>;
}
