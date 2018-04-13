//! Iterators over content of `Chunk`
use std::io::{self, Cursor, Seek, SeekFrom};
use std::str;
use super::{Result, Error};
use record_types::{MessageData, Connection, HeaderGen};
use record_types::message_data::MessageDataHeader;
use record_types::connection::ConnectionHeader;
use field_iter::FieldIterator;
use byteorder::{LE, ReadBytesExt};
use hex::FromHex;

/// Non-owning variant of [`MessageData`](record_types/struct.MessageData.html)
/// which borrows data from [`Chunk`](record_types/struct.Chunk.html) buffer.
#[derive(Debug, Clone, Default)]
pub struct MessageDataRef<'a> {
    /// ID for connection on which message arrived
    pub conn_id: u32,
    /// time at which the message was received in nanoseconds of UNIX epoch
    pub time: u64,
    /// serialized message data in the ROS serialization format
    pub data: &'a [u8],
}

/// Non-owning variant of [`Connection`](record_types/struct.Connection.html)
/// which borrows data from [`Chunk`](record_types/struct.Chunk.html) buffer.
#[derive(Debug, Clone)]
pub struct ConnectionRef<'a> {
    /// Unique connection ID
    pub id: u32,
    /// Topic on which the messages are stored
    pub storage_topic: &'a str,

    /// Name of the topic the subscriber is connecting to
    pub topic: &'a str,
    /// Message type
    pub tp: &'a str,
    /// MD5 hash sum of the message type
    pub md5sum: [u8; 16],
    /// Name of node sending data (can be empty)
    pub caller_id: &'a str,
    /// Is publisher in the latching mode? (i.e. sends the last value published
    /// to new subscribers)
    pub latching: bool,
}

/// Record types which can be stored in the `Chunk`
#[derive(Debug, Clone)]
pub enum ChunkRecord<'a> {
    MessageData(MessageDataRef<'a>),
    Connection(ConnectionRef<'a>),
}

impl<'a> MessageDataRef<'a> {
    fn new(header: MessageDataHeader, data: &'a [u8]) -> Result<Self> {
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

impl<'a> ConnectionRef<'a> {
    fn new(header: ConnectionHeader, data: &'a [u8]) -> Result<Self> {
        let id = header.id.ok_or(Error::InvalidHeader)?;
        let storage_topic = "";//header.storage_topic.ok_or(Error::InvalidHeader)?;

        let mut topic = None;
        let mut tp = None;
        let mut md5sum = None;
        let mut caller_id = None;
        let mut latching = false;

        for field in FieldIterator::new(&data) {
            let (name, val) = field?;
            match name {
                "topic" => set_field_str(&mut topic, val)?,
                "type" => set_field_str(&mut tp, val)?,
                "md5sum" => {
                    if md5sum.is_some() { Err(Error::InvalidRecord)? }
                    md5sum = Some(<[u8; 16]>::from_hex(val)
                        .map_err(|_| Error::InvalidRecord)?);
                },
                "callerid" => set_field_str(&mut caller_id, val)?,
                "latching" => latching = match val {
                    b"1" => true,
                    b"0" => false,
                    _ => Err(Error::InvalidRecord)?,
                },
                _ => warn!("Unknown field in the connection header: {}", name),
            }
        }

        let topic = topic.ok_or(Error::InvalidHeader)?;
        let tp = tp.ok_or(Error::InvalidHeader)?;
        let md5sum = md5sum.ok_or(Error::InvalidHeader)?;
        let caller_id = caller_id.unwrap_or("");
        Ok(Self { id, storage_topic, topic, tp, md5sum, caller_id, latching })
    }

    /// Convert to the `Connection` which owns `data` by copying its contents
    /// to the heap
    pub fn to_owned(self) -> Connection {
        Connection {
            id: self.id, storage_topic: self.storage_topic.to_string(),
            topic: self.topic.to_string(), tp: self.tp.to_string(),
            md5sum: <[u8; 16]>::from_hex(self.md5sum).expect("checked on init"),
            caller_id: self.caller_id.to_string(), latching: self.latching,
        }
    }
}

impl<'a> ChunkRecord<'a> {
    fn new(header: &'a [u8], data: &'a [u8]) -> Result<Self> {
        Ok(match MessageDataHeader::read_header(header) {
            Ok(header) => ChunkRecord::MessageData(
                MessageDataRef::new(header, data)?),
            // test if record is `Connection`
            Err(_) => {
                let header = ConnectionHeader::read_header(header)?;
                ChunkRecord::Connection(ConnectionRef::new(header, data)?)
            },
        })
    }

    fn message_only(header: &'a [u8], data: &'a [u8])
        -> Result<Option<MessageDataRef<'a>>>
    {
        Ok(match MessageDataHeader::read_header(header) {
            Ok(header) => Some(MessageDataRef::new(header, data)?),
            Err(_) => {
                ConnectionHeader::read_header(header)?;
                None
            },
        })
    }
}

/// Iterator which iterates over records stored in the
/// [`Chunk`](../record_types/struct.Chunk.html).
pub struct ChunkRecordsIterator<'a> {
    buf: Cursor<&'a [u8]>,
}

impl<'a> ChunkRecordsIterator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { buf: Cursor::new(data) }
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

impl<'a> Iterator for ChunkRecordsIterator<'a> {
    type Item = Result<ChunkRecord<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left() == 0 { return None; }

        let header = match self.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e)),
        };
        let data = match self.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e)),
        };

        Some(ChunkRecord::new(header, data))
    }
}


/// Iterator which iterates over `MessageData` records stored in the
/// [`Chunk`](../record_types/struct.Chunk.html).
///
/// It ignores `Connection` records.
pub struct ChunkMessagesIterator<'a> {
    inner: ChunkRecordsIterator<'a>,
}


impl<'a> ChunkMessagesIterator<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { inner: ChunkRecordsIterator::new(data) }
    }

    /// Seek to an offset, in bytes from the beggining of an internall chunk
    /// buffer.
    ///
    /// Offset values can be taken from `IndexData` records which follow
    /// `Chunk` used for iterator initialization. Be careful though, as
    /// incorrect offset value will lead to errors.
    pub fn seek(&mut self, offset: u32) -> io::Result<u64> {
        self.inner.seek(offset)
    }
}

impl<'a> Iterator for ChunkMessagesIterator<'a> {
    type Item = Result<MessageDataRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.left() == 0 { return None; }

        let header = match self.inner.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e)),
        };
        let data = match self.inner.next_chunk() {
            Ok(v) => v, Err(e) => return Some(Err(e)),
        };

        match ChunkRecord::message_only(header, data) {
            Ok(Some(v)) => Some(Ok(v)),
            // got connection record, ignore
            Ok(None) => self.next(),
            Err(e) => Some(Err(e)),
        }
    }
}

fn set_field_str<'a>(field: &mut Option<&'a str>, val: &'a [u8]) -> Result<()> {
    if field.is_some() { Err(Error::InvalidHeader)? }
    *field = Some(str::from_utf8(val)
        .map_err(|_| Error::InvalidHeader)?);
    Ok(())
}
