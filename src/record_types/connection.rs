use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{unknown_field, set_field_u32, set_field_string};
use byteorder::{LE, ReadBytesExt};
use field_iter::FieldIterator;
use std::io::{Read, Seek};
use hex::FromHex;

/// Connection record which contains message type for ROS topic.
///
/// Two topic fields exist `storage_topic` and `topic`. This is because messages
/// can be written to the bag file on a topic different from where they were
/// originally published.
#[derive(Debug, Clone)]
pub struct Connection {
    /// Unique connection ID
    pub id: u32,
    /// Topic on which the messages are stored
    pub storage_topic: String,

    /// Name of the topic the subscriber is connecting to
    pub topic: String,
    /// Message type
    pub tp: String,
    /// MD5 hash sum of the message type
    pub md5sum: [u8; 16],
    /// Name of node sending data (can be empty)
    pub caller_id: String,
    /// Is publisher in the latching mode? (i.e. sends the last value published
    /// to new subscribers)
    pub latching: bool,
}

#[derive(Default, Debug)]
pub(crate) struct ConnectionHeader {
    pub id: Option<u32>,
    pub storage_topic: Option<String>,
}

impl RecordGen for Connection {
    type Header = ConnectionHeader;

    fn parse_data<R: Read + Seek>(mut r: R, header: Self::Header) -> Result<Self> {
        let id = header.id.ok_or(Error::InvalidHeader)?;
        let storage_topic = header.storage_topic.ok_or(Error::InvalidHeader)?;

        let n = r.read_u32::<LE>()? as usize;
        let mut buf = vec![0u8; n];
        r.read_exact(&mut buf)?;

        let mut topic = None;
        let mut tp = None;
        let mut md5sum = None;
        let mut caller_id = None;
        let mut latching = false;

        for field in FieldIterator::new(&buf) {
            let (name, val) = field?;
            match name {
                "topic" => set_field_string(&mut topic, val)?,
                "type" => set_field_string(&mut tp, val)?,
                "md5sum" => {
                    if md5sum.is_some() { Err(Error::InvalidRecord)? }
                    md5sum = Some(<[u8; 16]>::from_hex(val)
                        .map_err(|_| Error::InvalidRecord)?);
                },
                "callerid" => set_field_string(&mut caller_id, val)?,
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
        let caller_id = caller_id.unwrap_or(String::default());
        Ok(Self { id, storage_topic, topic, tp, md5sum, caller_id, latching })
    }
}


impl HeaderGen for ConnectionHeader {
    const OP: u8 = 0x07;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"topic" => set_field_string(&mut self.storage_topic, val)?,
            b"conn" => set_field_u32(&mut self.id, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
