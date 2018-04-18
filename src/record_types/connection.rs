use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{read_record, check_op, unknown_field};
use super::utils::{set_field_u32, set_field_str};
use field_iter::FieldIterator;
use hex::FromHex;

use cursor::Cursor;

/// Connection record which contains message type for ROS topic.
///
/// Two topic fields exist `storage_topic` and `topic`. This is because messages
/// can be written to the bag file on a topic different from where they were
/// originally published.
#[derive(Debug, Clone)]
pub struct Connection<'a> {
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

#[derive(Default, Debug)]
pub(crate) struct ConnectionHeader<'a> {
    pub id: Option<u32>,
    pub storage_topic: Option<&'a str>,
}

impl<'a> RecordGen<'a> for Connection<'a> {
    type Header = ConnectionHeader<'a>;

    fn read_data(c: &mut Cursor<'a>, header: Self::Header) -> Result<Self> {
        let id = header.id.ok_or(Error::InvalidHeader)?;
        let storage_topic = header.storage_topic.ok_or(Error::InvalidHeader)?;

        let buf = c.next_chunk()?;

        let mut topic = None;
        let mut tp = None;
        let mut md5sum = None;
        let mut caller_id = None;
        let mut latching = false;

        for field in FieldIterator::new(&buf) {
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
}

impl<'a> HeaderGen<'a> for ConnectionHeader<'a> {
    const OP: u8 = 0x07;

    fn read_header(mut header: &'a [u8]) -> Result<Self> {
        let mut rec = Self::default();
        while header.len() != 0 {
            let (name, val, new_header) = read_record(header)?;
            header = new_header;
            match name {
                b"op" => check_op(val, Self::OP)?,
                b"topic" => set_field_str(&mut rec.storage_topic, val)?,
                _ => rec.process_field(name, val)?,
            }
        }
        Ok(rec)
    }

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"conn" => set_field_u32(&mut self.id, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
