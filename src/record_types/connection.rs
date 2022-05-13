use super::utils::{check_op, read_record, unknown_field};
use super::utils::{set_field_str, set_field_u32};
use super::{Error, HeaderGen, RecordGen, Result};
use log::warn;

use crate::cursor::Cursor;
use crate::field_iter::FieldIterator;

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
    /// Full text of the message definition
    pub message_definition: &'a str,
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
        let mut message_definition = None;
        let mut caller_id = None;
        let mut latching = false;

        for field in FieldIterator::new(buf) {
            let (name, val) = field?;
            match name {
                "topic" => set_field_str(&mut topic, val)?,
                "type" => set_field_str(&mut tp, val)?,
                "md5sum" => {
                    if md5sum.is_some() || val.len() != 32 {
                        return Err(Error::InvalidRecord);
                    }
                    let mut res = [0u8; 16];
                    base16ct::lower::decode(val, &mut res).map_err(|_| Error::InvalidRecord)?;
                    md5sum = Some(res);
                }
                "message_definition" => set_field_str(&mut message_definition, val)?,
                "callerid" => set_field_str(&mut caller_id, val)?,
                "latching" => {
                    latching = match val {
                        b"1" => true,
                        b"0" => false,
                        _ => return Err(Error::InvalidRecord),
                    }
                }
                _ => warn!("Unknown field in the connection header: {}", name),
            }
        }

        let topic = topic.ok_or(Error::InvalidHeader)?;
        let tp = tp.ok_or(Error::InvalidHeader)?;
        let md5sum = md5sum.ok_or(Error::InvalidHeader)?;
        let message_definition = message_definition.ok_or(Error::InvalidHeader)?;
        let caller_id = caller_id.unwrap_or("");
        Ok(Self {
            id,
            storage_topic,
            topic,
            tp,
            md5sum,
            message_definition,
            caller_id,
            latching,
        })
    }
}

impl<'a> HeaderGen<'a> for ConnectionHeader<'a> {
    const OP: u8 = 0x07;

    fn read_header(mut header: &'a [u8]) -> Result<Self> {
        let mut rec = Self::default();
        while !header.is_empty() {
            let (name, val, new_header) = read_record(header)?;
            header = new_header;
            match name {
                "op" => check_op(val, Self::OP)?,
                "topic" => set_field_str(&mut rec.storage_topic, val)?,
                _ => rec.process_field(name, val)?,
            }
        }
        Ok(rec)
    }

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "conn" => set_field_u32(&mut self.id, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
