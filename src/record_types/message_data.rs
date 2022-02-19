use super::utils::{set_field_time, set_field_u32, unknown_field};
use super::{Error, HeaderGen, RecordGen, Result};
use crate::cursor::Cursor;

/// Message data for a `Connection` with `conn_id` ID.
#[derive(Debug, Clone)]
pub struct MessageData<'a> {
    /// ID for connection on which message arrived
    pub conn_id: u32,
    /// Time at which the message was received in nanoseconds of UNIX epoch
    pub time: u64,
    /// Serialized message data in the ROS serialization format
    pub data: &'a [u8],
}

#[derive(Default, Debug)]
pub(crate) struct MessageDataHeader {
    pub conn_id: Option<u32>,
    pub time: Option<u64>,
}

impl<'a> RecordGen<'a> for MessageData<'a> {
    type Header = MessageDataHeader;

    fn read_data(c: &mut Cursor<'a>, header: Self::Header) -> Result<Self> {
        let conn_id = header.conn_id.ok_or(Error::InvalidHeader)?;
        let time = header.time.ok_or(Error::InvalidHeader)?;
        let data = c.next_chunk()?;
        Ok(MessageData {
            conn_id,
            time,
            data,
        })
    }
}

impl<'a> HeaderGen<'a> for MessageDataHeader {
    const OP: u8 = 0x02;

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "conn" => set_field_u32(&mut self.conn_id, val)?,
            "time" => set_field_time(&mut self.time, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
