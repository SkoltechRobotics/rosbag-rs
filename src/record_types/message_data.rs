use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{unknown_field, set_field_u32, set_field_time, read_to_vec};
use std::io::{Read, Seek};

/// Message data for a `Connection` with `conn_id` ID.
#[derive(Debug, Clone)]
pub struct MessageData {
    /// ID for connection on which message arrived
    pub conn_id: u32,
    /// Time at which the message was received in nanoseconds of UNIX epoch
    pub time: u64,
    /// Serialized message data in the ROS serialization format
    pub data: Vec<u8>,
}

#[derive(Default, Debug)]
pub(crate) struct MessageDataHeader {
    pub conn_id: Option<u32>,
    pub time: Option<u64>,
}

impl RecordGen for MessageData {
    type Header = MessageDataHeader;

    fn parse_data<R: Read + Seek>(r: R, header: Self::Header) -> Result<Self> {
        let conn_id = header.conn_id.ok_or(Error::InvalidHeader)?;
        let time = header.time.ok_or(Error::InvalidHeader)?;
        let data = read_to_vec(r)?;
        Ok(MessageData { conn_id, time, data })
    }
}

impl HeaderGen for MessageDataHeader {
    const OP: u8 = 0x02;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"conn" => set_field_u32(&mut self.conn_id, val)?,
            b"time" => set_field_time(&mut self.time, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
