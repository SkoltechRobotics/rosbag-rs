//! Collection of record types.
use std::io::{Read, Seek};
use super::{Result, Error};

mod bag_header;
pub use self::bag_header::BagHeader;
mod chunk;
pub use self::chunk::{Chunk, Compression};
pub(crate) mod message_data;
pub use self::message_data::MessageData;
pub(crate) mod connection;
pub use self::connection::Connection;
mod index_data;
pub use self::index_data::{IndexData, IndexDataEntry};
mod chunk_info;
pub use self::chunk_info::{ChunkInfo, ChunkInfoEntry};

mod utils;
use self::utils::{read_record, check_op};

pub(crate) trait HeaderGen: Sized + Default {
    const OP: u8;

    fn read_header(mut header: &[u8]) -> Result<Self> {
        let mut rec = Self::default();
        while header.len() != 0 {
            let (name, val, new_header) = read_record(header)?;
            header = new_header;
            if name == b"op" {
                check_op(val, Self::OP)?;
            } else {
                rec.process_field(name, val)?;
            }
        }
        Ok(rec)
    }

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> ;
}

pub(crate) trait RecordGen: Sized {
    /// `op` header field value
    const OP: u8 = Self::Header::OP;
    /// Type which holds header information
    type Header: HeaderGen;

    fn read<R: Read + Seek>(header: &[u8], r: R) -> Result<Self> {
        let header = Self::Header::read_header(header)?;
        Self::parse_data(r, header)
    }

    fn parse_data<R: Read + Seek>(r: R, h: Self::Header) -> Result<Self>;
}
