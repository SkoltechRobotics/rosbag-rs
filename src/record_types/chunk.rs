use super::{RecordGen, HeaderGen, Error, Result};
use super::utils::{unknown_field, read_to_vec, set_field_u32};
use msg_iter::ChunkMessagesIterator;
use std::io::{Read, Seek};

/// Compression options for `Chunk` data.
#[derive(Debug, Clone, Copy)]
pub enum Compression {
    Bzip2,
    None,
}

impl Compression {
    fn decompress(&self, data: Vec<u8>) -> Result<Vec<u8>> {
        Ok(match self {
            Compression::Bzip2 => unimplemented!(),
            Compression::None => data,
        })
    }
}

/// Bulk storage with optional compression for messages data and connection
/// records.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Compression type for the data
    pub compression: Compression,
    /// Decompressed messages data and connection records
    pub data: Vec<u8>,
}

impl Chunk {
    pub fn iter(&self) -> ChunkMessagesIterator {
        ChunkMessagesIterator::new(&self.data)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChunkHeader {
    compression: Option<Compression>,
    size: Option<u32>,
}

impl RecordGen for Chunk {
    type Header = ChunkHeader;

    fn parse_data<R: Read + Seek>(r: R, header: Self::Header) -> Result<Self> {
        let compression = header.compression.ok_or(Error::InvalidHeader)?;
        let size = header.size.ok_or(Error::InvalidHeader)?;
        let data = compression.decompress(read_to_vec(r)?)?;
        if data.len() != size as usize { Err(Error::InvalidRecord)? }
        Ok(Self { compression, data })
    }
}

impl HeaderGen for ChunkHeader {
    const OP: u8 = 0x05;

    fn process_field(&mut self, name: &[u8], val: &[u8]) -> Result<()> {
        match name {
            b"compression" => {
                if self.compression.is_some() { Err(Error::InvalidHeader)? }
                self.compression = Some(match val {
                    b"none" => Compression::None,
                    b"bzip2" => Compression::Bzip2,
                    _ => Err(Error::InvalidRecord)?,
                });
            },
            b"size" => set_field_u32(&mut self.size, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
