use super::utils::{set_field_u32, unknown_field};
use super::{Error, HeaderGen, RecordGen, Result};
use std::borrow::Cow;

use crate::cursor::Cursor;
use crate::msg_iter::MessageRecordsIterator;

/// Compression options for `Chunk` data.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Compression {
    /// Bzip2 compression.
    Bzip2,
    /// Lz4 compression.
    Lz4,
    /// No compression.
    None,
}

impl Compression {
    fn decompress(self, data: &[u8], decompressed_size: Option<u32>) -> Result<Cow<'_, [u8]>> {
        Ok(match self {
            Compression::Bzip2 => {
                let mut decompressed = Vec::new();
                decompressed.reserve(decompressed_size.map(|s| s as usize).unwrap_or(data.len()));
                let mut decompressor = bzip2::Decompress::new(false);
                decompressor
                    .decompress_vec(data, &mut decompressed)
                    .map_err(|e| Error::Bzip2DecompressionError(e.to_string()))?;
                Cow::from(decompressed)
            }
            Compression::Lz4 => {
                let mut decoder = lz4::Decoder::new(data)
                    .map_err(|e| Error::Lz4DecompressionError(e.to_string()))?;
                let mut decompressed = Vec::new();
                decompressed.reserve(decompressed_size.map(|s| s as usize).unwrap_or(data.len()));
                std::io::copy(&mut decoder, &mut decompressed).map_err(|_| {
                    Error::Lz4DecompressionError("Error while decoding".to_string())
                })?;
                Cow::from(decompressed)
            }
            Compression::None => Cow::from(data),
        })
    }
}

/// Bulk storage with optional compression for messages data and connection
/// records.
#[derive(Debug, Clone)]
pub struct Chunk<'a> {
    /// Compression type for the data
    pub compression: Compression,
    /// Decompressed messages data and connection records
    data: Cow<'a, [u8]>,
}

impl<'a> Chunk<'a> {
    /// Get iterator over only messages
    pub fn messages(&self) -> MessageRecordsIterator<'_> {
        MessageRecordsIterator::new(&self.data)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ChunkHeader {
    compression: Option<Compression>,
    size: Option<u32>,
}

impl<'a> RecordGen<'a> for Chunk<'a> {
    type Header = ChunkHeader;

    fn read_data(c: &mut Cursor<'a>, header: Self::Header) -> Result<Self> {
        let compression = header.compression.ok_or(Error::InvalidHeader)?;
        let size = header.size.ok_or(Error::InvalidHeader)?;
        let data = compression.decompress(c.next_chunk()?, header.size)?;
        if data.len() != size as usize {
            return Err(Error::InvalidRecord);
        }
        Ok(Self { compression, data })
    }
}

impl<'a> HeaderGen<'a> for ChunkHeader {
    const OP: u8 = 0x05;

    fn process_field(&mut self, name: &str, val: &[u8]) -> Result<()> {
        match name {
            "compression" => {
                if self.compression.is_some() {
                    return Err(Error::InvalidHeader);
                }
                self.compression = Some(match val {
                    b"none" => Compression::None,
                    b"bz2" => Compression::Bzip2,
                    b"lz4" => Compression::Lz4,
                    _ => return Err(Error::InvalidHeader),
                });
            }
            "size" => set_field_u32(&mut self.size, val)?,
            _ => unknown_field(name, val),
        }
        Ok(())
    }
}
