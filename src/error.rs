use crate::cursor::OutOfBounds;
use std::convert::From;
use std::fmt;

/// The error type for ROS bag file reading and parsing.
#[derive(Debug)]
pub enum Error {
    /// Invalid headed.
    InvalidHeader,
    /// Invalid record.
    InvalidRecord,
    /// Encountered unsupported version in record.
    UnsupportedVersion,
    /// Tried to access outside of rosbag file.
    OutOfBounds,
    /// Got unexpected record type in the chunk section.
    UnexpectedChunkSectionRecord(&'static str),
    /// Got unexpected record type in the index section.
    UnexpectedIndexSectionRecord(&'static str),
    /// Got unexpected record type inside [`Chunk`][crate::record_types::Chunk] payload.
    UnexpectedMessageRecord(&'static str),
    /// Bzip2 decompression failure.
    Bzip2DecompressionError(String),
    /// Lz4 decompression failure.
    Lz4DecompressionError(String),
}

impl From<OutOfBounds> for Error {
    fn from(_: OutOfBounds) -> Error {
        Error::OutOfBounds
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Error::*;
        let s = match self {
            InvalidHeader => "invalid header".to_string(),
            InvalidRecord => "invalid record".to_string(),
            UnsupportedVersion => "unsupported version".to_string(),
            OutOfBounds => "out of bounds".to_string(),
            UnexpectedChunkSectionRecord(t) => format!("unexpected {} in the chunk section", t),
            UnexpectedIndexSectionRecord(t) => format!("unexpected {} in the index section", t),
            UnexpectedMessageRecord(t) => format!("unexpected {} in chunk payload", t),
            Bzip2DecompressionError(e) => format!("bzip2 decompression error: {}", e),
            Lz4DecompressionError(e) => format!("LZ4 decompression error: {}", e),
        };
        write!(f, "rosbag::Error: {}", s)
    }
}

impl std::error::Error for Error {}
