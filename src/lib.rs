//! Utilities for reading ROS bag files.
extern crate byteorder;
extern crate hex;
#[macro_use] extern crate log;

use std::fs::File;
use std::path::Path;
use std::{result, str};
use std::convert::From;
use std::iter::Iterator;
use std::io::{self, BufReader, Read, ErrorKind, Seek, SeekFrom};

const VERSION_STRING: &'static str = "#ROSBAG V2.0\n";

mod record;
mod field_iter;
mod msg_iter;
pub mod record_types;

pub use record::Record;
pub use msg_iter::{ChunkMessagesIterator, MessageDataRef};

/// Low-level iterator over records extracted from ROS bag file.
pub struct RecordsIterator {
    file: BufReader<File>,
}

/// The error type for ROS bag file reading and parsing.
#[derive(Debug)]
pub enum Error {
    InvalidHeader,
    InvalidRecord,
    UnsupportedVersion,
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(val: io::Error) -> Error {
        Error::Io(val)
    }
}

/// A specialized Result type for ROS bag file reading and parsing.
pub type Result<T> = result::Result<T, Error>;

impl RecordsIterator {
    /// Create a new iterator over provided path to ROS bag file.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut buf = [0u8; 13];
        file.read_exact(&mut buf)?;
        if &buf != VERSION_STRING.as_bytes() {
            return Err(Error::UnsupportedVersion);
        }
        Ok(RecordsIterator { file: file })
    }

    /// Jump to the given position in the file.
    ///
    /// Be carefull to jump only to records beginning (e.g. to position listed
    /// in `BagHeader` or `ChunkInfo` records), as incorrect offset position
    /// will result in error on the next iteration and in the worst case
    /// scenario to a long blocking (programm will try to read a huge chunk of
    /// data).
    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Iterator for RecordsIterator {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        match Record::next_record(&mut self.file) {
            Err(Error::Io(ref err)) if err.kind() == ErrorKind::UnexpectedEof
                => None,
            v => Some(v),
        }
    }
}
