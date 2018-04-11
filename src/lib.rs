//! Assumptions:
//! - `MessageData` can be stored ony as part of `Chunk` data
//!
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
pub mod msg_iter;
pub mod record_types;

pub use record::Record;

pub struct RecordIterator {
    file: BufReader<File>,
}

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

pub type Result<T> = result::Result<T, Error>;

impl RecordIterator {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        let mut buf = [0u8; 13];
        file.read_exact(&mut buf)?;
        if &buf != VERSION_STRING.as_bytes() {
            return Err(Error::UnsupportedVersion);
        }
        Ok(RecordIterator { file: file })
    }

    pub fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

impl Iterator for RecordIterator {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        match Record::next_record(&mut self.file) {
            Err(Error::Io(ref err)) if err.kind() == ErrorKind::UnexpectedEof
                => None,
            v => Some(v),
        }
    }
}
