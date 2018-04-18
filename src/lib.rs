//! Utilities for reading ROS bag files.
//!
//! # Example
//! ```ignore
//! use rosbag::{RecordsIterator, Record};
//!
//! let mut bag = RecordsIterator::new(path).unwrap();
//! let header = match bag.next() {
//!     Some(Ok(Record::BagHeader(bh))) => bh,
//!     _ => panic!("Failed to acquire bag header record"),
//! };
//! // get first chunk and iterate over messages in it
//! for record in &mut bag {
//!     let record = record.unwrap();
//!     match record {
//!         Record::Chunk(chunk) => {
//!             for msg in chunk.iter_msgs() {
//!                 let msg = msg.unwrap();
//!                 println!("{}", msg.time)
//!             }
//!             break;
//!         },
//!         _ => (),
//!     }
//! }
//! // jump to index records
//! bag.seek(header.index_pos).unwrap();
//! for record in bag {
//!     let record = record.unwrap();
//!     println!("{:?}", record);
//! }
//! ```
extern crate byteorder;
extern crate hex;
#[macro_use] extern crate log;
extern crate memmap;

use std::fs::File;
use std::path::Path;
use std::{result, str};
use std::convert::From;
use std::iter::Iterator;
use std::io::{self, Read};

use memmap::Mmap;

const VERSION_STRING: &str = "#ROSBAG V2.0\n";

mod record;
mod field_iter;
mod cursor;
pub mod msg_iter;
pub mod record_types;

pub use record::Record;

use cursor::{Cursor, OutOfBounds};


/// Low-level iterator over records extracted from ROS bag file.
pub struct RosBag {
    data: Mmap,
}

/// The error type for ROS bag file reading and parsing.
#[derive(Debug)]
pub enum Error {
    InvalidHeader,
    InvalidRecord,
    UnsupportedVersion,
    OutOfBounds,
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(val: io::Error) -> Error {
        Error::Io(val)
    }
}

impl From<OutOfBounds> for Error {
    fn from(_: OutOfBounds) -> Error {
        Error::OutOfBounds
    }
}

/// A specialized Result type for ROS bag file reading and parsing.
pub type Result<T> = result::Result<T, Error>;

impl RosBag {
    /// Create a new iterator over provided path to ROS bag file.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buf = [0u8; 13];
        file.read_exact(&mut buf)?;
        if &buf != VERSION_STRING.as_bytes() {
            Err(io::Error::new(io::ErrorKind::InvalidData,
                "Invalid or unsupported rosbag header"))?;
        }
        let data = unsafe { Mmap::map(&file)? };
        Ok(Self { data })
    }

    pub fn records<'a>(&'a self) -> RecordsIterator<'a> {
        let mut cursor = Cursor::new(&self.data);
        cursor.seek(13).expect("13 bytes already have been read");
        RecordsIterator { cursor }
    }
}

pub struct RecordsIterator<'a> {
    cursor: Cursor<'a>,
}

impl<'a> RecordsIterator<'a> {
    /// Jump to the given position in the file.
    ///
    /// Be carefull to jump only to record beginnings (e.g. to position listed
    /// in `BagHeader` or `ChunkInfo` records), as incorrect offset position
    /// will result in error on the next iteration and in the worst case
    /// scenario to a long blocking (programm will try to read a huge chunk of
    /// data).
    pub fn seek(&mut self, pos: u64) -> Result<()> {
        Ok(self.cursor.seek(pos)?)
    }
}

impl<'a> Iterator for RecordsIterator<'a> {
    type Item = Result<Record<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.left() == 0 { return None; }
        Some(Record::next_record(&mut self.cursor))
    }
}
