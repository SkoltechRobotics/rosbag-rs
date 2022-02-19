//! Utilities for efficient reading of ROS bag files.
//!
//! # Example
//! ```ignore
//! use rosbag::{RosBag, Record};
//!
//! let bag = RosBag::new(path).unwrap();
//! // create low-level iterator over rosbag records
//! let mut records = bag.records();
//! // acquire `BagHeader` record, which should be first one
//! let header = match records.next() {
//!     Some(Ok(Record::BagHeader(bh))) => bh,
//!     _ => panic!("Failed to acquire bag header record"),
//! };
//! // get first `Chunk` record and iterate over `Message` records in it
//! for record in &mut records {
//!     match record? {
//!         Record::Chunk(chunk) => {
//!             for msg in chunk.iter_msgs() {
//!                 println!("{}", msg?.time)
//!             }
//!             break;
//!         },
//!         _ => (),
//!     }
//! }
//! // jump to index records
//! records.seek(header.index_pos).unwrap();
//! for record in records {
//!     println!("{:?}", record?);
//! }
//! ```
#![doc(html_root_url = "https://docs.rs/rosbag/0.4.0")]
#![warn(missing_docs, rust_2018_idioms)]

use std::fs::File;
use std::io::{self, Read};
use std::iter::Iterator;
use std::path::Path;
use std::{result, str};

use memmap2::Mmap;

const VERSION_STRING: &str = "#ROSBAG V2.0\n";

mod cursor;
mod error;
mod field_iter;
pub mod msg_iter;
mod record;
pub mod record_types;

use cursor::Cursor;
pub use error::Error;
pub use record::Record;

/// Struct which holds open rosbag file.
pub struct RosBag {
    data: Mmap,
}

/// A specialized Result type for ROS bag file reading and parsing.
pub type Result<T> = result::Result<T, Error>;

impl RosBag {
    /// Create a new iterator over provided path to ROS bag file.
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let mut buf = [0u8; 13];
        file.read_exact(&mut buf)?;
        if buf != VERSION_STRING.as_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid or unsupported rosbag header",
            ));
        }
        let data = unsafe { Mmap::map(&file)? };
        Ok(Self { data })
    }

    /// Get iterator over records.
    pub fn records(&self) -> RecordsIterator<'_> {
        let mut cursor = Cursor::new(&self.data);
        cursor
            .seek(VERSION_STRING.len() as u64)
            .expect("data header is checked on initialization");
        RecordsIterator { cursor }
    }
}

/// Low-level iterator over records extracted from ROS bag file.
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
        if self.cursor.left() == 0 {
            return None;
        }
        Some(Record::next_record(&mut self.cursor))
    }
}
