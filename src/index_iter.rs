use crate::record_types::{ChunkInfo, Connection, IndexData};
use crate::{record::Record, Cursor, Error, Result};

/// Record types which can be stored in the chunk section.
#[derive(Debug, Clone)]
pub enum IndexRecord<'a> {
    /// [`IndexData`] record.
    IndexData(IndexData<'a>),
    /// [`Connection`] record.
    Connection(Connection<'a>),
    /// [`ChunkInfo`] record.
    ChunkInfo(ChunkInfo<'a>),
}

/// Iterator over records stored in the chunk section of a rosbag file.
pub struct IndexRecordsIterator<'a> {
    pub(crate) cursor: Cursor<'a>,
    pub(crate) offset: u64,
}

impl<'a> IndexRecordsIterator<'a> {
    /// Jump to the given position in the file.
    ///
    /// Be carefull to jump only to record beginnings, as incorrect offset position
    /// will result in error on the next iteration and in the worst case
    /// scenario to a long blocking (programm will try to read a huge chunk of
    /// data).
    pub fn seek(&mut self, pos: u64) -> Result<()> {
        if pos < self.offset {
            return Err(Error::OutOfBounds);
        }
        Ok(self.cursor.seek(pos - self.offset)?)
    }
}

impl<'a> Iterator for IndexRecordsIterator<'a> {
    type Item = Result<IndexRecord<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.left() == 0 {
            return None;
        }
        let res = match Record::next_record(&mut self.cursor) {
            Ok(Record::IndexData(v)) => Ok(IndexRecord::IndexData(v)),
            Ok(Record::Connection(v)) => Ok(IndexRecord::Connection(v)),
            Ok(Record::ChunkInfo(v)) => Ok(IndexRecord::ChunkInfo(v)),
            Ok(v) => Err(Error::UnexpectedIndexSectionRecord(v.get_type())),
            Err(e) => Err(e),
        };
        Some(res)
    }
}
