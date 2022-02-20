use crate::record_types::{Chunk, IndexData};
use crate::{record::Record, Cursor, Error, Result};

/// Record types which can be stored in the chunk section.
#[derive(Debug, Clone)]
pub enum ChunkRecord<'a> {
    /// [`Chunk`] record.
    Chunk(Chunk<'a>),
    /// [`IndexData`] record.
    IndexData(IndexData<'a>),
}

/// Iterator over records stored in the chunk section of a rosbag file.
pub struct ChunkRecordsIterator<'a> {
    pub(crate) cursor: Cursor<'a>,
    pub(crate) offset: u64,
}

impl<'a> ChunkRecordsIterator<'a> {
    /// Jump to the given position in the file.
    ///
    /// Be carefull to jump only to record beginnings (e.g. to position listed
    /// in `ChunkInfo` records), as incorrect offset position
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

impl<'a> Iterator for ChunkRecordsIterator<'a> {
    type Item = Result<ChunkRecord<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor.left() == 0 {
            return None;
        }
        let res = match Record::next_record(&mut self.cursor) {
            Ok(Record::Chunk(v)) => Ok(ChunkRecord::Chunk(v)),
            Ok(Record::IndexData(v)) => Ok(ChunkRecord::IndexData(v)),
            Ok(v) => Err(Error::UnexpectedChunkSectionRecord(v.get_type())),
            Err(e) => Err(e),
        };
        Some(res)
    }
}
