use byteorder::{LE, ByteOrder};

pub(crate) struct Cursor<'a> {
    data: &'a [u8],
    pos: u64,
}

#[derive(Debug, Copy, Clone)]
pub struct OutOfBounds;

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn seek(&mut self, pos: u64) -> Result<(), OutOfBounds> {
        if pos > self.len() { Err(OutOfBounds)? }
        self.pos = pos;
        Ok(())
    }

    pub fn pos(&self) -> u64 { self.pos }

    pub fn len(&self) -> u64 { self.data.len() as u64 }

    pub fn left(&self) -> u64 { self.data.len() as u64 - self.pos() }

    pub fn next_bytes(&mut self, n: u64) -> Result<&'a [u8], OutOfBounds> {
        if self.pos + n > self.len() { Err(OutOfBounds)? }
        let s = self.pos as usize;
        self.pos += n;
        Ok(&self.data[s..self.pos as usize])
    }

    pub fn next_chunk(&mut self) -> Result<&'a [u8], OutOfBounds> {
        let n = self.next_u32()? as u64;
        Ok(self.next_bytes(n)?)
    }

    pub fn next_u32(&mut self) -> Result<u32, OutOfBounds> {
        Ok(LE::read_u32(self.next_bytes(4)?))
    }

    /*
    pub fn next_u64(&mut self) -> Result<u64, OutOfBounds> {
        Ok(LE::read_u64(self.next_bytes(4)?))
    }
    */

    pub fn next_time(&mut self) -> Result<u64, OutOfBounds> {
        let s = self.next_u32()? as u64;
        let ns = self.next_u32()? as u64;
        Ok(1_000_000_000*s + ns)
    }
}
