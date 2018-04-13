use std::io::Read;
use byteorder::{LE, ByteOrder, ReadBytesExt};
use super::{Result, Error};
use std::str;

pub(super) fn read_record(mut header: &[u8]) -> Result<(&[u8], &[u8], &[u8])> {
    if header.len() < 4 { Err(Error::InvalidHeader)? }
    let n = LE::read_u32(&header[..4]) as usize;
    header = &header[4..];

    if header.len() < n { Err(Error::InvalidHeader)? }
    let rec = &header[..n];
    header = &header[n..];

    let mut delim = 0;
    for (i, b) in rec.iter().enumerate() {
        match *b {
            b'=' => {
                delim = i;
                break;
            },
            0x20...0x7e => (),
            _ => Err(Error::InvalidHeader)?,
        }
    }
    if delim == 0 { Err(Error::InvalidHeader)? }
    let name = &rec[..delim];
    let val = &rec[delim+1..];
    Ok((name, val, header))
}

pub(super) fn read_to_vec<R: Read>(mut r: R) -> Result<Vec<u8>> {
    let n = r.read_u32::<LE>()? as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

pub(super) fn unknown_field(name: &[u8], val: &[u8]) {
    warn!("Unknown header field: {}={:?}",
        str::from_utf8(name).expect("already checked"), val);
}

pub(super) fn check_op(val: &[u8], op: u8) -> Result<()> {
    if val.len() == 1 && val[0] == op {
        Ok(())
    } else {
        Err(Error::InvalidRecord)
    }
}

pub(super) fn set_field_u64(field: &mut Option<u64>, val: &[u8]) -> Result<()> {
    if val.len() != 8 { Err(Error::InvalidHeader)? }
    if field.is_some() { Err(Error::InvalidHeader)? }
    *field = Some(LE::read_u64(val));
    Ok(())
}

pub(super) fn set_field_u32(field: &mut Option<u32>, val: &[u8]) -> Result<()> {
    if val.len() != 4 { Err(Error::InvalidHeader)? }
    if field.is_some() { Err(Error::InvalidHeader)? }
    *field = Some(LE::read_u32(val));
    Ok(())
}

pub(super) fn set_field_string(field: &mut Option<String>, val: &[u8]) -> Result<()> {
    if field.is_some() { Err(Error::InvalidHeader)? }
    *field = Some(str::from_utf8(val)
        .map_err(|_| Error::InvalidHeader)?
        .to_string());
    Ok(())
}

pub(super) fn set_field_time(field: &mut Option<u64>, val: &[u8]) -> Result<()> {
    if val.len() != 8 { Err(Error::InvalidHeader)? }
    if field.is_some() { Err(Error::InvalidHeader)? }
    let s = LE::read_u32(&val[..4]) as u64;
    let ns = LE::read_u32(&val[4..]) as u64;
    *field = Some(1_000_000_000*s + ns);
    Ok(())
}
