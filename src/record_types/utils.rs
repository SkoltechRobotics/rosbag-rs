use super::{Error, Result};
use byteorder::{ByteOrder, LE};
use std::str;

pub(crate) fn read_record(mut header: &[u8]) -> Result<(&str, &[u8], &[u8])> {
    if header.len() < 4 {
        return Err(Error::InvalidHeader);
    }
    let n = LE::read_u32(&header[..4]) as usize;
    header = &header[4..];

    if header.len() < n {
        return Err(Error::InvalidHeader);
    }
    let rec = &header[..n];
    header = &header[n..];

    let mut delim = 0;
    for (i, b) in rec.iter().enumerate() {
        #[allow(clippy::match_overlapping_arm)]
        match *b {
            b'=' => {
                delim = i;
                break;
            }
            0x20..=0x7e => (),
            _ => return Err(Error::InvalidHeader),
        }
    }
    if delim == 0 {
        return Err(Error::InvalidHeader);
    }
    // SAFETY: the string is already checked
    let name = unsafe { str::from_utf8_unchecked(&rec[..delim]) };
    let val = &rec[delim + 1..];
    Ok((name, val, header))
}

pub(crate) fn unknown_field(name: &str, val: &[u8]) {
    log::warn!("Unknown header field: {}={:?}", name, val);
}

pub(crate) fn check_op(val: &[u8], op: u8) -> Result<()> {
    if val.len() == 1 && val[0] == op {
        Ok(())
    } else {
        Err(Error::InvalidRecord)
    }
}

pub(crate) fn set_field_u64(field: &mut Option<u64>, val: &[u8]) -> Result<()> {
    if val.len() != 8 || field.is_some() {
        Err(Error::InvalidHeader)
    } else {
        *field = Some(LE::read_u64(val));
        Ok(())
    }
}

pub(crate) fn set_field_u32(field: &mut Option<u32>, val: &[u8]) -> Result<()> {
    if val.len() != 4 || field.is_some() {
        Err(Error::InvalidHeader)
    } else {
        *field = Some(LE::read_u32(val));
        Ok(())
    }
}

pub(crate) fn set_field_str<'a>(field: &mut Option<&'a str>, val: &'a [u8]) -> Result<()> {
    if field.is_some() {
        return Err(Error::InvalidHeader);
    }
    *field = Some(str::from_utf8(val).map_err(|_| Error::InvalidHeader)?);
    Ok(())
}

pub(crate) fn set_field_time(field: &mut Option<u64>, val: &[u8]) -> Result<()> {
    if val.len() != 8 || field.is_some() {
        return Err(Error::InvalidHeader);
    }
    let s = LE::read_u32(&val[..4]) as u64;
    let ns = LE::read_u32(&val[4..]) as u64;
    *field = Some(1_000_000_000 * s + ns);
    Ok(())
}
