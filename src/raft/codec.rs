//! Binary encoding helpers shared by the Raft log and the Raft RPC messages.
//!
//! All integers are big-endian; strings are a `u32` byte length plus UTF-8.

use std::io;

use crate::raft::types::{Command, LogEntry};

const COMMAND_SET: u8 = 1;
const COMMAND_DELETE: u8 = 2;
const COMMAND_NOOP: u8 = 3;

pub fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub fn put_string(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();

    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string is too large"))?;

    put_u32(out, length);
    out.extend_from_slice(bytes);

    Ok(())
}

pub fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> io::Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "byte string is too large"))?;

    put_u32(out, length);
    out.extend_from_slice(value);

    Ok(())
}

pub fn put_command(out: &mut Vec<u8>, command: &Command) -> io::Result<()> {
    match command {
        Command::Set { key, value } => {
            out.push(COMMAND_SET);
            put_string(out, key)?;
            put_string(out, value)?;
        }
        Command::Delete { key } => {
            out.push(COMMAND_DELETE);
            put_string(out, key)?;
        }
        Command::Noop => out.push(COMMAND_NOOP),
    }

    Ok(())
}

/// Term followed by the command; the log prepends its own index.
pub fn put_entry(out: &mut Vec<u8>, entry: &LogEntry) -> io::Result<()> {
    put_u64(out, entry.term);
    put_command(out, &entry.command)
}

pub fn read_u8(bytes: &[u8], cursor: &mut usize) -> io::Result<u8> {
    let value = *bytes
        .get(*cursor)
        .ok_or_else(|| invalid_data("truncated byte"))?;
    *cursor += 1;

    Ok(value)
}

pub fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| invalid_data("record offset overflow"))?;

    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated integer"))?;

    let mut array = [0u8; 4];
    array.copy_from_slice(slice);
    *cursor = end;

    Ok(u32::from_be_bytes(array))
}

pub fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| invalid_data("record offset overflow"))?;

    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated integer"))?;

    let mut array = [0u8; 8];
    array.copy_from_slice(slice);
    *cursor = end;

    Ok(u64::from_be_bytes(array))
}

pub fn read_string(bytes: &[u8], cursor: &mut usize) -> io::Result<String> {
    let length = read_u32(bytes, cursor)? as usize;

    let end = cursor
        .checked_add(length)
        .ok_or_else(|| invalid_data("string length overflow"))?;

    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated string"))?;

    let value = std::str::from_utf8(slice)
        .map_err(|_| invalid_data("string is not valid UTF-8"))?
        .to_owned();

    *cursor = end;

    Ok(value)
}

pub fn read_bytes(bytes: &[u8], cursor: &mut usize) -> io::Result<Vec<u8>> {
    let length = read_u32(bytes, cursor)? as usize;

    let end = cursor
        .checked_add(length)
        .ok_or_else(|| invalid_data("byte string length overflow"))?;

    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated byte string"))?;

    *cursor = end;

    Ok(slice.to_vec())
}

pub fn read_command(bytes: &[u8], cursor: &mut usize) -> io::Result<Command> {
    match read_u8(bytes, cursor)? {
        COMMAND_SET => Ok(Command::Set {
            key: read_string(bytes, cursor)?,
            value: read_string(bytes, cursor)?,
        }),
        COMMAND_DELETE => Ok(Command::Delete {
            key: read_string(bytes, cursor)?,
        }),
        COMMAND_NOOP => Ok(Command::Noop),
        _ => Err(invalid_data("unknown Raft command type")),
    }
}

pub fn read_entry(bytes: &[u8], cursor: &mut usize) -> io::Result<LogEntry> {
    let term = read_u64(bytes, cursor)?;
    let command = read_command(bytes, cursor)?;

    Ok(LogEntry { term, command })
}
