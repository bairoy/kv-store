//! Binary encoding helpers shared by the Raft log and the Raft RPC messages.

use std::io;

use crate::raft::types::{ClusterConfig, Command, LogEntry, NodeId, SessionOp, TxnCommand, TxnOp};

const COMMAND_SET: u8 = 1;
const COMMAND_DELETE: u8 = 2;
const COMMAND_NOOP: u8 = 3;
const COMMAND_CONFIG: u8 = 4;
const COMMAND_TXN: u8 = 5;
const COMMAND_SESSION: u8 = 6;

const SESSION_SET: u8 = 1;
const SESSION_DELETE: u8 = 2;
const SESSION_INCR: u8 = 3;

const TXN_PREPARE: u8 = 1;
const TXN_DECIDE: u8 = 2;
const TXN_FINISH: u8 = 3;
const OP_SET: u8 = 1;
const OP_DELETE: u8 = 2;

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
        Command::Config(config) => {
            out.push(COMMAND_CONFIG);
            put_config(out, config)?;
        }
        Command::Txn(txn) => {
            out.push(COMMAND_TXN);
            put_txn(out, txn)?;
        }
        Command::Session { client, seq, op } => {
            out.push(COMMAND_SESSION);
            put_string(out, client)?;
            put_u64(out, *seq);
            put_session_op(out, op)?;
        }
    }

    Ok(())
}

fn put_session_op(out: &mut Vec<u8>, op: &SessionOp) -> io::Result<()> {
    match op {
        SessionOp::Set { key, value } => {
            out.push(SESSION_SET);
            put_string(out, key)?;
            put_string(out, value)?;
        }
        SessionOp::Delete { key } => {
            out.push(SESSION_DELETE);
            put_string(out, key)?;
        }
        SessionOp::Incr { key } => {
            out.push(SESSION_INCR);
            put_string(out, key)?;
        }
    }

    Ok(())
}

fn read_session_op(bytes: &[u8], cursor: &mut usize) -> io::Result<SessionOp> {
    match read_u8(bytes, cursor)? {
        SESSION_SET => Ok(SessionOp::Set {
            key: read_string(bytes, cursor)?,
            value: read_string(bytes, cursor)?,
        }),
        SESSION_DELETE => Ok(SessionOp::Delete {
            key: read_string(bytes, cursor)?,
        }),
        SESSION_INCR => Ok(SessionOp::Incr {
            key: read_string(bytes, cursor)?,
        }),
        _ => Err(invalid_data("unknown session operation type")),
    }
}

pub fn put_txn(out: &mut Vec<u8>, txn: &TxnCommand) -> io::Result<()> {
    match txn {
        TxnCommand::Prepare {
            id,
            coordinator,
            participants,
            ops,
        } => {
            out.push(TXN_PREPARE);
            put_string(out, id)?;
            put_u32(out, *coordinator);

            put_count(out, participants.len())?;
            for participant in participants {
                put_u32(out, *participant);
            }

            put_ops(out, ops)?;
        }
        TxnCommand::Decide { id, commit } => {
            out.push(TXN_DECIDE);
            put_string(out, id)?;
            out.push(u8::from(*commit));
        }
        TxnCommand::Finish { id, commit } => {
            out.push(TXN_FINISH);
            put_string(out, id)?;
            out.push(u8::from(*commit));
        }
    }

    Ok(())
}

fn put_count(out: &mut Vec<u8>, count: usize) -> io::Result<()> {
    let count = u32::try_from(count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "list is too long"))?;

    put_u32(out, count);

    Ok(())
}

pub fn put_ops(out: &mut Vec<u8>, ops: &[TxnOp]) -> io::Result<()> {
    put_count(out, ops.len())?;

    for op in ops {
        match op {
            TxnOp::Set { key, value } => {
                out.push(OP_SET);
                put_string(out, key)?;
                put_string(out, value)?;
            }
            TxnOp::Delete { key } => {
                out.push(OP_DELETE);
                put_string(out, key)?;
            }
        }
    }

    Ok(())
}

pub fn put_config(out: &mut Vec<u8>, config: &ClusterConfig) -> io::Result<()> {
    for list in [&config.voters, &config.learners] {
        let count = u32::try_from(list.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many servers"))?;

        put_u32(out, count);

        for id in list {
            put_u64(out, *id);
        }
    }

    Ok(())
}

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

pub fn read_config(bytes: &[u8], cursor: &mut usize) -> io::Result<ClusterConfig> {
    let mut lists: [Vec<NodeId>; 2] = [Vec::new(), Vec::new()];

    for list in &mut lists {
        let count = read_u32(bytes, cursor)? as usize;

        if count > bytes.len().saturating_sub(*cursor) / 8 {
            return Err(invalid_data("server list is longer than the record"));
        }

        for _ in 0..count {
            list.push(read_u64(bytes, cursor)?);
        }
    }

    let [voters, learners] = lists;

    Ok(ClusterConfig { voters, learners })
}

pub fn read_ops(bytes: &[u8], cursor: &mut usize) -> io::Result<Vec<TxnOp>> {
    let count = read_u32(bytes, cursor)? as usize;

    if count > bytes.len().saturating_sub(*cursor) / 5 {
        return Err(invalid_data("op list is longer than the record"));
    }

    let mut ops = Vec::with_capacity(count);

    for _ in 0..count {
        ops.push(match read_u8(bytes, cursor)? {
            OP_SET => TxnOp::Set {
                key: read_string(bytes, cursor)?,
                value: read_string(bytes, cursor)?,
            },
            OP_DELETE => TxnOp::Delete {
                key: read_string(bytes, cursor)?,
            },
            _ => return Err(invalid_data("unknown transaction op")),
        });
    }

    Ok(ops)
}

fn read_flag(bytes: &[u8], cursor: &mut usize) -> io::Result<bool> {
    match read_u8(bytes, cursor)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invalid_data("invalid flag")),
    }
}

pub fn read_txn(bytes: &[u8], cursor: &mut usize) -> io::Result<TxnCommand> {
    match read_u8(bytes, cursor)? {
        TXN_PREPARE => {
            let id = read_string(bytes, cursor)?;
            let coordinator = read_u32(bytes, cursor)?;

            let count = read_u32(bytes, cursor)? as usize;

            if count > bytes.len().saturating_sub(*cursor) / 4 {
                return Err(invalid_data("participant list is longer than the record"));
            }

            let mut participants = Vec::with_capacity(count);
            for _ in 0..count {
                participants.push(read_u32(bytes, cursor)?);
            }

            Ok(TxnCommand::Prepare {
                id,
                coordinator,
                participants,
                ops: read_ops(bytes, cursor)?,
            })
        }
        TXN_DECIDE => Ok(TxnCommand::Decide {
            id: read_string(bytes, cursor)?,
            commit: read_flag(bytes, cursor)?,
        }),
        TXN_FINISH => Ok(TxnCommand::Finish {
            id: read_string(bytes, cursor)?,
            commit: read_flag(bytes, cursor)?,
        }),
        _ => Err(invalid_data("unknown transaction step")),
    }
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
        COMMAND_CONFIG => Ok(Command::Config(read_config(bytes, cursor)?)),
        COMMAND_TXN => Ok(Command::Txn(read_txn(bytes, cursor)?)),
        COMMAND_SESSION => Ok(Command::Session {
            client: read_string(bytes, cursor)?,
            seq: read_u64(bytes, cursor)?,
            op: read_session_op(bytes, cursor)?,
        }),
        _ => Err(invalid_data("unknown Raft command type")),
    }
}

pub fn read_entry(bytes: &[u8], cursor: &mut usize) -> io::Result<LogEntry> {
    let term = read_u64(bytes, cursor)?;
    let command = read_command(bytes, cursor)?;

    Ok(LogEntry { term, command })
}
