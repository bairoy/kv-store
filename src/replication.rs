use std::io;

use crate::replication_log::{LogEntry, ReplicationEntry};

const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const SNAPSHOT_CHUNK_OVERHEAD: usize = 1 + 8 + 4 + 4;
pub const MAX_SNAPSHOT_CHUNK_SIZE: usize = MAX_MESSAGE_SIZE - SNAPSHOT_CHUNK_OVERHEAD;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicationMessage {
    Hello {
        last_offset: u64,
    },
    Entry(ReplicationEntry),
    Ack {
        offset: u64,
    },
    SnapshotBegin {
        last_included_offset: u64,
        total_bytes: u64,
    },
    SnapshotChunk {
        last_included_offset: u64,
        chunk_index: u32,
        data: Vec<u8>,
    },
    SnapshotEnd {
        last_included_offset: u64,
        total_chunks: u32,
    },
}

pub fn encode_message(message: &ReplicationMessage) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();

    match message {
        ReplicationMessage::Hello { last_offset } => {
            out.push(1);
            put_u64(&mut out, *last_offset);
        }

        ReplicationMessage::Entry(entry) => {
            if entry.offset == 0 {
                return Err(invalid_input("entry offset must be positive"));
            }

            out.push(2);
            put_u64(&mut out, entry.offset);

            match &entry.operation {
                LogEntry::Set { key, value } => {
                    out.push(1);
                    put_string(&mut out, key)?;
                    put_string(&mut out, value)?;
                }

                LogEntry::Delete { key } => {
                    out.push(2);
                    put_string(&mut out, key)?;
                }
            }
        }

        ReplicationMessage::Ack { offset } => {
            out.push(3);
            put_u64(&mut out, *offset);
        }

        ReplicationMessage::SnapshotBegin {
            last_included_offset,
            total_bytes,
        } => {
            out.push(4);
            put_u64(&mut out, *last_included_offset);
            put_u64(&mut out, *total_bytes);
        }

        ReplicationMessage::SnapshotChunk {
            last_included_offset,
            chunk_index,
            data,
        } => {
            if data.is_empty() || data.len() > MAX_SNAPSHOT_CHUNK_SIZE {
                return Err(invalid_input("invalid snapshot chunk size"));
            }

            let length = u32::try_from(data.len())
                .map_err(|_| invalid_input("snapshot chunk is too large"))?;

            out.push(5);
            put_u64(&mut out, *last_included_offset);
            put_u32(&mut out, *chunk_index);
            put_u32(&mut out, length);
            out.extend_from_slice(data);
        }

        ReplicationMessage::SnapshotEnd {
            last_included_offset,
            total_chunks,
        } => {
            out.push(6);
            put_u64(&mut out, *last_included_offset);
            put_u32(&mut out, *total_chunks);
        }
    }

    if out.len() > MAX_MESSAGE_SIZE {
        return Err(invalid_input("replication message is too large"));
    }

    Ok(out)
}

pub fn decode_message(bytes: &[u8]) -> io::Result<ReplicationMessage> {
    if bytes.is_empty() || bytes.len() > MAX_MESSAGE_SIZE {
        return Err(invalid_record());
    }

    let mut cursor = 0;
    let tag = read_u8(bytes, &mut cursor)?;

    let message = match tag {
        1 => ReplicationMessage::Hello {
            last_offset: read_u64(bytes, &mut cursor)?,
        },

        2 => {
            let offset = read_u64(bytes, &mut cursor)?;

            if offset == 0 {
                return Err(invalid_record());
            }

            let operation = match read_u8(bytes, &mut cursor)? {
                1 => LogEntry::Set {
                    key: read_string(bytes, &mut cursor)?,
                    value: read_string(bytes, &mut cursor)?,
                },
                2 => LogEntry::Delete {
                    key: read_string(bytes, &mut cursor)?,
                },
                _ => return Err(invalid_record()),
            };

            ReplicationMessage::Entry(ReplicationEntry { offset, operation })
        }

        3 => ReplicationMessage::Ack {
            offset: read_u64(bytes, &mut cursor)?,
        },

        4 => ReplicationMessage::SnapshotBegin {
            last_included_offset: read_u64(bytes, &mut cursor)?,
            total_bytes: read_u64(bytes, &mut cursor)?,
        },

        5 => {
            let last_included_offset = read_u64(bytes, &mut cursor)?;
            let chunk_index = read_u32(bytes, &mut cursor)?;
            let length = read_u32(bytes, &mut cursor)? as usize;

            if length == 0 || length > MAX_SNAPSHOT_CHUNK_SIZE {
                return Err(invalid_record());
            }

            let end = cursor.checked_add(length).ok_or_else(invalid_record)?;
            let data = bytes.get(cursor..end).ok_or_else(invalid_record)?.to_vec();

            cursor = end;

            ReplicationMessage::SnapshotChunk {
                last_included_offset,
                chunk_index,
                data,
            }
        }

        6 => ReplicationMessage::SnapshotEnd {
            last_included_offset: read_u64(bytes, &mut cursor)?,
            total_chunks: read_u32(bytes, &mut cursor)?,
        },

        _ => return Err(invalid_record()),
    };

    if cursor != bytes.len() {
        return Err(invalid_record());
    }

    Ok(message)
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_string(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();

    let length = u32::try_from(bytes.len()).map_err(|_| invalid_input("string is too large"))?;

    put_u32(out, length);
    out.extend_from_slice(bytes);

    Ok(())
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> io::Result<u8> {
    let value = *bytes.get(*cursor).ok_or_else(invalid_record)?;
    *cursor += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let end = cursor.checked_add(4).ok_or_else(invalid_record)?;
    let slice = bytes.get(*cursor..end).ok_or_else(invalid_record)?;

    let mut array = [0u8; 4];
    array.copy_from_slice(slice);
    *cursor = end;

    Ok(u32::from_be_bytes(array))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.checked_add(8).ok_or_else(invalid_record)?;
    let slice = bytes.get(*cursor..end).ok_or_else(invalid_record)?;

    let mut array = [0u8; 8];
    array.copy_from_slice(slice);
    *cursor = end;

    Ok(u64::from_be_bytes(array))
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> io::Result<String> {
    let length = read_u32(bytes, cursor)? as usize;
    let end = cursor.checked_add(length).ok_or_else(invalid_record)?;
    let slice = bytes.get(*cursor..end).ok_or_else(invalid_record)?;

    let value = std::str::from_utf8(slice)
        .map_err(|_| invalid_record())?
        .to_owned();

    *cursor = end;
    Ok(value)
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_record() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid replication message")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(message: ReplicationMessage) {
        let encoded = encode_message(&message).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn hello_round_trip() {
        round_trip(ReplicationMessage::Hello { last_offset: 42 });
    }

    #[test]
    fn set_entry_round_trip() {
        round_trip(ReplicationMessage::Entry(ReplicationEntry {
            offset: 7,
            operation: LogEntry::Set {
                key: "name".into(),
                value: "Baiju".into(),
            },
        }));
    }

    #[test]
    fn delete_entry_round_trip() {
        round_trip(ReplicationMessage::Entry(ReplicationEntry {
            offset: 8,
            operation: LogEntry::Delete { key: "name".into() },
        }));
    }

    #[test]
    fn ack_round_trip() {
        round_trip(ReplicationMessage::Ack { offset: 8 });
    }

    #[test]
    fn snapshot_messages_round_trip() {
        round_trip(ReplicationMessage::SnapshotBegin {
            last_included_offset: 100,
            total_bytes: 10_000,
        });

        round_trip(ReplicationMessage::SnapshotChunk {
            last_included_offset: 100,
            chunk_index: 0,
            data: vec![42; 1024],
        });

        round_trip(ReplicationMessage::SnapshotEnd {
            last_included_offset: 100,
            total_chunks: 10,
        });
    }

    #[test]
    fn rejects_invalid_messages() {
        assert!(decode_message(&[]).is_err());
        assert!(decode_message(&[99]).is_err());
        assert!(decode_message(&[1, 0]).is_err());

        assert!(
            encode_message(&ReplicationMessage::SnapshotChunk {
                last_included_offset: 10,
                chunk_index: 0,
                data: Vec::new(),
            })
            .is_err()
        );
    }
}
