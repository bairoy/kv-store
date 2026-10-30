use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

const HEADER_SIZE: u64 = 4;
const MAX_RECORD_SIZE: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogEntry {
    Set { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationEntry {
    pub offset: u64,
    pub operation: LogEntry,
}

pub struct ReplicationLog {
    file: File,
    entries: Vec<ReplicationEntry>,
    next_offset: u64,
}

impl ReplicationLog {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;

        let mut entries = Vec::new();
        let mut position = 0u64;
        let file_len = file.metadata()?.len();

        while position < file_len {
            file.seek(SeekFrom::Start(position))?;

            let mut header = [0u8; 4];

            if let Err(error) = file.read_exact(&mut header) {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(error);
            }

            let length = u32::from_be_bytes(header) as usize;

            if length == 0 || length > MAX_RECORD_SIZE {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid replication record length",
                ));
            }

            let record_end = position
                .checked_add(HEADER_SIZE)
                .and_then(|p| p.checked_add(length as u64))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "replication record length overflow",
                    )
                })?;

            if record_end > file_len {
                break;
            }

            let mut payload = vec![0u8; length];
            file.read_exact(&mut payload)?;

            let entry = decode_entry(&payload)?;

            let expected_offset = entries.len() as u64 + 1;

            if entry.offset != expected_offset {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "replication offsets are not consecutive",
                ));
            }

            entries.push(entry);
            position = record_end;
        }

        if position < file_len {
            file.set_len(position)?;
            file.sync_all()?;
        }

        file.seek(SeekFrom::End(0))?;

        let next_offset = entries.last().map_or(1, |entry| entry.offset + 1);

        Ok(Self {
            file,
            entries,
            next_offset,
        })
    }

    pub fn append(&mut self, operation: LogEntry) -> io::Result<ReplicationEntry> {
        let entry = ReplicationEntry {
            offset: self.next_offset,
            operation,
        };

        let payload = encode_entry(&entry)?;

        if payload.len() > MAX_RECORD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "replication entry is too large",
            ));
        }

        let length = u32::try_from(payload.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "replication entry is too large",
            )
        })?;

        self.file.write_all(&length.to_be_bytes())?;
        self.file.write_all(&payload)?;

        self.file.sync_all()?;

        self.entries.push(entry.clone());
        self.next_offset += 1;

        Ok(entry)
    }

    pub fn last_offset(&self) -> u64 {
        self.next_offset - 1
    }

    pub fn entries_after(&self, offset: u64) -> Vec<ReplicationEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.offset > offset)
            .cloned()
            .collect()
    }
}

fn encode_entry(entry: &ReplicationEntry) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();

    out.extend_from_slice(&entry.offset.to_be_bytes());

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

    Ok(out)
}

fn decode_entry(bytes: &[u8]) -> io::Result<ReplicationEntry> {
    let mut cursor = 0;

    let offset = read_u64(bytes, &mut cursor)?;
    let kind = *bytes.get(cursor).ok_or_else(invalid_record)?;
    cursor += 1;

    let operation = match kind {
        1 => LogEntry::Set {
            key: read_string(bytes, &mut cursor)?,
            value: read_string(bytes, &mut cursor)?,
        },
        2 => LogEntry::Delete {
            key: read_string(bytes, &mut cursor)?,
        },
        _ => return Err(invalid_record()),
    };

    if cursor != bytes.len() {
        return Err(invalid_record());
    }

    Ok(ReplicationEntry { offset, operation })
}

fn put_string(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();

    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "string is too large"))?;

    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(bytes);

    Ok(())
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
    let end = cursor.checked_add(4).ok_or_else(invalid_record)?;
    let slice = bytes.get(*cursor..end).ok_or_else(invalid_record)?;

    let mut array = [0u8; 4];
    array.copy_from_slice(slice);
    *cursor = end;

    let length = u32::from_be_bytes(array) as usize;
    let end = cursor.checked_add(length).ok_or_else(invalid_record)?;
    let slice = bytes.get(*cursor..end).ok_or_else(invalid_record)?;

    let value = std::str::from_utf8(slice)
        .map_err(|_| invalid_record())?
        .to_string();

    *cursor = end;
    Ok(value)
}

fn invalid_record() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid replication log record")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path() -> std::path::PathBuf {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        std::env::temp_dir().join(format!("replication-log-{id}.bin"))
    }

    #[test]
    fn offsets_survive_restart() {
        let path = temp_path();

        {
            let mut log = ReplicationLog::open(&path).unwrap();

            let first = log
                .append(LogEntry::Set {
                    key: "name".into(),
                    value: "Baiju".into(),
                })
                .unwrap();

            let second = log.append(LogEntry::Delete { key: "name".into() }).unwrap();

            assert_eq!(first.offset, 1);
            assert_eq!(second.offset, 2);
            assert_eq!(log.last_offset(), 2);
        }

        {
            let mut log = ReplicationLog::open(&path).unwrap();

            assert_eq!(log.last_offset(), 2);

            let third = log
                .append(LogEntry::Set {
                    key: "city".into(),
                    value: "Kathmandu".into(),
                })
                .unwrap();

            assert_eq!(third.offset, 3);
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn entries_after_returns_missing_operations() {
        let path = temp_path();
        let mut log = ReplicationLog::open(&path).unwrap();

        for i in 0..5 {
            log.append(LogEntry::Set {
                key: format!("key{i}"),
                value: format!("value{i}"),
            })
            .unwrap();
        }

        let missing = log.entries_after(3);

        assert_eq!(missing.len(), 2);
        assert_eq!(missing[0].offset, 4);
        assert_eq!(missing[1].offset, 5);

        drop(log);
        let _ = std::fs::remove_file(path);
    }
}
