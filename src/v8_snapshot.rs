use crate::replication::{MAX_SNAPSHOT_CHUNK_SIZE, ReplicationMessage};

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;

const MAGIC: &[u8; 8] = b"KVSSV001";
const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_STRING_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub last_included_offset: u64,
    pub entries: BTreeMap<String, String>,
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn put_string(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    let bytes = value.as_bytes();

    if bytes.len() > MAX_STRING_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "snapshot string is too large",
        ));
    }

    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "snapshot string is too large"))?;

    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(bytes);

    Ok(())
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> io::Result<u32> {
    let end = cursor
        .checked_add(4)
        .ok_or_else(|| invalid_data("snapshot cursor overflow"))?;

    let part = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated snapshot"))?;

    *cursor = end;

    Ok(u32::from_be_bytes(part.try_into().unwrap()))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| invalid_data("snapshot cursor overflow"))?;

    let part = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated snapshot"))?;

    *cursor = end;

    Ok(u64::from_be_bytes(part.try_into().unwrap()))
}

fn read_string(bytes: &[u8], cursor: &mut usize) -> io::Result<String> {
    let length = read_u32(bytes, cursor)? as usize;

    if length > MAX_STRING_BYTES {
        return Err(invalid_data("snapshot string is too large"));
    }

    let end = cursor
        .checked_add(length)
        .ok_or_else(|| invalid_data("snapshot cursor overflow"))?;

    let part = bytes
        .get(*cursor..end)
        .ok_or_else(|| invalid_data("truncated snapshot string"))?;

    *cursor = end;

    String::from_utf8(part.to_vec()).map_err(|_| invalid_data("snapshot contains invalid UTF-8"))
}

impl Snapshot {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let count = u64::try_from(self.entries.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "too many snapshot entries")
        })?;

        let mut out = Vec::new();

        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.last_included_offset.to_be_bytes());
        out.extend_from_slice(&count.to_be_bytes());

        for (key, value) in &self.entries {
            put_string(&mut out, key)?;
            put_string(&mut out, value)?;
        }

        if out.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot exceeds maximum size",
            ));
        }

        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
            return Err(invalid_data("snapshot exceeds maximum size"));
        }

        if bytes.len() < 24 || &bytes[..8] != MAGIC {
            return Err(invalid_data("invalid snapshot header"));
        }

        let mut cursor = 8;

        let last_included_offset = read_u64(bytes, &mut cursor)?;
        let count = read_u64(bytes, &mut cursor)?;

        if count > bytes.len() as u64 {
            return Err(invalid_data("invalid snapshot entry count"));
        }

        let mut entries = BTreeMap::new();

        for _ in 0..count {
            let key = read_string(bytes, &mut cursor)?;
            let value = read_string(bytes, &mut cursor)?;

            if entries.insert(key, value).is_some() {
                return Err(invalid_data("duplicate snapshot key"));
            }
        }

        if cursor != bytes.len() {
            return Err(invalid_data("trailing bytes in snapshot"));
        }

        Ok(Self {
            last_included_offset,
            entries,
        })
    }

    pub fn write_atomic(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        let temp_path = path.with_extension("snapshot.tmp");
        let bytes = self.encode()?;

        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)?;

            file.write_all(&bytes)?;
            file.sync_all()?;
        }

        fs::rename(&temp_path, path)?;

        #[cfg(unix)]
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }

        Ok(())
    }

    pub fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = File::open(path)?;

        let length = file.metadata()?.len();

        if length > MAX_SNAPSHOT_BYTES {
            return Err(invalid_data("snapshot exceeds maximum size"));
        }

        let mut bytes = Vec::with_capacity(length as usize);
        file.read_to_end(&mut bytes)?;

        Self::decode(&bytes)
    }

    pub fn to_messages(&self) -> io::Result<Vec<ReplicationMessage>> {
        let bytes = self.encode()?;
        let total_bytes = bytes.len() as u64;

        let chunks: Vec<&[u8]> = bytes.chunks(MAX_SNAPSHOT_CHUNK_SIZE).collect();

        let total_chunks = u32::try_from(chunks.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many snapshot chunks"))?;

        let mut messages = Vec::with_capacity(chunks.len() + 2);

        messages.push(ReplicationMessage::SnapshotBegin {
            last_included_offset: self.last_included_offset,
            total_bytes,
        });

        for (index, chunk) in chunks.iter().enumerate() {
            messages.push(ReplicationMessage::SnapshotChunk {
                last_included_offset: self.last_included_offset,
                chunk_index: index as u32,
                data: chunk.to_vec(),
            });
        }

        messages.push(ReplicationMessage::SnapshotEnd {
            last_included_offset: self.last_included_offset,
            total_chunks,
        });

        Ok(messages)
    }
}

pub struct SnapshotReceiver {
    last_included_offset: u64,
    expected_bytes: u64,
    received_bytes: u64,
    next_chunk_index: u32,
    bytes: Vec<u8>,
}

impl SnapshotReceiver {
    pub fn begin(last_included_offset: u64, total_bytes: u64) -> io::Result<Self> {
        if total_bytes == 0 || total_bytes > MAX_SNAPSHOT_BYTES {
            return Err(invalid_data("invalid snapshot byte count"));
        }

        let capacity = usize::try_from(total_bytes)
            .map_err(|_| invalid_data("snapshot is too large for this platform"))?;

        Ok(Self {
            last_included_offset,
            expected_bytes: total_bytes,
            received_bytes: 0,
            next_chunk_index: 0,
            bytes: Vec::with_capacity(capacity),
        })
    }

    pub fn push_chunk(
        &mut self,
        last_included_offset: u64,
        chunk_index: u32,
        data: &[u8],
    ) -> io::Result<()> {
        if last_included_offset != self.last_included_offset {
            return Err(invalid_data("snapshot chunk offset mismatch"));
        }

        if chunk_index != self.next_chunk_index {
            return Err(invalid_data("snapshot chunks are out of order"));
        }

        if data.is_empty() || data.len() > MAX_SNAPSHOT_CHUNK_SIZE {
            return Err(invalid_data("invalid snapshot chunk size"));
        }

        let new_size = self
            .received_bytes
            .checked_add(data.len() as u64)
            .ok_or_else(|| invalid_data("snapshot size overflow"))?;

        if new_size > self.expected_bytes {
            return Err(invalid_data("snapshot exceeds declared size"));
        }

        self.bytes.extend_from_slice(data);
        self.received_bytes = new_size;

        self.next_chunk_index = self
            .next_chunk_index
            .checked_add(1)
            .ok_or_else(|| invalid_data("too many snapshot chunks"))?;

        Ok(())
    }

    pub fn finish(self, last_included_offset: u64, total_chunks: u32) -> io::Result<Snapshot> {
        if last_included_offset != self.last_included_offset {
            return Err(invalid_data("snapshot end offset mismatch"));
        }

        if total_chunks != self.next_chunk_index {
            return Err(invalid_data("snapshot chunk count mismatch"));
        }

        if self.received_bytes != self.expected_bytes {
            return Err(invalid_data("incomplete snapshot transfer"));
        }

        let snapshot = Snapshot::decode(&self.bytes)?;

        if snapshot.last_included_offset != self.last_included_offset {
            return Err(invalid_data("snapshot payload offset mismatch"));
        }

        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trip() {
        let mut entries = BTreeMap::new();
        entries.insert("alpha".to_string(), "one".to_string());
        entries.insert("beta".to_string(), "two".to_string());

        let snapshot = Snapshot {
            last_included_offset: 42,
            entries,
        };

        let encoded = snapshot.encode().unwrap();
        let decoded = Snapshot::decode(&encoded).unwrap();

        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn rejects_invalid_header() {
        assert!(Snapshot::decode(b"not-a-snapshot").is_err());
    }

    #[test]
    fn produces_begin_and_end_messages() {
        let snapshot = Snapshot {
            last_included_offset: 10,
            entries: BTreeMap::new(),
        };

        let messages = snapshot.to_messages().unwrap();

        assert!(matches!(
            messages.first(),
            Some(ReplicationMessage::SnapshotBegin { .. })
        ));

        assert!(matches!(
            messages.last(),
            Some(ReplicationMessage::SnapshotEnd { .. })
        ));
    }
    #[test]
    fn receiver_accepts_valid_transfer() {
        let snapshot = Snapshot {
            last_included_offset: 42,
            entries: BTreeMap::from([("name".to_string(), "Bob".to_string())]),
        };

        let messages = snapshot.to_messages().unwrap();

        let Some(ReplicationMessage::SnapshotBegin {
            last_included_offset,
            total_bytes,
        }) = messages.first()
        else {
            panic!("expected SnapshotBegin");
        };

        let mut receiver = SnapshotReceiver::begin(*last_included_offset, *total_bytes).unwrap();

        for message in &messages[1..messages.len() - 1] {
            if let ReplicationMessage::SnapshotChunk {
                last_included_offset,
                chunk_index,
                data,
            } = message
            {
                receiver
                    .push_chunk(*last_included_offset, *chunk_index, data)
                    .unwrap();
            }
        }

        let Some(ReplicationMessage::SnapshotEnd {
            last_included_offset,
            total_chunks,
        }) = messages.last()
        else {
            panic!("expected SnapshotEnd");
        };

        let received = receiver
            .finish(*last_included_offset, *total_chunks)
            .unwrap();

        assert_eq!(received, snapshot);
    }

    #[test]
    fn receiver_rejects_out_of_order_chunks() {
        let mut receiver = SnapshotReceiver::begin(42, 10).unwrap();

        assert!(receiver.push_chunk(42, 1, b"hello").is_err());
    }

    #[test]
    fn receiver_rejects_incomplete_transfer() {
        let mut receiver = SnapshotReceiver::begin(42, 10).unwrap();

        receiver.push_chunk(42, 0, b"hello").unwrap();

        assert!(receiver.finish(42, 1).is_err());
    }
}
