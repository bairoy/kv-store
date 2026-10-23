//! A Raft snapshot: the full state machine as of `last_included_index`.
//!
//! Once a snapshot is durable, log entries up to that index can be discarded
//! (Raft paper, section 7). The same encoding is used on disk and as the
//! payload streamed by InstallSnapshot.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;

use crate::raft::codec::{
    invalid_data, put_config, put_string, put_u64, read_config, read_string, read_u64,
};
use crate::raft::fsutil::write_file_atomic;
use crate::raft::types::ClusterConfig;

const MAGIC: &[u8; 8] = b"RAFTSNP2";
const HEADER_SIZE: usize = 8 + 8 + 8 + 8;
const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftSnapshot {
    pub last_included_index: u64,
    pub last_included_term: u64,
    /// Who was in the group as of `last_included_index`. Needed because the
    /// config entries that established it are discarded with the log prefix.
    pub config: ClusterConfig,
    pub entries: BTreeMap<String, String>,
}

impl RaftSnapshot {
    pub fn encode(&self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();

        out.extend_from_slice(MAGIC);
        put_u64(&mut out, self.last_included_index);
        put_u64(&mut out, self.last_included_term);
        put_u64(&mut out, self.entries.len() as u64);
        put_config(&mut out, &self.config)?;

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
        let (last_included_index, last_included_term, count) = decode_header(bytes)?;
        let mut cursor = HEADER_SIZE;
        let config = read_config(bytes, &mut cursor)?;

        // Every entry takes at least 8 bytes, so a larger count is corrupt.
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
            last_included_index,
            last_included_term,
            config,
            entries,
        })
    }

    pub fn write_atomic(&self, path: impl AsRef<Path>) -> io::Result<()> {
        write_file_atomic(path.as_ref(), &self.encode()?)
    }

    pub fn read(path: impl AsRef<Path>) -> io::Result<Self> {
        let mut file = File::open(path)?;

        if file.metadata()?.len() > MAX_SNAPSHOT_BYTES {
            return Err(invalid_data("snapshot exceeds maximum size"));
        }

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        Self::decode(&bytes)
    }

    /// Read just `(last_included_index, last_included_term)` from a file.
    pub fn read_meta(path: impl AsRef<Path>) -> io::Result<(u64, u64)> {
        let mut header = [0u8; HEADER_SIZE];
        File::open(path)?.read_exact(&mut header)?;

        let (index, term, _) = decode_header(&header)?;

        Ok((index, term))
    }
}

fn decode_header(bytes: &[u8]) -> io::Result<(u64, u64, u64)> {
    if bytes.len() < HEADER_SIZE || &bytes[..8] != MAGIC {
        return Err(invalid_data("invalid Raft snapshot header"));
    }

    let mut cursor = 8;
    let index = read_u64(bytes, &mut cursor)?;
    let term = read_u64(bytes, &mut cursor)?;
    let count = read_u64(bytes, &mut cursor)?;

    if index == 0 || term == 0 {
        return Err(invalid_data("invalid Raft snapshot position"));
    }

    Ok((index, term, count))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> RaftSnapshot {
        RaftSnapshot {
            last_included_index: 42,
            last_included_term: 3,
            config: ClusterConfig {
                voters: vec![1, 2, 3],
                learners: vec![4],
            },
            entries: BTreeMap::from([
                ("name".to_string(), "baiju".to_string()),
                ("city".to_string(), "pune".to_string()),
            ]),
        }
    }

    #[test]
    fn snapshot_round_trips_through_bytes_and_files() {
        let snapshot = sample();
        assert_eq!(
            RaftSnapshot::decode(&snapshot.encode().unwrap()).unwrap(),
            snapshot
        );

        let dir = std::env::temp_dir().join(format!("kv_raft_snapshot_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("raft.snapshot");

        snapshot.write_atomic(&path).unwrap();

        assert_eq!(RaftSnapshot::read(&path).unwrap(), snapshot);
        assert_eq!(RaftSnapshot::read_meta(&path).unwrap(), (42, 3));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_snapshots_are_rejected() {
        let good = sample().encode().unwrap();

        assert!(RaftSnapshot::decode(&[]).is_err());
        assert!(RaftSnapshot::decode(&good[..good.len() - 1]).is_err());

        let mut trailing = good.clone();
        trailing.push(0);
        assert!(RaftSnapshot::decode(&trailing).is_err());

        let mut bad_magic = good.clone();
        bad_magic[0] = b'X';
        assert!(RaftSnapshot::decode(&bad_magic).is_err());

        let mut huge_count = good;
        huge_count[24..32].copy_from_slice(&u64::MAX.to_be_bytes());
        assert!(RaftSnapshot::decode(&huge_count).is_err());
    }
}
