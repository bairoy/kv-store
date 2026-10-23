use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::raft::codec;
use crate::raft::fsutil::{sync_parent_directory, temp_path_for, write_file_atomic};
use crate::raft::types::{ClusterConfig, Command, LogEntry};

const HEADER_SIZE: u64 = 4;
const MAX_RECORD_SIZE: usize = 1024 * 1024;

/// Current format: magic, base index, base term, then the cluster config
/// as of the base. (`RBAS` is the old format without a config.)
const BASE_MAGIC: &[u8; 4] = b"RBS2";
const LEGACY_BASE_MAGIC: &[u8; 4] = b"RBAS";
const LEGACY_BASE_FILE_SIZE: usize = 4 + 8 + 8;

/// Durable Raft log with support for discarding a compacted prefix.
///
/// Entries `1..=base_index` have been folded into a snapshot and are gone.
/// The remaining entries are `base_index + 1..=last_index()`. The snapshot
/// position `(base_index, base_term)` lives in a small side file so it
/// survives restarts even when no entries remain.
#[derive(Debug)]
pub struct RaftLog {
    path: PathBuf,
    file: File,
    base_path: PathBuf,
    base_index: u64,
    base_term: u64,
    /// The cluster config as of `base_index` (or the bootstrap config).
    base_config: ClusterConfig,
    entries: Vec<LogEntry>,
    record_positions: Vec<u64>,
}

impl RaftLog {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_config(path, ClusterConfig::default())
    }

    /// `bootstrap` is the cluster config to assume when the log holds no
    /// config entry and no compaction has happened yet.
    pub fn open_with_config(path: impl AsRef<Path>, bootstrap: ClusterConfig) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let base_path = path.with_extension("base");
        let (base_index, base_term, stored_config) = load_base(&base_path)?;
        let base_config = stored_config.unwrap_or(bootstrap);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        let file_len = file.metadata()?.len();
        let mut position = 0u64;
        let mut entries = Vec::new();
        let mut record_positions = Vec::new();

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
                return Err(invalid_data("invalid Raft record length"));
            }

            let record_end = position
                .checked_add(HEADER_SIZE)
                .and_then(|p| p.checked_add(length as u64))
                .ok_or_else(|| invalid_data("Raft record length overflow"))?;

            if record_end > file_len {
                break;
            }

            let mut payload = vec![0; length];
            file.read_exact(&mut payload)?;

            let (index, entry) = decode_entry(&payload)?;

            // A crash after the base was written but before the log file was
            // rewritten leaves records the snapshot already covers; skip them.
            if index <= base_index {
                position = record_end;
                continue;
            }

            let expected_index = base_index + entries.len() as u64 + 1;

            if index != expected_index {
                return Err(invalid_data("Raft log indices are not consecutive"));
            }

            record_positions.push(position);
            entries.push(entry);
            position = record_end;
        }

        // Discard an incomplete trailing record, if one exists.
        if position < file_len {
            file.set_len(position)?;
            file.sync_all()?;
        }

        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            path,
            file,
            base_path,
            base_index,
            base_term,
            base_config,
            entries,
            record_positions,
        })
    }

    /// Index of the last entry folded into the snapshot (0 if none).
    pub fn base_index(&self) -> u64 {
        self.base_index
    }

    /// Term of the entry at `base_index` (0 if none).
    pub fn base_term(&self) -> u64 {
        self.base_term
    }

    /// The cluster config as of `base_index`.
    pub fn base_config(&self) -> &ClusterConfig {
        &self.base_config
    }

    /// The cluster config in effect at the end of the log, with the index of
    /// the entry that set it (0 if it comes from the base / bootstrap).
    pub fn latest_config(&self) -> (u64, ClusterConfig) {
        self.config_as_of(self.last_index())
    }

    /// The config in effect once entries `..=index` have been applied: the
    /// latest `Config` entry at or before `index`, else the base config.
    pub fn config_as_of(&self, index: u64) -> (u64, ClusterConfig) {
        let last = index.min(self.last_index());

        let mut position = last.saturating_sub(self.base_index) as usize;

        while position > 0 {
            if let Command::Config(config) = &self.entries[position - 1].command {
                return (self.base_index + position as u64, config.clone());
            }

            position -= 1;
        }

        (0, self.base_config.clone())
    }

    /// Index of the first entry still stored in the log.
    pub fn first_index(&self) -> u64 {
        self.base_index + 1
    }

    pub fn last_index(&self) -> u64 {
        self.base_index + self.entries.len() as u64
    }

    pub fn last_term(&self) -> u64 {
        self.entries
            .last()
            .map_or(self.base_term, |entry| entry.term)
    }

    /// Term at `index`. The snapshot boundary itself is answered from the
    /// base; anything older than that is no longer known.
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == self.base_index {
            return Some(self.base_term);
        }

        self.entry_at(index).map(|entry| entry.term)
    }

    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index <= self.base_index {
            return None;
        }

        let position = usize::try_from(index - self.base_index - 1).ok()?;
        self.entries.get(position)
    }

    /// Entries from `start_index` on. Indices already compacted are not
    /// returned; callers that need them must send a snapshot instead.
    pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
        let start_index = start_index.max(self.first_index());

        let Ok(start) = usize::try_from(start_index - self.first_index()) else {
            return Vec::new();
        };

        self.entries.get(start..).unwrap_or(&[]).to_vec()
    }

    pub fn append(&mut self, entry: LogEntry) -> io::Result<u64> {
        let index = self
            .last_index()
            .checked_add(1)
            .ok_or_else(|| invalid_data("Raft log index overflow"))?;

        let payload = encode_entry(index, &entry)?;

        if payload.len() > MAX_RECORD_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Raft entry exceeds maximum record size",
            ));
        }

        let length = u32::try_from(payload.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Raft entry is too large"))?;

        let position = self.file.seek(SeekFrom::End(0))?;

        let result = (|| {
            self.file.write_all(&length.to_be_bytes())?;
            self.file.write_all(&payload)?;
            self.file.sync_all()?;
            Ok::<(), io::Error>(())
        })();

        if let Err(error) = result {
            // Restore the previous record boundary after a failed append.
            self.file.set_len(position)?;
            self.file.sync_all()?;
            self.file.seek(SeekFrom::End(0))?;
            return Err(error);
        }

        self.record_positions.push(position);
        self.entries.push(entry);

        Ok(index)
    }

    pub fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        if index <= self.base_index {
            return Err(invalid_data("cannot truncate a compacted Raft entry"));
        }

        let Ok(position) = usize::try_from(index - self.base_index - 1) else {
            return Err(invalid_data("Raft log index is out of range"));
        };

        if position >= self.entries.len() {
            return Ok(());
        }

        let file_position = self.record_positions[position];

        self.file.set_len(file_position)?;
        self.file.sync_all()?;
        self.file.seek(SeekFrom::End(0))?;

        self.entries.truncate(position);
        self.record_positions.truncate(position);

        Ok(())
    }

    /// Fold everything up to `index` (whose term is `term`) into a snapshot.
    ///
    /// `config` is the cluster config as of `index` (it must travel with the
    /// snapshot, since the config entries being dropped are the only record
    /// of it). The caller must already have made the snapshot durable. If the
    /// log has a matching entry at `index` the later entries are kept;
    /// otherwise (the snapshot is ahead of, or disagrees with, this log) the
    /// whole log is discarded.
    pub fn compact_to(&mut self, index: u64, term: u64, config: ClusterConfig) -> io::Result<()> {
        if index <= self.base_index {
            return Ok(());
        }

        if term == 0 {
            return Err(invalid_data("snapshot term must not be zero"));
        }

        let keep_suffix = index < self.last_index() && self.term_at(index) == Some(term);

        let dropped = if keep_suffix {
            usize::try_from(index - self.base_index)
                .map_err(|_| invalid_data("Raft log index is out of range"))?
        } else {
            self.entries.len()
        };

        let kept: Vec<LogEntry> = self.entries[dropped..].to_vec();

        // 1. Record the new base durably. From here on, older records in the
        //    file are ignored on open, so a crash below is harmless.
        write_file_atomic(&self.base_path, &encode_base(index, term, &config)?)?;

        // 2. Rewrite the log file with only the kept entries.
        let temp_path = temp_path_for(&self.path);
        let mut positions = Vec::with_capacity(kept.len());

        {
            let mut temp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)?;

            let mut written = 0u64;

            for (offset, entry) in kept.iter().enumerate() {
                let record_index = index + 1 + offset as u64;
                let payload = encode_entry(record_index, entry)?;
                let length = u32::try_from(payload.len())
                    .map_err(|_| invalid_data("Raft entry is too large"))?;

                temp.write_all(&length.to_be_bytes())?;
                temp.write_all(&payload)?;

                positions.push(written);
                written += HEADER_SIZE + payload.len() as u64;
            }

            temp.sync_all()?;
        }

        fs::rename(&temp_path, &self.path)?;
        sync_parent_directory(&self.path)?;

        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        file.seek(SeekFrom::End(0))?;

        self.file = file;
        self.base_index = index;
        self.base_term = term;
        self.base_config = config;
        self.entries = kept;
        self.record_positions = positions;

        Ok(())
    }

    pub fn append_from_leader(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        incoming: &[LogEntry],
        commit_index: u64,
    ) -> io::Result<bool> {
        let mut prev_log_index = prev_log_index;
        let mut prev_log_term = prev_log_term;
        let mut incoming = incoming;

        // Entries at or below the snapshot are committed and already folded
        // in; skip them and continue from the snapshot boundary.
        if prev_log_index < self.base_index {
            let skip = usize::try_from(self.base_index - prev_log_index)
                .unwrap_or(usize::MAX)
                .min(incoming.len());

            incoming = &incoming[skip..];
            prev_log_index = prev_log_index.saturating_add(skip as u64);

            if prev_log_index < self.base_index {
                // The whole batch lies inside the snapshot.
                return Ok(true);
            }

            prev_log_term = self.base_term;
        }

        // The preceding entry must match before this batch is accepted.
        if self.term_at(prev_log_index) != Some(prev_log_term) {
            return Ok(false);
        }

        for (offset, incoming_entry) in incoming.iter().enumerate() {
            let offset =
                u64::try_from(offset).map_err(|_| invalid_data("too many incoming entries"))?;

            let index = prev_log_index
                .checked_add(offset)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| invalid_data("Raft log index overflow"))?;

            match self.entry_at(index) {
                Some(existing) if existing.term == incoming_entry.term => {
                    // Same index and term must identify the same entry.
                    if existing != incoming_entry {
                        return Err(invalid_data(
                            "conflicting commands have the same Raft index and term",
                        ));
                    }
                }

                Some(_) => {
                    if index <= commit_index {
                        return Err(invalid_data(
                            "attempted to overwrite a committed Raft entry",
                        ));
                    }

                    self.truncate_from(index)?;

                    for entry in incoming.iter().skip(offset as usize) {
                        self.append(entry.clone())?;
                    }

                    return Ok(true);
                }

                None => {
                    for entry in incoming.iter().skip(offset as usize) {
                        self.append(entry.clone())?;
                    }

                    return Ok(true);
                }
            }
        }

        Ok(true)
    }
}

fn encode_base(index: u64, term: u64, config: &ClusterConfig) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();

    out.extend_from_slice(BASE_MAGIC);
    codec::put_u64(&mut out, index);
    codec::put_u64(&mut out, term);
    codec::put_config(&mut out, config)?;

    Ok(out)
}

/// `(index, term, config)`. A missing file means "never compacted"; the
/// config is `None` for the legacy format (the caller's bootstrap applies).
fn load_base(path: &Path) -> io::Result<(u64, u64, Option<ClusterConfig>)> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((0, 0, None)),
        Err(error) => return Err(error),
    };

    if bytes.len() < 4 {
        return Err(invalid_data("invalid Raft log base file"));
    }

    let mut cursor = 4;
    let config_follows = match &bytes[..4] {
        magic if magic == BASE_MAGIC => true,
        magic if magic == LEGACY_BASE_MAGIC && bytes.len() == LEGACY_BASE_FILE_SIZE => false,
        _ => return Err(invalid_data("invalid Raft log base file")),
    };

    let index = codec::read_u64(&bytes, &mut cursor)?;
    let term = codec::read_u64(&bytes, &mut cursor)?;

    if index == 0 || term == 0 {
        return Err(invalid_data("invalid Raft log base position"));
    }

    let config = if config_follows {
        let config = codec::read_config(&bytes, &mut cursor)?;

        if cursor != bytes.len() {
            return Err(invalid_data("trailing bytes in Raft log base file"));
        }

        Some(config)
    } else {
        None
    };

    Ok((index, term, config))
}

fn encode_entry(index: u64, entry: &LogEntry) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();

    codec::put_u64(&mut out, index);
    codec::put_entry(&mut out, entry)?;

    Ok(out)
}

fn decode_entry(bytes: &[u8]) -> io::Result<(u64, LogEntry)> {
    let mut cursor = 0;

    let index = codec::read_u64(bytes, &mut cursor)?;
    let entry = codec::read_entry(bytes, &mut cursor)?;

    if cursor != bytes.len() || index == 0 || entry.term == 0 {
        return Err(invalid_data("invalid Raft log record"));
    }

    Ok((index, entry))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
