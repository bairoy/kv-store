use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::raft::types::NodeId;

const MAGIC: &[u8; 4] = b"RAFT";
const STATE_SIZE: usize = 21;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
}

impl PersistentState {
    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();

        let mut file = match File::open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(error) => return Err(error),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;

        if bytes.len() != STATE_SIZE {
            return Err(invalid_data("invalid Raft state file length"));
        }

        if &bytes[0..4] != MAGIC {
            return Err(invalid_data("invalid Raft state file magic"));
        }

        let current_term = u64::from_be_bytes(bytes[4..12].try_into().unwrap());

        let vote_marker = bytes[12];

        let voted_for = match vote_marker {
            0 => {
                if bytes[13..21].iter().any(|byte| *byte != 0) {
                    return Err(invalid_data("invalid vote data when no vote is recorded"));
                }
                None
            }
            1 => Some(u64::from_be_bytes(bytes[13..21].try_into().unwrap())),
            _ => return Err(invalid_data("invalid vote marker")),
        };

        Ok(Self {
            current_term,
            voted_for,
        })
    }

    pub fn save(&self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();

        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));

        fs::create_dir_all(parent)?;

        let temp_path = temp_path_for(path);

        let mut bytes = Vec::with_capacity(STATE_SIZE);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.current_term.to_be_bytes());

        match self.voted_for {
            Some(node_id) => {
                bytes.push(1);
                bytes.extend_from_slice(&node_id.to_be_bytes());
            }
            None => {
                bytes.push(0);
                bytes.extend_from_slice(&0_u64.to_be_bytes());
            }
        }

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
        sync_directory(parent)?;

        Ok(())
    }
}

fn temp_path_for(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_path(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join("kv_store_raft_state_tests")
            .join(format!("{}_{}.state", name, std::process::id()))
    }

    #[test]
    fn missing_state_uses_defaults() {
        let path = state_path("missing");
        let _ = fs::remove_file(&path);

        assert_eq!(
            PersistentState::load(&path).unwrap(),
            PersistentState::default()
        );
    }

    #[test]
    fn state_survives_restart() {
        let path = state_path("restart");
        let state = PersistentState {
            current_term: 7,
            voted_for: Some(2),
        };

        state.save(&path).unwrap();

        assert_eq!(PersistentState::load(&path).unwrap(), state);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn state_without_vote_survives_restart() {
        let path = state_path("no_vote");
        let state = PersistentState {
            current_term: 4,
            voted_for: None,
        };

        state.save(&path).unwrap();

        assert_eq!(PersistentState::load(&path).unwrap(), state);

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn corrupted_state_is_rejected() {
        let path = state_path("corrupt");

        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }

        fs::write(&path, b"bad").unwrap();

        assert!(PersistentState::load(&path).is_err());

        let _ = fs::remove_file(&path);
    }
}
