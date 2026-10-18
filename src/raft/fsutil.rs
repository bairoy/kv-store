//! Small crash-safe file helpers for the Raft module.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Replace `path` with `bytes` so readers see the old or the new content,
/// never a partial write (temp file, fsync, rename, fsync directory).
pub fn write_file_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = temp_path_for(path);

    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)?;

        file.write_all(bytes)?;
        file.sync_all()?;
    }

    fs::rename(&temp, path)?;
    sync_parent_directory(path)
}

pub fn temp_path_for(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".tmp");
    PathBuf::from(name)
}

pub fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    sync_directory(parent)
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}
