//! Write-ahead log: append, replay, truncate.

use super::*;

impl LsmStore {
    pub(super) fn append_wal(&mut self, record: &str) -> io::Result<()> {
        self.wal.write_all(record.as_bytes())?;
        self.wal.flush()?;

        let started = std::time::Instant::now();
        self.wal.get_ref().sync_all()?;
        crate::metrics::observe_duration("wal_fsync_seconds", &[], started.elapsed());

        Ok(())
    }

    pub(super) fn replay_wal(&mut self) -> io::Result<()> {
        let file = File::open(&self.wal_path)?;

        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;

            let parts: Vec<&str> = line.splitn(3, '\t').collect();

            match parts.as_slice() {
                ["SET", key, value] => {
                    self.memtable.set((*key).to_string(), (*value).to_string());
                }

                ["DEL", key] => {
                    self.memtable.delete((*key).to_string());
                }

                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid WAL record",
                    ));
                }
            }
        }

        Ok(())
    }

    pub(super) fn truncate_wal(&mut self) -> io::Result<()> {
        self.wal.flush()?;

        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.wal_path)?;

        file.sync_all()?;

        drop(file);

        let new_wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.wal_path)?;

        self.wal = BufWriter::new(new_wal);

        Ok(())
    }
}

pub(super) fn truncate_wal_file(path: &Path) -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;

    file.sync_all()?;
    drop(file);

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    sync_directory(parent)?;

    Ok(())
}
