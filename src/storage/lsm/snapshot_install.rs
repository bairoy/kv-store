//! Installing a leader snapshot (replication catch-up), with crash recovery.

use super::wal::truncate_wal_file;
use super::*;

pub(super) const SNAPSHOT_INSTALL_MARKER: &str = "snapshot.install";
pub(super) const SNAPSHOT_OFFSET_FILE: &str = "snapshot.offset";

impl LsmStore {
    pub fn install_snapshot(
        &mut self,
        entries: &BTreeMap<String, String>,
        last_included_offset: u64,
    ) -> io::Result<()> {
        // Choose a filename that does not already exist.
        let final_path = loop {
            let path = self.next_sstable_path();

            if !path.exists() {
                break path;
            }

            self.next_sstable_id += 1;
        };

        let final_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid snapshot SSTable filename",
                )
            })?
            .to_string();

        // The staging filename does not end in ".db", so the ordinary
        // SSTable loader cannot mistake it for an active table.
        let staged_path = self
            .sstable_dir
            .join(format!("snapshot-staged-{}.staged", std::process::id()));

        let mut table_entries = BTreeMap::<String, Option<String>>::new();

        for (key, value) in entries {
            table_entries.insert(key.clone(), Some(value.clone()));
        }

        // Build and fsync the replacement before recording the transition.
        Sstable::create_from_entries(&staged_path, table_entries.iter())?;
        sync_directory(&self.sstable_dir)?;

        // Once this marker is durable, startup recovery must finish
        // this installation rather than resume with the old database.
        Self::write_snapshot_install_marker(
            &self.sstable_dir,
            &staged_path,
            &final_path,
            last_included_offset,
        )?;

        // Use the same recovery procedure during live installation and
        // startup. This keeps both paths consistent.
        Self::recover_snapshot_install(&self.sstable_dir, &self.wal_path)?;

        // Replace the in-memory view with the installed generation.
        self.sstables = Self::load_sstables(&self.sstable_dir)?;
        self.memtable = Memtable::new();

        self.next_sstable_id = next_sstable_id_in(&self.sstable_dir)?;

        // Reopen the WAL so future writes continue appending normally.
        let wal_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.wal_path)?;

        self.wal = BufWriter::new(wal_file);

        Ok(())
    }

    pub fn installed_snapshot_offset(&self) -> io::Result<u64> {
        let path = self.sstable_dir.join(SNAPSHOT_OFFSET_FILE);

        match fs::read_to_string(path) {
            Ok(contents) => contents.trim().parse::<u64>().map_err(|error| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid snapshot offset: {error}"),
                )
            }),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error),
        }
    }

    pub(super) fn write_snapshot_install_marker(
        dir: &Path,
        staged_path: &Path,
        final_path: &Path,
        offset: u64,
    ) -> io::Result<()> {
        let staged_name = staged_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid staged snapshot filename",
                )
            })?;

        let final_name = final_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid final snapshot filename",
                )
            })?;

        let marker_path = dir.join(SNAPSHOT_INSTALL_MARKER);
        let temp_path = dir.join("snapshot.install.tmp");

        {
            let file = File::create(&temp_path)?;
            let mut writer = BufWriter::new(file);

            writeln!(writer, "VERSION\t1")?;
            writeln!(writer, "STAGED\t{staged_name}")?;
            writeln!(writer, "FINAL\t{final_name}")?;
            writeln!(writer, "OFFSET\t{offset}")?;

            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        fs::rename(&temp_path, &marker_path)?;
        sync_directory(dir)?;

        Ok(())
    }

    pub(super) fn recover_snapshot_install(dir: &Path, wal_path: &Path) -> io::Result<()> {
        let marker_path = dir.join(SNAPSHOT_INSTALL_MARKER);

        if !marker_path.exists() {
            return Ok(());
        }

        let contents = fs::read_to_string(&marker_path)?;

        let mut version = None;
        let mut staged_name = None;
        let mut final_name = None;
        let mut offset = None;

        for line in contents.lines() {
            let mut parts = line.splitn(2, '\t');

            let kind = parts.next().unwrap_or_default();
            let value = parts.next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed snapshot installation marker",
                )
            })?;

            match kind {
                "VERSION" => version = Some(value.to_string()),
                "STAGED" => staged_name = Some(value.to_string()),
                "FINAL" => final_name = Some(value.to_string()),
                "OFFSET" => {
                    offset = Some(value.parse::<u64>().map_err(|error| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid snapshot offset in marker: {error}"),
                        )
                    })?);
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unknown field in snapshot installation marker",
                    ));
                }
            }
        }

        if version.as_deref() != Some("1") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unsupported snapshot installation marker version",
            ));
        }

        let staged_name = staged_name.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot marker is missing STAGED",
            )
        })?;

        let final_name = final_name.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot marker is missing FINAL",
            )
        })?;

        let offset = offset.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot marker is missing OFFSET",
            )
        })?;

        // Do not allow marker contents to escape the SSTable directory.
        validate_basename(&staged_name)?;
        validate_basename(&final_name)?;

        let staged_path = dir.join(&staged_name);
        let final_path = dir.join(&final_name);

        // Complete activation. The staged file was made durable before
        // the marker was written, so at least one valid candidate must exist.
        if !final_path.exists() {
            if !staged_path.exists() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "snapshot marker exists, but staged and final tables are missing",
                ));
            }

            // Validate the staged table before activating it.
            drop(Sstable::open(staged_path.clone())?);

            fs::rename(&staged_path, &final_path)?;
            sync_directory(dir)?;
        } else {
            // If activation already completed, a leftover staging file is
            // no longer needed.
            if staged_path.exists() {
                fs::remove_file(&staged_path)?;
            }
        }

        // Validate the authoritative replacement before destroying old data.
        drop(Sstable::open(final_path.clone())?);

        // Old WAL records must not be replayed over the snapshot.
        truncate_wal_file(wal_path)?;

        // Remove all old SSTables except the selected replacement.
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            let is_sstable = path.extension().and_then(|ext| ext.to_str()) == Some("db");

            if is_sstable && path != final_path {
                fs::remove_file(path)?;
            }
        }

        sync_directory(dir)?;

        // Persist the snapshot base offset before removing the marker.
        persist_snapshot_offset(dir, offset)?;

        // The transition is complete. Removing the marker is the final step.
        if marker_path.exists() {
            fs::remove_file(&marker_path)?;
        }

        sync_directory(dir)?;

        Ok(())
    }
}

pub(super) fn validate_basename(name: &str) -> io::Result<()> {
    let path = Path::new(name);

    if name.is_empty()
        || path.file_name().and_then(|part| part.to_str()) != Some(name)
        || name == "."
        || name == ".."
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid filename in snapshot installation marker",
        ));
    }

    Ok(())
}

pub(super) fn persist_snapshot_offset(dir: &Path, offset: u64) -> io::Result<()> {
    let offset_path = dir.join(SNAPSHOT_OFFSET_FILE);
    let temp_path = dir.join("snapshot.offset.tmp");

    {
        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        writeln!(writer, "{offset}")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }

    fs::rename(&temp_path, &offset_path)?;
    sync_directory(dir)?;

    Ok(())
}
