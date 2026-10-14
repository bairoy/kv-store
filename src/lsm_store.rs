use crate::memtable::Memtable;
use crate::sstable::{Sstable, SstableEntry};

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

const COMPACTION_MARKER: &str = "compaction.marker";
const SNAPSHOT_INSTALL_MARKER: &str = "snapshot.install";
const SNAPSHOT_OFFSET_FILE: &str = "snapshot.offset";
pub struct LsmStore {
    memtable: Memtable,
    sstables: Vec<Sstable>,

    wal: BufWriter<File>,
    wal_path: PathBuf,

    sstable_dir: PathBuf,
    next_sstable_id: usize,
}

impl LsmStore {
    pub fn open(sstable_dir: impl AsRef<Path>, wal_path: impl AsRef<Path>) -> io::Result<Self> {
        let sstable_dir = sstable_dir.as_ref().to_path_buf();
        let wal_path = wal_path.as_ref().to_path_buf();
        println!("LsmStore::open: creating SSTable directory");
        fs::create_dir_all(&sstable_dir)?;

        // Recover storage transitions before reading active data.
        println!("LsmStore::open: recovering compaction");
        Self::recover_compaction(&sstable_dir)?;
        println!("LsmStore::open: recovering snapshot installation");

        Self::recover_snapshot_install(&sstable_dir, &wal_path)?;

        println!("LsmStore::open: loading SSTables");
        let sstables = Self::load_sstables(&sstable_dir)?;
        println!("LsmStore::open: calculating next SSTable ID");
        let next_sstable_id = next_sstable_id_in(&sstable_dir)?;
        println!("LsmStore::open: opening WAL");

        let wal_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&wal_path)?;

        let mut store = Self {
            memtable: Memtable::new(),
            sstables,
            wal: BufWriter::new(wal_file),
            wal_path,
            sstable_dir,
            next_sstable_id,
        };

        store.replay_wal()?;

        Ok(store)
    }
    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        self.append_wal(&format!("SET\t{key}\t{value}\n"))?;

        self.memtable.set(key, value);

        Ok(())
    }

    pub fn delete(&mut self, key: String) -> io::Result<Option<String>> {
        let old_value = self.get(&key);

        self.append_wal(&format!("DEL\t{key}\n"))?;

        self.memtable.delete(key);

        Ok(old_value)
    }

    /// Read path:
    ///
    /// 1. Memtable
    /// 2. Newest SSTable
    /// 3. Older SSTable
    ///
    /// SSTable lookup itself uses:
    ///
    /// Bloom Filter → Sparse Index → bounded scan
    pub fn get(&self, key: &str) -> Option<String> {
        // Memtable has priority.
        if let Some(value) = self.memtable.get(key) {
            return value.clone();
        }

        // sstables are stored oldest → newest.
        // Search newest → oldest.
        for table in self.sstables.iter().rev() {
            match table.get(key) {
                Ok(Some(SstableEntry::Value(value))) => {
                    return Some(value);
                }

                Ok(Some(SstableEntry::Tombstone)) => {
                    // A tombstone is a real answer.
                    //
                    // We must NOT continue to older SSTables,
                    // otherwise an old value could incorrectly
                    // reappear.
                    return None;
                }

                Ok(None) => {
                    // Key is not in this SSTable.
                    continue;
                }

                Err(error) => {
                    eprintln!("SSTable read error for {:?}: {}", table.path(), error);

                    return None;
                }
            }
        }

        None
    }
    pub fn snapshot_entries(&self) -> io::Result<BTreeMap<String, String>> {
        let mut merged: BTreeMap<String, Option<String>> = BTreeMap::new();

        // Older tables are inserted first; newer tables overwrite them.
        for table in &self.sstables {
            for (key, value) in table.read_all()? {
                merged.insert(key, value);
            }
        }

        // The memtable contains the newest state, including tombstones.
        for (key, value) in self.memtable.iter() {
            merged.insert(key.clone(), value.clone());
        }

        // Snapshots contain live values only.
        Ok(merged
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect())
    }

    /// Flush the current memtable to a new immutable SSTable.
    pub fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        // The SSTable is created and fsynced before we truncate
        // the WAL.
        let path = self.next_sstable_path();

        let table = Sstable::create(&path, &self.memtable)?;

        // The SSTable is now durable.
        //
        // Only now can the WAL entries represented by this
        // memtable be discarded.
        self.truncate_wal()?;

        self.sstables.push(table);

        self.memtable = Memtable::new();

        self.next_sstable_id += 1;

        Ok(())
    }

    /// Merge all current SSTables into one new SSTable.
    ///
    /// This first implementation performs compaction explicitly.
    ///
    /// Newest SSTable wins.
    ///
    /// Tombstones are removed because we compact ALL SSTables,
    /// so there is no older SSTable left that could contain the
    /// value shadowed by the tombstone.
    pub fn compact(&mut self) -> io::Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }

        let old_tables = self.sstables.clone();

        let new_path = self.next_sstable_path();

        // -------------------------------------------------------
        // Phase 1:
        // Write a durable compaction marker BEFORE creating the
        // replacement SSTable.
        //
        // If we crash after this point, recovery knows whether
        // the new table was successfully installed.
        // -------------------------------------------------------

        let old_names: Vec<String> = old_tables
            .iter()
            .filter_map(|table| {
                table
                    .path()
                    .file_name()
                    .map(|name| name.to_string_lossy().to_string())
            })
            .collect();

        let new_name = new_path
            .file_name()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "new SSTable path has no filename",
                )
            })?
            .to_string_lossy()
            .to_string();

        self.write_compaction_marker(&new_name, &old_names)?;

        // -------------------------------------------------------
        // Phase 2:
        // Merge the SSTables.
        // -------------------------------------------------------

        let mut seen = HashSet::new();
        let mut merged = BTreeMap::new();

        // sstables = oldest → newest
        //
        // Therefore iterate backwards.
        for table in old_tables.iter().rev() {
            let entries = table.read_all()?;

            for (key, value) in entries {
                // If we've already encountered this key in a
                // newer SSTable, the newer version wins.
                if !seen.insert(key.clone()) {
                    continue;
                }

                match value {
                    Some(value) => {
                        merged.insert(key, Some(value));
                    }

                    None => {
                        // Tombstone:
                        //
                        // We intentionally do NOT write it into
                        // the compacted table because ALL older
                        // SSTables are part of this compaction.
                    }
                }
            }
        }

        // -------------------------------------------------------
        // Phase 3:
        // Create and fsync the new SSTable.
        // -------------------------------------------------------

        let new_table = Sstable::create_from_entries(&new_path, merged.iter())?;

        // Ensure the directory metadata is durable on Unix.
        sync_directory(&self.sstable_dir)?;

        // -------------------------------------------------------
        // Phase 4:
        // Delete old SSTables.
        //
        // This happens ONLY after the new SSTable is durable.
        // -------------------------------------------------------

        for table in &old_tables {
            fs::remove_file(table.path())?;
        }

        sync_directory(&self.sstable_dir)?;

        // -------------------------------------------------------
        // Phase 5:
        // Remove the marker.
        //
        // Compaction is now complete.
        // -------------------------------------------------------

        let marker_path = self.marker_path();

        if marker_path.exists() {
            fs::remove_file(&marker_path)?;
        }

        sync_directory(&self.sstable_dir)?;

        self.sstables = vec![new_table];

        self.next_sstable_id += 1;

        Ok(())
    }
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
    fn append_wal(&mut self, record: &str) -> io::Result<()> {
        self.wal.write_all(record.as_bytes())?;
        self.wal.flush()?;
        self.wal.get_ref().sync_all()?;

        Ok(())
    }

    fn replay_wal(&mut self) -> io::Result<()> {
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

    fn truncate_wal(&mut self) -> io::Result<()> {
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

    fn next_sstable_path(&self) -> PathBuf {
        self.sstable_dir
            .join(format!("sstable-{:06}.db", self.next_sstable_id))
    }

    fn load_sstables(dir: &Path) -> io::Result<Vec<Sstable>> {
        let mut paths = Vec::new();

        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            let is_sstable = path.extension().and_then(|ext| ext.to_str()) == Some("db");

            if is_sstable {
                paths.push(path);
            }
        }

        // Oldest → newest.
        paths.sort();

        let mut tables = Vec::new();

        for path in paths {
            tables.push(Sstable::open(path)?);
        }

        Ok(tables)
    }

    fn marker_path(&self) -> PathBuf {
        self.sstable_dir.join(COMPACTION_MARKER)
    }

    fn write_compaction_marker(&self, new_name: &str, old_names: &[String]) -> io::Result<()> {
        let marker_path = self.marker_path();
        let temp_path = self.sstable_dir.join("compaction.marker.tmp");

        {
            let mut file = BufWriter::new(File::create(&temp_path)?);

            writeln!(file, "NEW\t{new_name}")?;

            for old_name in old_names {
                writeln!(file, "OLD\t{old_name}")?;
            }

            file.flush()?;
            file.get_ref().sync_all()?;
        }

        fs::rename(&temp_path, &marker_path)?;

        sync_directory(&self.sstable_dir)?;

        Ok(())
    }

    /// Recover a compaction interrupted by a crash.
    ///
    /// Cases:
    ///
    /// 1. Marker exists, new SSTable doesn't exist:
    ///    compaction never completed → keep old tables.
    ///
    /// 2. Marker exists, new SSTable exists:
    ///    new table is durable → finish deleting old tables.
    fn recover_compaction(dir: &Path) -> io::Result<()> {
        let marker_path = dir.join(COMPACTION_MARKER);

        if !marker_path.exists() {
            return Ok(());
        }

        let file = File::open(&marker_path)?;
        let reader = BufReader::new(file);

        let mut new_name = None;
        let mut old_names = Vec::new();

        for line in reader.lines() {
            let line = line?;

            let parts: Vec<&str> = line.splitn(2, '\t').collect();

            match parts.as_slice() {
                ["NEW", name] => {
                    new_name = Some((*name).to_string());
                }

                ["OLD", name] => {
                    old_names.push((*name).to_string());
                }

                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid compaction marker",
                    ));
                }
            }
        }

        let new_name = new_name.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compaction marker has no NEW entry",
            )
        })?;

        let new_path = dir.join(&new_name);

        if new_path.exists() {
            // New SSTable is durable.
            //
            // Finish deleting old SSTables.
            for old_name in old_names {
                let old_path = dir.join(old_name);

                if old_path.exists() {
                    fs::remove_file(old_path)?;
                }
            }
        }

        // If new table doesn't exist, the compaction never
        // reached the durable-output stage. Leave old tables.
        //
        // In either case the marker is no longer needed.
        fs::remove_file(marker_path)?;

        sync_directory(dir)?;

        Ok(())
    }
    fn write_snapshot_install_marker(
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
    fn recover_snapshot_install(dir: &Path, wal_path: &Path) -> io::Result<()> {
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
fn validate_basename(name: &str) -> io::Result<()> {
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

fn truncate_wal_file(path: &Path) -> io::Result<()> {
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

fn persist_snapshot_offset(dir: &Path, offset: u64) -> io::Result<()> {
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
fn next_sstable_id_in(dir: &Path) -> io::Result<usize> {
    let mut max_id = None;

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();

        if path.extension().and_then(|ext| ext.to_str()) != Some("db") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };

        let Some(number) = stem.strip_prefix("sstable-") else {
            continue;
        };

        if let Ok(id) = number.parse::<usize>() {
            max_id = Some(max_id.map_or(id, |current: usize| current.max(id)));
        }
    }

    Ok(max_id.map_or(0, |id| id.saturating_add(1)))
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

    fn temp_dir(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kv_store_v7_lsm_{}_{}", name, std::process::id()));

        let _ = fs::remove_dir_all(&dir);

        fs::create_dir_all(&dir).unwrap();

        dir
    }

    fn wal_path(dir: &Path) -> PathBuf {
        dir.join("wal.log")
    }

    #[test]
    fn set_and_get() {
        let dir = temp_dir("set_get");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("name".to_string(), "Baiju".to_string()).unwrap();

        assert_eq!(store.get("name"), Some("Baiju".to_string()));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn flush_and_read_from_sstable() {
        let dir = temp_dir("flush");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("name".to_string(), "Baiju".to_string()).unwrap();

        store.flush().unwrap();

        assert_eq!(store.get("name"), Some("Baiju".to_string()));

        assert_eq!(store.sstables.len(), 1);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn newest_sstable_shadows_old_value() {
        let dir = temp_dir("shadow");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("name".to_string(), "Alice".to_string()).unwrap();

        store.flush().unwrap();

        store.set("name".to_string(), "Bob".to_string()).unwrap();

        store.flush().unwrap();

        assert_eq!(store.get("name"), Some("Bob".to_string()));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn tombstone_hides_old_value() {
        let dir = temp_dir("tombstone");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("name".to_string(), "Alice".to_string()).unwrap();

        store.flush().unwrap();

        store.delete("name".to_string()).unwrap();

        store.flush().unwrap();

        assert_eq!(store.get("name"), None);

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_keeps_newest_value() {
        let dir = temp_dir("compaction_value");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("name".to_string(), "Alice".to_string()).unwrap();

        store.flush().unwrap();

        store.set("name".to_string(), "Bob".to_string()).unwrap();

        store.flush().unwrap();

        assert_eq!(store.sstables.len(), 2);

        store.compact().unwrap();

        assert_eq!(store.sstables.len(), 1);

        assert_eq!(store.get("name"), Some("Bob".to_string()));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_removes_tombstoned_value() {
        let dir = temp_dir("compaction_delete");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("name".to_string(), "Alice".to_string()).unwrap();

        store.flush().unwrap();

        store.delete("name".to_string()).unwrap();
        store.flush().unwrap();

        assert_eq!(store.get("name"), None);

        store.compact().unwrap();

        assert_eq!(store.sstables.len(), 1);

        assert_eq!(store.get("name"), None);

        let entries = store.sstables[0].read_all().unwrap();

        assert!(!entries.iter().any(|(key, _)| key == "name"));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn compaction_preserves_different_keys() {
        let dir = temp_dir("compaction_multiple");

        let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

        store.set("a".to_string(), "1".to_string()).unwrap();

        store.set("b".to_string(), "2".to_string()).unwrap();

        store.flush().unwrap();

        store.set("a".to_string(), "10".to_string()).unwrap();

        store.set("c".to_string(), "3".to_string()).unwrap();

        store.flush().unwrap();

        store.compact().unwrap();

        assert_eq!(store.get("a"), Some("10".to_string()));
        assert_eq!(store.get("b"), Some("2".to_string()));
        assert_eq!(store.get("c"), Some("3".to_string()));

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn restart_rebuilds_index_and_bloom_filter() {
        let dir = temp_dir("restart_metadata");

        {
            let mut store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

            for i in 0..300 {
                store
                    .set(format!("key-{i:03}"), format!("value-{i}"))
                    .unwrap();
            }

            store.flush().unwrap();
        }

        {
            let store = LsmStore::open(&dir, wal_path(&dir)).unwrap();

            assert_eq!(store.get("key-150"), Some("value-150".to_string()));

            assert_eq!(store.get("does-not-exist"), None);
        }

        let _ = fs::remove_dir_all(dir);
    }
    #[test]
    fn snapshot_install_replaces_old_logical_state() {
        let dir = temp_dir("snapshot_replace");
        let sstable_dir = dir.join("sstables");
        let wal_path = dir.join("wal.log");

        let mut store = LsmStore::open(&sstable_dir, &wal_path).unwrap();

        store.set("name".to_string(), "Alice".to_string()).unwrap();
        store.set("age".to_string(), "20".to_string()).unwrap();
        store.flush().unwrap();

        let snapshot = BTreeMap::from([
            ("name".to_string(), "Bob".to_string()),
            ("city".to_string(), "Kathmandu".to_string()),
        ]);

        store.install_snapshot(&snapshot, 50).unwrap();

        assert_eq!(store.get("name"), Some("Bob".to_string()));
        assert_eq!(store.get("city"), Some("Kathmandu".to_string()));
        assert_eq!(store.get("age"), None);

        drop(store);

        let reopened = LsmStore::open(&sstable_dir, &wal_path).unwrap();

        assert_eq!(reopened.get("name"), Some("Bob".to_string()));
        assert_eq!(reopened.get("city"), Some("Kathmandu".to_string()));
        assert_eq!(reopened.get("age"), None);
        assert_eq!(reopened.installed_snapshot_offset().unwrap(), 50);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_install_prevents_old_wal_replay() {
        let dir = temp_dir("snapshot_old_wal");
        let sstable_dir = dir.join("sstables");
        let wal_path = dir.join("wal.log");

        let mut store = LsmStore::open(&sstable_dir, &wal_path).unwrap();

        store.set("name".to_string(), "Alice".to_string()).unwrap();

        let snapshot = BTreeMap::from([("name".to_string(), "Bob".to_string())]);

        store.install_snapshot(&snapshot, 75).unwrap();

        drop(store);

        let reopened = LsmStore::open(&sstable_dir, &wal_path).unwrap();

        assert_eq!(reopened.get("name"), Some("Bob".to_string()));
        assert_eq!(reopened.get("missing"), None);

        let _ = fs::remove_dir_all(&dir);
    }
}
