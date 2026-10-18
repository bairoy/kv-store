use crate::storage::memtable::Memtable;
use crate::storage::sstable::{Sstable, SstableEntry};

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

mod compaction;
mod snapshot_install;
#[cfg(test)]
mod tests;
mod wal;

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
