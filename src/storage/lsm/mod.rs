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
        tracing::debug!("LsmStore::open: creating SSTable directory");
        fs::create_dir_all(&sstable_dir)?;

        tracing::debug!("LsmStore::open: recovering compaction");
        Self::recover_compaction(&sstable_dir)?;
        tracing::debug!("LsmStore::open: recovering snapshot installation");

        Self::recover_snapshot_install(&sstable_dir, &wal_path)?;

        tracing::debug!("LsmStore::open: loading SSTables");
        let sstables = Self::load_sstables(&sstable_dir)?;
        tracing::debug!("LsmStore::open: calculating next SSTable ID");
        let next_sstable_id = next_sstable_id_in(&sstable_dir)?;
        tracing::debug!("LsmStore::open: opening WAL");

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

    pub fn write_batch(&mut self, writes: Vec<(String, Option<String>)>) -> io::Result<()> {
        if writes.is_empty() {
            return Ok(());
        }

        let mut record = String::new();

        for (key, value) in &writes {
            match value {
                Some(value) => record.push_str(&format!("SET\t{key}\t{value}\n")),
                None => record.push_str(&format!("DEL\t{key}\n")),
            }
        }

        self.append_wal(&record)?;

        for (key, value) in writes {
            match value {
                Some(value) => self.memtable.set(key, value),
                None => self.memtable.delete(key),
            }
        }

        Ok(())
    }

    pub fn delete(&mut self, key: String) -> io::Result<Option<String>> {
        let old_value = self.get(&key);

        self.append_wal(&format!("DEL\t{key}\n"))?;

        self.memtable.delete(key);

        Ok(old_value)
    }

    pub fn get(&self, key: &str) -> Option<String> {
        if let Some(value) = self.memtable.get(key) {
            return value.clone();
        }

        for table in self.sstables.iter().rev() {
            match table.get(key) {
                Ok(Some(SstableEntry::Value(value))) => {
                    return Some(value);
                }

                Ok(Some(SstableEntry::Tombstone)) => {
                    return None;
                }

                Ok(None) => {
                    continue;
                }

                Err(error) => {
                    tracing::error!("SSTable read error for {:?}: {}", table.path(), error);

                    return None;
                }
            }
        }

        None
    }

    pub fn snapshot_entries(&self) -> io::Result<BTreeMap<String, String>> {
        let mut merged: BTreeMap<String, Option<String>> = BTreeMap::new();

        for table in &self.sstables {
            for (key, value) in table.read_all()? {
                merged.insert(key, value);
            }
        }

        for (key, value) in self.memtable.iter() {
            merged.insert(key.clone(), value.clone());
        }

        Ok(merged
            .into_iter()
            .filter_map(|(key, value)| value.map(|value| (key, value)))
            .collect())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        let path = self.next_sstable_path();

        let table = Sstable::create(&path, &self.memtable)?;

        self.truncate_wal()?;

        crate::metrics::inc("lsm_flushes_total", &[]);

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
