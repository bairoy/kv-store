use crate::storage::bloom_filter::BloomFilter;
use crate::storage::memtable::Memtable;
use crate::storage::sparse_index::SparseIndex;

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const SPARSE_INDEX_INTERVAL: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SstableEntry {
    Value(String),
    Tombstone,
}

#[derive(Debug, Clone)]
pub struct Sstable {
    path: PathBuf,
    index: SparseIndex,
    bloom: BloomFilter,
}

impl Sstable {
    /// Create an SSTable from the current Memtable.
    pub fn create(path: &Path, memtable: &Memtable) -> io::Result<Self> {
        Self::create_from_entries(path, memtable.iter())
    }

    /// Create an SSTable from sorted key-value entries.
    ///
    /// The entries are expected to be sorted by key.
    pub fn create_from_entries<'a, I>(path: &Path, entries: I) -> io::Result<Self>
    where
        I: IntoIterator<Item = (&'a String, &'a Option<String>)>,
    {
        let entries: Vec<(&String, &Option<String>)> = entries.into_iter().collect();

        let mut bloom = BloomFilter::for_items(entries.len());

        let temp_path = path.with_extension("tmp");

        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        let mut index_entries = Vec::new();

        // Current byte position in the SSTable.
        let mut offset: u64 = 0;

        for (position, (key, value)) in entries.iter().enumerate() {
            // Every Nth key becomes an index entry.
            if position % SPARSE_INDEX_INTERVAL == 0 {
                index_entries.push(((*key).clone(), offset));
            }

            // Add key to Bloom filter.
            bloom.insert(key);

            let line = match value {
                Some(value) => {
                    format!("SET\t{key}\t{value}\n")
                }

                None => {
                    format!("DEL\t{key}\n")
                }
            };

            writer.write_all(line.as_bytes())?;

            offset += line.len() as u64;
        }

        // Flush Rust's buffered data to the OS.
        writer.flush()?;

        // Make the SSTable durable.
        writer.get_ref().sync_all()?;

        // Atomically install the completed SSTable.
        fs::rename(&temp_path, path)?;

        Ok(Self {
            path: path.to_path_buf(),
            index: SparseIndex::new(index_entries),
            bloom,
        })
    }

    /// Open an existing SSTable.
    ///
    /// The sparse index and Bloom filter are rebuilt by scanning
    /// the SSTable once.
    pub fn open(path: PathBuf) -> io::Result<Self> {
        let mut index_entries = Vec::new();
        let mut bloom_keys = Vec::new();

        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        let mut offset: u64 = 0;
        let mut position: usize = 0;

        loop {
            let mut line = String::new();

            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                break;
            }

            let (key, _) = parse_record(&line)?;

            // Rebuild sparse index.
            if position % SPARSE_INDEX_INTERVAL == 0 {
                index_entries.push((key.clone(), offset));
            }

            // Rebuild Bloom filter.
            bloom_keys.push(key);

            offset += bytes_read as u64;
            position += 1;
        }

        let mut bloom = BloomFilter::for_items(bloom_keys.len());

        for key in &bloom_keys {
            bloom.insert(key);
        }

        Ok(Self {
            path,
            index: SparseIndex::new(index_entries),
            bloom,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn index(&self) -> &SparseIndex {
        &self.index
    }

    pub fn bloom(&self) -> &BloomFilter {
        &self.bloom
    }

    /// Search for a key using:
    ///
    /// Bloom filter
    ///      ↓
    /// Sparse index
    ///      ↓
    /// Bounded disk scan
    pub fn get(&self, key: &str) -> io::Result<Option<SstableEntry>> {
        // Bloom says definitely absent.
        if !self.bloom.might_contain(key) {
            return Ok(None);
        }

        // Find the byte range where the key could exist.
        let (start, end) = match self.index.range_for(key) {
            Some(range) => range,
            None => return Ok(None),
        };

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        // Jump directly to the approximate location.
        reader.seek(SeekFrom::Start(start))?;

        loop {
            let current_offset = reader.stream_position()?;

            // We reached the next sparse-index boundary.
            if let Some(end_offset) = end {
                if current_offset >= end_offset {
                    break;
                }
            }

            let mut line = String::new();

            let bytes_read = reader.read_line(&mut line)?;

            if bytes_read == 0 {
                break;
            }

            let (current_key, value) = parse_record(&line)?;

            match current_key.as_str().cmp(key) {
                std::cmp::Ordering::Less => {
                    continue;
                }

                std::cmp::Ordering::Equal => {
                    return Ok(Some(match value {
                        Some(value) => SstableEntry::Value(value),

                        None => SstableEntry::Tombstone,
                    }));
                }

                std::cmp::Ordering::Greater => {
                    // Because SSTable is sorted, the key
                    // cannot appear later.
                    break;
                }
            }
        }

        Ok(None)
    }

    /// Read the entire SSTable.
    ///
    /// Primarily used by compaction.
    pub fn read_all(&self) -> io::Result<Vec<(String, Option<String>)>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);

        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;

            let (key, value) = parse_record(&line)?;

            entries.push((key, value));
        }

        Ok(entries)
    }
}

/// Parse one SSTable record.
///
/// Format:
///
/// SET <tab> key <tab> value
///
/// or
///
/// DEL <tab> key
fn parse_record(line: &str) -> io::Result<(String, Option<String>)> {
    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    let parts: Vec<&str> = line.splitn(3, '\t').collect();

    match parts.as_slice() {
        ["SET", key, value] => Ok(((*key).to_string(), Some((*value).to_string()))),

        ["DEL", key] => Ok(((*key).to_string(), None)),

        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid SSTable record",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeMap;

    fn temp_path(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "kv_store_v7_sstable_{}_{}",
            name,
            std::process::id()
        ));

        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("tmp"));

        path
    }

    #[test]
    fn create_and_get() {
        let path = temp_path("create_get");

        let mut memtable = Memtable::new();

        memtable.set("apple".to_string(), "red".to_string());

        memtable.set("banana".to_string(), "yellow".to_string());

        let table = Sstable::create(&path, &memtable).unwrap();

        assert_eq!(
            table.get("apple").unwrap(),
            Some(SstableEntry::Value("red".to_string()))
        );

        assert_eq!(
            table.get("banana").unwrap(),
            Some(SstableEntry::Value("yellow".to_string()))
        );

        assert_eq!(table.get("missing").unwrap(), None);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn tombstone_is_found() {
        let path = temp_path("tombstone");

        let mut memtable = Memtable::new();

        memtable.set("apple".to_string(), "red".to_string());

        memtable.delete("apple".to_string());

        let table = Sstable::create(&path, &memtable).unwrap();

        assert_eq!(table.get("apple").unwrap(), Some(SstableEntry::Tombstone));

        let _ = fs::remove_file(path);
    }

    #[test]
    fn sparse_index_is_built() {
        let path = temp_path("index");

        let mut entries = BTreeMap::new();

        for i in 0..300 {
            entries.insert(format!("key-{i:03}"), Some(format!("value-{i}")));
        }

        let table = Sstable::create_from_entries(&path, entries.iter()).unwrap();

        // 300 entries / interval 128:
        //
        // positions:
        // 0
        // 128
        // 256
        //
        // => 3 index entries
        assert_eq!(table.index().len(), 3);

        let _ = fs::remove_file(path);
    }

    #[test]
    fn metadata_is_rebuilt_after_restart() {
        let path = temp_path("restart");

        let mut memtable = Memtable::new();

        for i in 0..300 {
            memtable.set(format!("key-{i:03}"), format!("value-{i}"));
        }

        let table = Sstable::create(&path, &memtable).unwrap();

        let original_index_len = table.index().len();

        let original_bloom_bits = table.bloom().bit_count();

        drop(table);

        // Simulate restart.
        let reopened = Sstable::open(path.clone()).unwrap();

        assert_eq!(reopened.index().len(), original_index_len);

        assert_eq!(reopened.bloom().bit_count(), original_bloom_bits);

        assert_eq!(
            reopened.get("key-150").unwrap(),
            Some(SstableEntry::Value("value-150".to_string()))
        );

        let _ = fs::remove_file(path);
    }
}
