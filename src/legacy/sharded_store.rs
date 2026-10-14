use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::sync::Mutex;

const DEFAULT_SHARD_COUNT: usize = 16;

struct Shard {
    map: Mutex<HashMap<String, String>>,
}

pub struct ShardedStore {
    shards: Vec<Shard>,
    wal: Mutex<BufWriter<File>>,
}

impl ShardedStore {
    // ---------------------------------------------------------
    // OPEN
    // ---------------------------------------------------------

    pub fn open(snapshot_path: &str, wal_path: &str) -> io::Result<Self> {
        Self::open_with_shards(snapshot_path, wal_path, DEFAULT_SHARD_COUNT)
    }

    pub fn open_with_shards(
        snapshot_path: &str,
        wal_path: &str,
        shard_count: usize,
    ) -> io::Result<Self> {
        if shard_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shard count must be greater than zero",
            ));
        }

        let shards = (0..shard_count)
            .map(|_| Shard {
                map: Mutex::new(HashMap::new()),
            })
            .collect();

        let store = Self {
            shards,

            wal: Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(wal_path)?,
            )),
        };

        // Recover state.
        store.load_snapshot(snapshot_path)?;
        store.replay_wal(wal_path)?;

        Ok(store)
    }

    // ---------------------------------------------------------
    // SHARD SELECTION
    // ---------------------------------------------------------

    fn shard_index(&self, key: &str) -> usize {
        let mut hasher = DefaultHasher::new();

        key.hash(&mut hasher);

        (hasher.finish() as usize) % self.shards.len()
    }

    // ---------------------------------------------------------
    // SNAPSHOT RECOVERY
    // ---------------------------------------------------------

    fn load_snapshot(&self, snapshot_path: &str) -> io::Result<()> {
        match File::open(snapshot_path) {
            Ok(file) => {
                let reader = BufReader::new(file);

                for line in reader.lines() {
                    let line = line?;

                    let parts: Vec<&str> = line.splitn(2, '\t').collect();

                    if let [key, value] = parts.as_slice() {
                        let index = self.shard_index(key);

                        let mut shard = self.shards[index].map.lock().unwrap();

                        shard.insert(key.to_string(), value.to_string());
                    }
                }
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // No snapshot yet.
            }

            Err(error) => return Err(error),
        }

        Ok(())
    }

    fn replay_wal(&self, wal_path: &str) -> io::Result<()> {
        match File::open(wal_path) {
            Ok(file) => {
                let reader = BufReader::new(file);

                for line in reader.lines() {
                    let line = line?;

                    let parts: Vec<&str> = line.split('\t').collect();

                    match parts.as_slice() {
                        ["SET", key, value] => {
                            let index = self.shard_index(key);

                            let mut shard = self.shards[index].map.lock().unwrap();

                            shard.insert(key.to_string(), value.to_string());
                        }

                        ["DEL", key] => {
                            let index = self.shard_index(key);

                            let mut shard = self.shards[index].map.lock().unwrap();

                            shard.remove(*key);
                        }

                        _ => {}
                    }
                }
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // No WAL yet.
            }

            Err(error) => return Err(error),
        }

        Ok(())
    }

    // ---------------------------------------------------------
    // SET
    // ---------------------------------------------------------

    pub fn set(&self, key: String, value: String) -> io::Result<()> {
        let entry = LogEntry::Set(key.clone(), value.clone());

        // WAL must become durable first.
        {
            let mut wal = self.wal.lock().unwrap();

            wal.write_all(entry.encode().as_bytes())?;

            wal.flush()?;

            wal.get_ref().sync_all()?;
        }

        // Then update only the required shard.
        let index = self.shard_index(&key);

        let mut shard = self.shards[index].map.lock().unwrap();

        shard.insert(key, value);

        Ok(())
    }

    // ---------------------------------------------------------
    // GET
    // ---------------------------------------------------------

    pub fn get(&self, key: &str) -> Option<String> {
        let index = self.shard_index(key);

        let shard = self.shards[index].map.lock().unwrap();

        shard.get(key).cloned()
    }

    // ---------------------------------------------------------
    // DELETE
    // ---------------------------------------------------------

    pub fn delete(&self, key: &str) -> io::Result<Option<String>> {
        let entry = LogEntry::Delete(key.to_string());

        // WAL must become durable first.
        {
            let mut wal = self.wal.lock().unwrap();

            wal.write_all(entry.encode().as_bytes())?;

            wal.flush()?;

            wal.get_ref().sync_all()?;
        }

        let index = self.shard_index(key);

        let mut shard = self.shards[index].map.lock().unwrap();

        Ok(shard.remove(key))
    }

    // ---------------------------------------------------------
    // SNAPSHOT
    // ---------------------------------------------------------

    pub fn snapshot(&self, snapshot_path: &str, wal_path: &str) -> io::Result<()> {
        // First make the WAL durable.
        {
            let mut wal = self.wal.lock().unwrap();

            wal.flush()?;
            wal.get_ref().sync_all()?;
        }

        let temp_path = format!("{snapshot_path}.tmp");

        {
            let mut file = File::create(&temp_path)?;

            // Write every shard into the snapshot.
            for shard in &self.shards {
                let shard = shard.map.lock().unwrap();

                for (key, value) in shard.iter() {
                    writeln!(file, "{key}\t{value}")?;
                }
            }

            file.sync_all()?;
        }

        // Atomically install snapshot.
        std::fs::rename(&temp_path, snapshot_path)?;

        // Snapshot is durable, so truncate WAL.
        let new_wal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(wal_path)?;

        let mut wal = self.wal.lock().unwrap();

        *wal = BufWriter::new(new_wal);

        Ok(())
    }
}

// =============================================================
// WAL ENTRY
// =============================================================

pub enum LogEntry {
    Set(String, String),
    Delete(String),
}

impl LogEntry {
    fn encode(&self) -> String {
        match self {
            LogEntry::Set(key, value) => {
                format!("SET\t{key}\t{value}\n")
            }

            LogEntry::Delete(key) => {
                format!("DEL\t{key}\n")
            }
        }
    }
}

// =============================================================
// TESTS
// =============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn cleanup(snapshot: &str, wal: &str) {
        let _ = std::fs::remove_file(snapshot);

        let _ = std::fs::remove_file(wal);

        let _ = std::fs::remove_file(format!("{snapshot}.tmp"));
    }

    #[test]
    fn set_then_get_returns_value() {
        let snapshot = "test_sharded_snapshot_1.db";

        let wal = "test_sharded_store_1.log";

        let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

        store.set("name".to_string(), "Baiju".to_string()).unwrap();

        assert_eq!(store.get("name"), Some("Baiju".to_string()));

        cleanup(snapshot, wal);
    }

    #[test]
    fn delete_then_get_returns_none() {
        let snapshot = "test_sharded_snapshot_2.db";

        let wal = "test_sharded_store_2.log";

        let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

        store.set("name".to_string(), "Baiju".to_string()).unwrap();

        store.delete("name").unwrap();

        assert_eq!(store.get("name"), None);

        cleanup(snapshot, wal);
    }

    #[test]
    fn delete_missing_key_returns_none() {
        let snapshot = "test_sharded_snapshot_3.db";

        let wal = "test_sharded_store_3.log";

        let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

        assert_eq!(store.delete("missing").unwrap(), None);

        cleanup(snapshot, wal);
    }

    #[test]
    fn data_survives_restart() {
        let snapshot = "test_sharded_snapshot_4.db";

        let wal = "test_sharded_store_4.log";

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();
        }

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            assert_eq!(store.get("name"), Some("Baiju".to_string()));

            assert_eq!(store.get("language"), Some("Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn snapshot_survives_restart() {
        let snapshot = "test_sharded_snapshot_5.db";

        let wal = "test_sharded_store_5.log";

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();

            store.snapshot(snapshot, wal).unwrap();
        }

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            assert_eq!(store.get("name"), Some("Baiju".to_string()));

            assert_eq!(store.get("language"), Some("Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn wal_tail_replays_after_snapshot() {
        let snapshot = "test_sharded_snapshot_6.db";

        let wal = "test_sharded_store_6.log";

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store.snapshot(snapshot, wal).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();
        }

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            assert_eq!(store.get("name"), Some("Baiju".to_string()));

            assert_eq!(store.get("language"), Some("Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn snapshot_truncates_wal() {
        let snapshot = "test_sharded_snapshot_7.db";

        let wal = "test_sharded_store_7.log";

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();

            store.snapshot(snapshot, wal).unwrap();
        }

        let contents = std::fs::read_to_string(wal).unwrap();

        assert!(contents.is_empty());

        cleanup(snapshot, wal);
    }

    #[test]
    fn snapshot_then_delete_survives_restart() {
        let snapshot = "test_sharded_snapshot_8.db";

        let wal = "test_sharded_store_8.log";

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store.snapshot(snapshot, wal).unwrap();

            store.delete("name").unwrap();
        }

        {
            let store = ShardedStore::open_with_shards(snapshot, wal, 4).unwrap();

            assert_eq!(store.get("name"), None);
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn multiple_shards_are_created() {
        let snapshot = "test_sharded_snapshot_9.db";

        let wal = "test_sharded_store_9.log";

        let store = ShardedStore::open_with_shards(snapshot, wal, 8).unwrap();

        assert_eq!(store.shards.len(), 8);

        cleanup(snapshot, wal);
    }
    #[test]
    fn concurrent_operations_are_safe() {
        use std::sync::Arc;
        use std::thread;

        let snapshot = "test_concurrent_snapshot.db";
        let wal = "test_concurrent_store.log";

        let store = Arc::new(ShardedStore::open_with_shards(snapshot, wal, 16).unwrap());

        let mut handles = Vec::new();

        for thread_id in 0..16 {
            let store = Arc::clone(&store);

            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("key-{thread_id}-{i}");

                    let value = format!("value-{thread_id}-{i}");

                    store.set(key.clone(), value.clone()).unwrap();

                    assert_eq!(store.get(&key), Some(value));
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that all threads' writes survived.
        for thread_id in 0..16 {
            for i in 0..100 {
                let key = format!("key-{thread_id}-{i}");

                let expected = format!("value-{thread_id}-{i}");

                assert_eq!(store.get(&key), Some(expected));
            }
        }

        cleanup(snapshot, wal);
    }
}
