use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};

pub struct Store {
    map: HashMap<String, String>,
    wal: BufWriter<File>,
}

impl Store {
    pub fn open(snapshot_path: &str, wal_path: &str) -> io::Result<Self> {
        let mut map = HashMap::new();

        match File::open(snapshot_path) {
            Ok(file) => {
                let reader = BufReader::new(file);

                for line in reader.lines() {
                    let line = line?;
                    let parts: Vec<&str> = line.splitn(2, '\t').collect();

                    if let [key, value] = parts.as_slice() {
                        map.insert(key.to_string(), value.to_string());
                    }
                }
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {}

            Err(error) => return Err(error),
        }

        match File::open(wal_path) {
            Ok(file) => {
                let reader = BufReader::new(file);

                for line in reader.lines() {
                    let line = line?;
                    let parts: Vec<&str> = line.split('\t').collect();

                    match parts.as_slice() {
                        ["SET", key, value] => {
                            map.insert(key.to_string(), value.to_string());
                        }

                        ["DEL", key] => {
                            map.remove(*key);
                        }

                        _ => {}
                    }
                }
            }

            Err(error) if error.kind() == io::ErrorKind::NotFound => {}

            Err(error) => return Err(error),
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(wal_path)?;

        Ok(Self {
            map,
            wal: BufWriter::new(file),
        })
    }

    pub fn snapshot(&mut self, snapshot_path: &str, wal_path: &str) -> io::Result<()> {
        self.wal.flush()?;
        self.wal.get_ref().sync_all()?;

        let temp_path = format!("{snapshot_path}.tmp");

        {
            let mut file = File::create(&temp_path)?;

            for (key, value) in &self.map {
                writeln!(file, "{key}\t{value}")?;
            }

            file.sync_all()?;
        }

        std::fs::rename(&temp_path, snapshot_path)?;

        let new_wal = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(wal_path)?;

        self.wal = BufWriter::new(new_wal);

        Ok(())
    }

    pub fn set(&mut self, key: String, value: String) -> io::Result<()> {
        let entry = LogEntry::Set(key.clone(), value.clone());

        self.wal.write_all(entry.encode().as_bytes())?;
        self.wal.flush()?;
        self.wal.get_ref().sync_all()?;

        self.map.insert(key, value);

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> io::Result<Option<String>> {
        let entry = LogEntry::Delete(key.to_string());

        self.wal.write_all(entry.encode().as_bytes())?;
        self.wal.flush()?;
        self.wal.get_ref().sync_all()?;

        Ok(self.map.remove(key))
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.map.get(key)
    }
}

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
        let snapshot = "test_snapshot_1.db";
        let wal = "test_store_1.log";

        let mut store = Store::open(snapshot, wal).unwrap();

        store.set("name".to_string(), "Baiju".to_string()).unwrap();

        assert_eq!(store.get("name"), Some(&"Baiju".to_string()));

        drop(store);
        cleanup(snapshot, wal);
    }

    #[test]
    fn delete_then_get_returns_none() {
        let snapshot = "test_snapshot_2.db";
        let wal = "test_store_2.log";

        let mut store = Store::open(snapshot, wal).unwrap();

        store.set("name".to_string(), "Baiju".to_string()).unwrap();

        store.delete("name").unwrap();

        assert_eq!(store.get("name"), None);

        drop(store);
        cleanup(snapshot, wal);
    }

    #[test]
    fn data_survives_restart() {
        let snapshot = "test_snapshot_3.db";
        let wal = "test_store_3.log";

        {
            let mut store = Store::open(snapshot, wal).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();
        }

        {
            let store = Store::open(snapshot, wal).unwrap();

            assert_eq!(store.get("name"), Some(&"Baiju".to_string()));

            assert_eq!(store.get("language"), Some(&"Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn snapshot_survives_restart() {
        let snapshot = "test_snapshot_4.db";
        let wal = "test_store_4.log";

        {
            let mut store = Store::open(snapshot, wal).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();

            store.snapshot(snapshot, wal).unwrap();
        }

        {
            let store = Store::open(snapshot, wal).unwrap();

            assert_eq!(store.get("name"), Some(&"Baiju".to_string()));

            assert_eq!(store.get("language"), Some(&"Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn wal_tail_replays_after_snapshot() {
        let snapshot = "test_snapshot_5.db";
        let wal = "test_store_5.log";

        {
            let mut store = Store::open(snapshot, wal).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store.snapshot(snapshot, wal).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();
        }

        {
            let store = Store::open(snapshot, wal).unwrap();

            assert_eq!(store.get("name"), Some(&"Baiju".to_string()));

            assert_eq!(store.get("language"), Some(&"Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn snapshot_truncates_wal() {
        let snapshot = "test_snapshot_6.db";
        let wal = "test_store_6.log";

        {
            let mut store = Store::open(snapshot, wal).unwrap();

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
        let snapshot = "test_snapshot_7.db";
        let wal = "test_store_7.log";

        {
            let mut store = Store::open(snapshot, wal).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store.snapshot(snapshot, wal).unwrap();

            store.delete("name").unwrap();
        }

        {
            let store = Store::open(snapshot, wal).unwrap();

            assert_eq!(store.get("name"), None);
        }

        cleanup(snapshot, wal);
    }

    #[test]
    fn snapshot_and_old_wal_replay_safely() {
        let snapshot = "test_snapshot_8.db";
        let wal = "test_store_8.log";

        {
            let mut store = Store::open(snapshot, wal).unwrap();

            store.set("name".to_string(), "Baiju".to_string()).unwrap();

            store
                .set("language".to_string(), "Rust".to_string())
                .unwrap();

            store.wal.flush().unwrap();
            store.wal.get_ref().sync_all().unwrap();

            let temp_path = format!("{snapshot}.tmp");

            {
                let mut file = File::create(&temp_path).unwrap();

                for (key, value) in &store.map {
                    writeln!(file, "{key}\t{value}").unwrap();
                }

                file.sync_all().unwrap();
            }

            std::fs::rename(&temp_path, snapshot).unwrap();
        }

        {
            let store = Store::open(snapshot, wal).unwrap();

            assert_eq!(store.get("name"), Some(&"Baiju".to_string()));

            assert_eq!(store.get("language"), Some(&"Rust".to_string()));
        }

        cleanup(snapshot, wal);
    }
}
