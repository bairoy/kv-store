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
