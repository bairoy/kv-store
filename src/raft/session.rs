//! Exactly-once client writes: the replicated session table.

use std::io;

use crate::raft::types::SessionOp;
use crate::storage::lsm::LsmStore;

pub const DEFAULT_MAX_SESSIONS: u64 = 10_000;

const SESSION_PREFIX: &str = "\u{1}sess:";
const QUEUE_PREFIX: &str = "\u{1}sessq:";
const META_KEY: &str = "\u{1}sessmeta";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEffect {
    Applied,
    Duplicate,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    Applied(String),
    Stale,
    Unknown,
}

pub fn valid_client_id(client: &str) -> bool {
    !client.is_empty()
        && client.len() <= 64
        && client
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn session_key(client: &str) -> String {
    format!("{SESSION_PREFIX}{client}")
}

fn queue_key(position: u64) -> String {
    format!("{QUEUE_PREFIX}{position:020}")
}

fn read_row(store: &LsmStore, client: &str) -> Option<(u64, String)> {
    let row = store.get(&session_key(client))?;
    let (seq, result) = row.split_once('|')?;

    Some((seq.parse().ok()?, result.to_string()))
}

fn read_meta(store: &LsmStore) -> (u64, u64) {
    store
        .get(META_KEY)
        .and_then(|text| {
            let (head, tail) = text.split_once(',')?;
            Some((head.parse().ok()?, tail.parse().ok()?))
        })
        .unwrap_or((0, 0))
}

pub fn apply(
    store: &mut LsmStore,
    client: &str,
    seq: u64,
    op: &SessionOp,
    max_sessions: u64,
) -> io::Result<SessionEffect> {
    let row = read_row(store, client);

    #[cfg(feature = "fault-injection")]
    let row = if std::env::var_os("KV_FAULT_NO_DEDUP").is_some() {
        None
    } else {
        row
    };

    if let Some((last, _)) = &row {
        if seq == *last {
            return Ok(SessionEffect::Duplicate);
        }

        if seq < *last {
            return Ok(SessionEffect::Stale);
        }
    }

    let (result, mut writes) = run(store, op);

    if row.is_none() {
        register(store, client, max_sessions.max(1), &mut writes);
    }

    writes.push((session_key(client), Some(format!("{seq}|{result}"))));

    store.write_batch(writes)?;

    Ok(SessionEffect::Applied)
}

pub fn lookup(store: &LsmStore, client: &str, seq: u64) -> Lookup {
    match read_row(store, client) {
        Some((last, result)) if last == seq => Lookup::Applied(result),
        Some((last, _)) if seq < last => Lookup::Stale,
        _ => Lookup::Unknown,
    }
}

fn run(store: &LsmStore, op: &SessionOp) -> (String, Vec<(String, Option<String>)>) {
    if op.key().starts_with('\u{1}') {
        return ("E:that key is reserved".to_string(), Vec::new());
    }

    match op {
        SessionOp::Set { key, value } => {
            ("OK".to_string(), vec![(key.clone(), Some(value.clone()))])
        }
        SessionOp::Delete { key } => {
            let result = if store.get(key).is_some() {
                "OK"
            } else {
                "NOTFOUND"
            };

            (result.to_string(), vec![(key.clone(), None)])
        }
        SessionOp::Incr { key } => {
            let current = match store.get(key) {
                None => Ok(0),
                Some(text) => text.trim().parse::<i64>(),
            };

            match current.ok().and_then(|number| number.checked_add(1)) {
                Some(next) => (
                    format!("V:{next}"),
                    vec![(key.clone(), Some(next.to_string()))],
                ),
                None => (
                    "E:the value is not an integer (or would overflow)".to_string(),
                    Vec::new(),
                ),
            }
        }
    }
}

fn register(
    store: &LsmStore,
    client: &str,
    max_sessions: u64,
    writes: &mut Vec<(String, Option<String>)>,
) {
    let (mut head, mut tail) = read_meta(store);

    writes.push((queue_key(tail), Some(client.to_string())));
    tail += 1;

    while tail - head > max_sessions {
        if let Some(victim) = store.get(&queue_key(head)) {
            writes.push((session_key(&victim), None));
        }

        writes.push((queue_key(head), None));
        head += 1;
    }

    writes.push((META_KEY.to_string(), Some(format!("{head},{tail}"))));
}

pub fn session_count(store: &LsmStore) -> u64 {
    let (head, tail) = read_meta(store);
    tail - head
}

pub fn is_error_result(result: &str) -> Option<&str> {
    result.strip_prefix("E:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn open_store(name: &str) -> (LsmStore, PathBuf) {
        let dir = std::env::temp_dir()
            .join("kv_store_session_tests")
            .join(format!(
                "{name}_{}_{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&dir).expect("create dir");

        let store = LsmStore::open(dir.join("sstables"), dir.join("wal.log")).expect("open store");

        (store, dir)
    }

    fn incr(key: &str) -> SessionOp {
        SessionOp::Incr { key: key.into() }
    }

    fn set(key: &str, value: &str) -> SessionOp {
        SessionOp::Set {
            key: key.into(),
            value: value.into(),
        }
    }

    const CAP: u64 = 100;

    #[test]
    fn a_retried_request_is_applied_once() {
        let (mut store, dir) = open_store("once");

        assert_eq!(
            apply(&mut store, "c1", 1, &incr("n"), CAP).unwrap(),
            SessionEffect::Applied
        );
        assert_eq!(
            apply(&mut store, "c1", 1, &incr("n"), CAP).unwrap(),
            SessionEffect::Duplicate
        );
        assert_eq!(
            apply(&mut store, "c1", 1, &incr("n"), CAP).unwrap(),
            SessionEffect::Duplicate
        );

        assert_eq!(store.get("n").as_deref(), Some("1"));
        assert_eq!(lookup(&store, "c1", 1), Lookup::Applied("V:1".into()));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_new_seq_runs_and_an_old_seq_is_ignored() {
        let (mut store, dir) = open_store("order");

        apply(&mut store, "c1", 1, &incr("n"), CAP).unwrap();
        apply(&mut store, "c1", 2, &incr("n"), CAP).unwrap();

        assert_eq!(
            apply(&mut store, "c1", 1, &incr("n"), CAP).unwrap(),
            SessionEffect::Stale
        );

        assert_eq!(store.get("n").as_deref(), Some("2"));
        assert_eq!(lookup(&store, "c1", 1), Lookup::Stale);
        assert_eq!(lookup(&store, "c1", 2), Lookup::Applied("V:2".into()));
        assert_eq!(lookup(&store, "c1", 3), Lookup::Unknown);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_retry_does_not_overwrite_a_newer_write_by_someone_else() {
        let (mut store, dir) = open_store("lost_update");

        apply(&mut store, "a", 1, &set("k", "1"), CAP).unwrap();
        apply(&mut store, "b", 1, &set("k", "2"), CAP).unwrap();
        apply(&mut store, "a", 1, &set("k", "1"), CAP).unwrap();

        assert_eq!(store.get("k").as_deref(), Some("2"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn clients_are_independent() {
        let (mut store, dir) = open_store("independent");

        apply(&mut store, "a", 1, &incr("n"), CAP).unwrap();
        apply(&mut store, "b", 1, &incr("n"), CAP).unwrap();

        assert_eq!(store.get("n").as_deref(), Some("2"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn delete_remembers_whether_the_key_existed() {
        let (mut store, dir) = open_store("delete");

        store.set("k".into(), "v".into()).unwrap();

        let delete = SessionOp::Delete { key: "k".into() };
        apply(&mut store, "c", 1, &delete, CAP).unwrap();
        assert_eq!(lookup(&store, "c", 1), Lookup::Applied("OK".into()));

        apply(&mut store, "c", 2, &delete, CAP).unwrap();
        assert_eq!(lookup(&store, "c", 2), Lookup::Applied("NOTFOUND".into()));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn incr_on_a_non_integer_is_an_error_and_changes_nothing() {
        let (mut store, dir) = open_store("bad_incr");

        store.set("k".into(), "hello".into()).unwrap();
        apply(&mut store, "c", 1, &incr("k"), CAP).unwrap();

        assert_eq!(store.get("k").as_deref(), Some("hello"));
        assert!(matches!(lookup(&store, "c", 1), Lookup::Applied(r) if r.starts_with("E:")));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_table_is_bounded_and_forgets_the_oldest_client() {
        let (mut store, dir) = open_store("bounded");

        for client in ["a", "b", "c"] {
            apply(&mut store, client, 1, &incr("n"), 2).unwrap();
        }

        assert_eq!(session_count(&store), 2);
        assert_eq!(lookup(&store, "a", 1), Lookup::Unknown, "a was evicted");
        assert_eq!(lookup(&store, "b", 1), Lookup::Applied("V:2".into()));
        assert_eq!(lookup(&store, "c", 1), Lookup::Applied("V:3".into()));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_table_survives_a_restart_and_a_replay_does_not_double_apply() {
        let (mut store, dir) = open_store("restart");

        apply(&mut store, "c", 1, &incr("n"), CAP).unwrap();
        drop(store);

        let mut store = LsmStore::open(dir.join("sstables"), dir.join("wal.log")).expect("reopen");

        assert_eq!(
            apply(&mut store, "c", 1, &incr("n"), CAP).unwrap(),
            SessionEffect::Duplicate
        );
        assert_eq!(store.get("n").as_deref(), Some("1"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn session_rows_are_part_of_the_snapshot() {
        let (mut store, dir) = open_store("snapshot");

        apply(&mut store, "c", 7, &incr("n"), CAP).unwrap();

        let entries = store.snapshot_entries().unwrap();

        assert!(entries.contains_key(&session_key("c")));
        assert!(entries.contains_key(META_KEY));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn client_ids_are_validated() {
        assert!(valid_client_id("c-7f3a.1_x"));
        assert!(!valid_client_id(""));
        assert!(!valid_client_id("has space"));
        assert!(!valid_client_id("tab\there"));
        assert!(!valid_client_id(&"x".repeat(65)));
    }
}
