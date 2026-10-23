//! The state-machine half of cross-partition transactions (two-phase commit).
//!
//! The replicated log orders three kinds of step (`TxnCommand`); this module
//! says what each one *does* to a partition's key-value store. Every piece of
//! transaction state lives in the store itself, under a reserved key prefix,
//! so it is made durable, snapshotted and recovered by the machinery that
//! already protects ordinary data.
//!
//! ```text
//! \u{1}txn:rec:<id>        the writes this partition promised to apply
//! \u{1}txn:lock:<key>      <id> of the transaction that has locked <key>
//! \u{1}txn:vote:<id>       YES / NO   (this partition's phase-1 answer)
//! \u{1}txn:decision:<id>   COMMIT / ABORT   (coordinator partition only)
//! \u{1}txn:done:<id>       COMMIT / ABORT   (tombstone: finished here)
//! ```
//!
//! Every step is **idempotent**: after a crash, entries are re-applied from
//! the log, and applying one twice must leave the same state.
//!
//! What this gives, and what it does not: a transaction is atomic across
//! partitions (all writes happen or none), conflicting transactions cannot
//! interleave (locks; the loser aborts instead of waiting, so no deadlock),
//! and the decision survives crashes (it is a Raft entry). Plain reads and
//! plain single-key writes do not look at the locks, so a reader can briefly
//! see one partition's part of a transaction before another's.

use std::io;

use crate::raft::codec::{invalid_data, put_ops, put_u32, read_ops, read_u32};
use crate::raft::types::{TxnCommand, TxnOp};
use crate::storage::lsm::LsmStore;

const PREFIX: &str = "\u{1}txn:";

/// A list of writes as one hex word (safe inside a space-separated command).
pub fn ops_to_hex(ops: &[TxnOp]) -> io::Result<String> {
    let mut bytes = Vec::new();
    put_ops(&mut bytes, ops)?;

    Ok(hex_encode(&bytes))
}

pub fn ops_from_hex(text: &str) -> io::Result<Vec<TxnOp>> {
    let bytes = hex_decode(text)?;
    let mut cursor = 0;
    let ops = read_ops(&bytes, &mut cursor)?;

    if cursor != bytes.len() {
        return Err(invalid_data("trailing bytes in transaction ops"));
    }

    Ok(ops)
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn hex_decode(text: &str) -> io::Result<Vec<u8>> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return Err(invalid_data("malformed hex"));
    }

    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16))
        .collect::<Result<_, _>>()
        .map_err(|_| invalid_data("malformed hex"))
}

/// Keys users may not touch: they hold transaction bookkeeping.
pub fn is_reserved(key: &str) -> bool {
    key.starts_with(PREFIX)
}

fn record_key(id: &str) -> String {
    format!("{PREFIX}rec:{id}")
}

fn lock_key(key: &str) -> String {
    format!("{PREFIX}lock:{key}")
}

fn vote_key(id: &str) -> String {
    format!("{PREFIX}vote:{id}")
}

fn decision_key(id: &str) -> String {
    format!("{PREFIX}decision:{id}")
}

fn done_key(id: &str) -> String {
    format!("{PREFIX}done:{id}")
}

/// What a partition has promised for a prepared transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTxn {
    pub id: String,
    pub coordinator: u32,
    pub participants: Vec<u32>,
    pub ops: Vec<TxnOp>,
}

impl PendingTxn {
    fn encode(&self) -> io::Result<String> {
        let mut bytes = Vec::new();

        put_u32(&mut bytes, self.coordinator);
        put_u32(&mut bytes, self.participants.len() as u32);
        for participant in &self.participants {
            put_u32(&mut bytes, *participant);
        }
        put_ops(&mut bytes, &self.ops)?;

        Ok(hex_encode(&bytes))
    }

    fn decode(id: &str, text: &str) -> io::Result<Self> {
        let bytes = hex_decode(text)?;

        let mut cursor = 0;
        let coordinator = read_u32(&bytes, &mut cursor)?;

        let count = read_u32(&bytes, &mut cursor)? as usize;
        if count > bytes.len().saturating_sub(cursor) / 4 {
            return Err(invalid_data("participant list is longer than the record"));
        }

        let mut participants = Vec::with_capacity(count);
        for _ in 0..count {
            participants.push(read_u32(&bytes, &mut cursor)?);
        }

        let ops = read_ops(&bytes, &mut cursor)?;

        if cursor != bytes.len() {
            return Err(invalid_data("trailing bytes in transaction record"));
        }

        Ok(Self {
            id: id.to_string(),
            coordinator,
            participants,
            ops,
        })
    }
}

/// What applying a step changed about the set of in-flight transactions, so
/// the server can keep its in-memory index current without rescanning.
#[derive(Debug, PartialEq, Eq)]
pub enum TxnEffect {
    Nothing,
    Prepared(PendingTxn),
    Finished(String),
}

/// Apply one replicated step to the store. Idempotent.
pub fn apply(store: &mut LsmStore, command: &TxnCommand) -> io::Result<TxnEffect> {
    match command {
        TxnCommand::Prepare {
            id,
            coordinator,
            participants,
            ops,
        } => {
            // Already voted (a replay) or already finished (a Prepare that
            // arrived late, after the transaction was aborted): ignore.
            if store.get(&vote_key(id)).is_some() || store.get(&done_key(id)).is_some() {
                return Ok(TxnEffect::Nothing);
            }

            // Another transaction holds one of these keys: vote NO. We never
            // wait for a lock, so two transactions cannot deadlock.
            let conflict = ops
                .iter()
                .any(|op| matches!(store.get(&lock_key(op.key())), Some(holder) if holder != *id));

            if conflict {
                store.set(vote_key(id), "NO".to_string())?;
                return Ok(TxnEffect::Nothing);
            }

            let pending = PendingTxn {
                id: id.clone(),
                coordinator: *coordinator,
                participants: participants.clone(),
                ops: ops.clone(),
            };

            for op in ops {
                store.set(lock_key(op.key()), id.clone())?;
            }
            store.set(record_key(id), pending.encode()?)?;

            // The vote is written last: until it exists, a replay redoes the
            // (idempotent) steps above.
            store.set(vote_key(id), "YES".to_string())?;

            Ok(TxnEffect::Prepared(pending))
        }

        TxnCommand::Decide { id, commit } => {
            // The first decision wins; it is the transaction's commit point.
            if store.get(&decision_key(id)).is_none() {
                let outcome = if *commit { "COMMIT" } else { "ABORT" };
                store.set(decision_key(id), outcome.to_string())?;
            }

            Ok(TxnEffect::Nothing)
        }

        TxnCommand::Finish { id, commit } => {
            if store.get(&done_key(id)).is_some() {
                return Ok(TxnEffect::Nothing);
            }

            if let Some(text) = store.get(&record_key(id)) {
                let pending = PendingTxn::decode(id, &text)?;

                if *commit {
                    for op in &pending.ops {
                        match op {
                            TxnOp::Set { key, value } => store.set(key.clone(), value.clone())?,
                            TxnOp::Delete { key } => {
                                store.delete(key.clone())?;
                            }
                        }
                    }
                }

                for op in &pending.ops {
                    let lock = lock_key(op.key());

                    if store.get(&lock).as_deref() == Some(id.as_str()) {
                        store.delete(lock)?;
                    }
                }

                store.delete(record_key(id))?;
            }

            store.delete(vote_key(id))?;

            let outcome = if *commit { "COMMIT" } else { "ABORT" };
            store.set(done_key(id), outcome.to_string())?;

            Ok(TxnEffect::Finished(id.clone()))
        }
    }
}

/// The answer a step produced, read back after it was applied: the vote for a
/// `Prepare`, the winning decision for a `Decide`, `OK` for a `Finish`.
pub fn outcome(store: &LsmStore, command: &TxnCommand) -> String {
    match command {
        TxnCommand::Prepare { id, .. } => store.get(&vote_key(id)).unwrap_or_else(|| {
            // No vote and no lock means the transaction already finished.
            if store.get(&done_key(id)).is_some() {
                "NO".to_string()
            } else {
                "NONE".to_string()
            }
        }),
        TxnCommand::Decide { id, .. } => store
            .get(&decision_key(id))
            .unwrap_or_else(|| "NONE".to_string()),
        TxnCommand::Finish { .. } => "OK".to_string(),
    }
}

/// The recorded decision for `id`, if this partition is its coordinator and
/// one was made.
pub fn decision(store: &LsmStore, id: &str) -> Option<String> {
    store.get(&decision_key(id))
}

/// Every transaction this partition has prepared but not finished. Used when
/// a server starts, or installs a snapshot, to rebuild its in-memory index.
pub fn scan_pending(store: &LsmStore) -> io::Result<Vec<PendingTxn>> {
    let prefix = record_key("");
    let mut pending = Vec::new();

    for (key, value) in store.snapshot_entries()? {
        if let Some(id) = key.strip_prefix(&prefix) {
            pending.push(PendingTxn::decode(id, &value)?);
        }
    }

    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn open_store(name: &str) -> (LsmStore, PathBuf) {
        let dir = std::env::temp_dir()
            .join("kv_store_txn_tests")
            .join(format!(
                "{name}_{}_{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&dir).expect("create dir");

        let store = LsmStore::open(dir.join("sstables"), dir.join("wal.log")).expect("open store");

        (store, dir)
    }

    fn set(key: &str, value: &str) -> TxnOp {
        TxnOp::Set {
            key: key.into(),
            value: value.into(),
        }
    }

    fn prepare(id: &str, ops: Vec<TxnOp>) -> TxnCommand {
        TxnCommand::Prepare {
            id: id.into(),
            coordinator: 1,
            participants: vec![1, 2],
            ops,
        }
    }

    fn finish(id: &str, commit: bool) -> TxnCommand {
        TxnCommand::Finish {
            id: id.into(),
            commit,
        }
    }

    #[test]
    fn committing_applies_the_writes_and_releases_the_locks() {
        let (mut store, dir) = open_store("commit");
        store.set("old".into(), "x".into()).unwrap();

        let command = prepare(
            "t1",
            vec![set("a", "1"), TxnOp::Delete { key: "old".into() }],
        );

        assert!(matches!(
            apply(&mut store, &command).unwrap(),
            TxnEffect::Prepared(_)
        ));
        assert_eq!(outcome(&store, &command), "YES");

        // Nothing is visible until the transaction finishes.
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("old"), Some("x".into()));

        assert_eq!(
            apply(&mut store, &finish("t1", true)).unwrap(),
            TxnEffect::Finished("t1".into())
        );

        assert_eq!(store.get("a"), Some("1".into()));
        assert_eq!(store.get("old"), None);
        assert!(scan_pending(&store).unwrap().is_empty());

        // The key is free again for the next transaction.
        let next = prepare("t2", vec![set("a", "2")]);
        apply(&mut store, &next).unwrap();
        assert_eq!(outcome(&store, &next), "YES");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn aborting_discards_the_writes_and_releases_the_locks() {
        let (mut store, dir) = open_store("abort");

        apply(&mut store, &prepare("t1", vec![set("a", "1")])).unwrap();
        apply(&mut store, &finish("t1", false)).unwrap();

        assert_eq!(store.get("a"), None);

        let next = prepare("t2", vec![set("a", "2")]);
        apply(&mut store, &next).unwrap();
        assert_eq!(outcome(&store, &next), "YES", "the lock must be gone");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_transaction_touching_a_locked_key_votes_no_and_changes_nothing() {
        let (mut store, dir) = open_store("conflict");

        apply(&mut store, &prepare("t1", vec![set("shared", "1")])).unwrap();

        let rival = prepare("t2", vec![set("other", "x"), set("shared", "2")]);
        assert_eq!(apply(&mut store, &rival).unwrap(), TxnEffect::Nothing);
        assert_eq!(outcome(&store, &rival), "NO");

        // The rival took none of its locks.
        let free = prepare("t3", vec![set("other", "y")]);
        apply(&mut store, &free).unwrap();
        assert_eq!(outcome(&store, &free), "YES");

        // Finishing the loser must not disturb the winner's lock.
        apply(&mut store, &finish("t2", false)).unwrap();
        let still_locked = prepare("t4", vec![set("shared", "3")]);
        apply(&mut store, &still_locked).unwrap();
        assert_eq!(outcome(&store, &still_locked), "NO");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn replaying_any_step_twice_changes_nothing() {
        let (mut store, dir) = open_store("replay");

        let command = prepare("t1", vec![set("a", "1")]);
        apply(&mut store, &command).unwrap();
        assert_eq!(apply(&mut store, &command).unwrap(), TxnEffect::Nothing);

        apply(&mut store, &finish("t1", true)).unwrap();
        assert_eq!(
            apply(&mut store, &finish("t1", true)).unwrap(),
            TxnEffect::Nothing
        );

        // A plain write after the commit must not be overwritten by a replay.
        store.set("a".into(), "changed".into()).unwrap();
        apply(&mut store, &finish("t1", true)).unwrap();
        assert_eq!(store.get("a"), Some("changed".into()));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_prepare_that_arrives_after_the_transaction_ended_is_ignored() {
        let (mut store, dir) = open_store("late_prepare");

        // The transaction was aborted before this partition saw its Prepare.
        apply(&mut store, &finish("t1", false)).unwrap();

        let late = prepare("t1", vec![set("a", "1")]);
        apply(&mut store, &late).unwrap();

        assert_eq!(outcome(&store, &late), "NO");
        assert!(scan_pending(&store).unwrap().is_empty(), "no lock may leak");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_first_decision_wins() {
        let (mut store, dir) = open_store("decide");

        let abort = TxnCommand::Decide {
            id: "t1".into(),
            commit: false,
        };
        let commit = TxnCommand::Decide {
            id: "t1".into(),
            commit: true,
        };

        apply(&mut store, &abort).unwrap();
        apply(&mut store, &commit).unwrap();

        assert_eq!(
            outcome(&store, &commit),
            "ABORT",
            "a later COMMIT must lose"
        );
        assert_eq!(decision(&store, "t1"), Some("ABORT".into()));
        assert_eq!(decision(&store, "unknown"), None);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn prepared_transactions_survive_a_restart_and_are_found_again() {
        let (mut store, dir) = open_store("restart");

        apply(
            &mut store,
            &prepare("t1", vec![set("a", "1"), set("b", "2")]),
        )
        .unwrap();
        drop(store);

        let store = LsmStore::open(dir.join("sstables"), dir.join("wal.log")).expect("reopen");
        let pending = scan_pending(&store).unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, "t1");
        assert_eq!(pending[0].coordinator, 1);
        assert_eq!(pending[0].participants, vec![1, 2]);
        assert_eq!(pending[0].ops, vec![set("a", "1"), set("b", "2")]);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn reserved_keys_are_recognised() {
        assert!(is_reserved(&lock_key("a")));
        assert!(is_reserved(&vote_key("t1")));
        assert!(!is_reserved("user:1"));
    }
}
