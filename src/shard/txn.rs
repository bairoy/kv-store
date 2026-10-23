//! Atomic writes across partitions: two-phase commit where every step is a
//! Raft entry.
//!
//! ```text
//! TXN SET a 1 ; SET b 2 ; DELETE c          (keys may live in different partitions)
//!
//! group the writes by partition;  coordinator = the lowest participating partition
//!
//! phase 1  PREPARE  -> every participant partition (a Raft entry in each group):
//!                      lock the keys and vote YES, or vote NO if a key is
//!                      already locked by another transaction
//! decision DECIDE   -> the coordinator partition (a Raft entry): COMMIT if
//!                      everybody voted YES, else ABORT. The FIRST decision
//!                      recorded wins; this is the commit point.
//! phase 2  FINISH   -> every participant (a Raft entry): apply the writes
//!                      (or discard them) and release the locks
//! ```
//!
//! Classic two-phase commit blocks forever if the coordinator dies between
//! the phases, with participants holding locks. Here the coordinator is not a
//! process but a *replicated log*: the decision is durable and any leader can
//! read it. A participant stuck in "prepared" for longer than the timeout
//! asks the coordinator partition for the decision; if there is none it
//! writes ABORT there (first writer wins, so this cannot race with a slow
//! coordinator that is about to commit: one of the two decisions is simply
//! recorded first and the other learns of it).
//!
//! Limits: writes only (`SET`/`DELETE`, no reads inside a transaction), and
//! plain reads do not see locks, so a reader can briefly observe one
//! partition's part of a committed transaction before another's.

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::sleep;

use crate::net::protocol::{Command as ClientCommand, Response, encode_response, parse_request};
use crate::raft::txn::{self, ops_from_hex, ops_to_hex};
use crate::raft::types::{Role, TxnCommand, TxnOp};
use crate::shard::node::{ShardNode, decode_value, error_response};
use crate::shard::routing::PartitionId;

const STEP_ATTEMPTS: usize = 20;
const RETRY_PAUSE: Duration = Duration::from_millis(100);

/// `;` between writes, with spaces around it so values may contain `;`.
const SEPARATOR: &str = " ; ";

/// One step of the protocol, as sent to a partition's leader.
enum Step<'a> {
    Prepare {
        id: &'a str,
        coordinator: PartitionId,
        participants: &'a [PartitionId],
        ops: &'a [TxnOp],
    },
    Decide {
        id: &'a str,
        commit: bool,
    },
    Finish {
        id: &'a str,
        commit: bool,
    },
    Status {
        id: &'a str,
    },
}

fn verdict(commit: bool) -> &'static str {
    if commit { "COMMIT" } else { "ABORT" }
}

impl Step<'_> {
    /// The local admin command (`TXNOP`) that carries this step.
    fn command(&self, partition: PartitionId) -> Result<String, String> {
        Ok(match self {
            Step::Prepare {
                id,
                coordinator,
                participants,
                ops,
            } => {
                let participants = participants
                    .iter()
                    .map(|p| p.to_string())
                    .collect::<Vec<_>>()
                    .join(",");

                let ops = ops_to_hex(ops).map_err(|error| error.to_string())?;

                format!("TXNOP {partition} PREPARE {id} {coordinator} {participants} {ops}")
            }
            Step::Decide { id, commit } => {
                format!("TXNOP {partition} DECIDE {id} {}", verdict(*commit))
            }
            Step::Finish { id, commit } => {
                format!("TXNOP {partition} FINISH {id} {}", verdict(*commit))
            }
            Step::Status { id } => format!("TXNOP {partition} STATUS {id}"),
        })
    }
}

impl ShardNode {
    // ---------------------------------------------------------- coordinator

    /// `TXN <writes>`: run a transaction, answering `+OK` only if it committed.
    pub(crate) async fn transaction(&self, text: &str) -> String {
        match self.run_transaction(text).await {
            Ok(()) => encode_response(Response::Ok),
            Err(message) => error_response(message),
        }
    }

    async fn run_transaction(&self, text: &str) -> Result<(), String> {
        let writes = parse_writes(text)?;
        let table = self.table();

        let mut by_partition: BTreeMap<PartitionId, Vec<TxnOp>> = BTreeMap::new();
        for op in writes {
            by_partition
                .entry(table.partition_of(op.key()))
                .or_default()
                .push(op);
        }

        let participants: Vec<PartitionId> = by_partition.keys().copied().collect();
        let coordinator = participants[0];
        let id = self.next_txn_id();

        // ---- phase 1: prepare everywhere ----
        let mut abort_reason = None;

        for (partition, ops) in &by_partition {
            let step = Step::Prepare {
                id: &id,
                coordinator,
                participants: &participants,
                ops,
            };

            match self.txn_step(*partition, step).await {
                Ok(vote) if vote == "YES" => {}
                Ok(vote) => {
                    abort_reason = Some(format!(
                        "partition {partition} voted {vote}: a key is locked by another transaction"
                    ));
                    break;
                }
                Err(error) => {
                    abort_reason =
                        Some(format!("partition {partition} could not prepare: {error}"));
                    break;
                }
            }
        }

        // ---- decision: one Raft entry in the coordinator partition ----
        let wanted_commit = abort_reason.is_none();

        let decision = self
            .txn_step(
                coordinator,
                Step::Decide {
                    id: &id,
                    commit: wanted_commit,
                },
            )
            .await
            .map_err(|error| {
                format!(
                    "could not record the decision ({error}); the transaction will be \
                     resolved automatically within {:?}",
                    self.txn_timeout
                )
            })?;

        let committed = decision == "COMMIT";

        // ---- phase 2: finish everywhere ----
        // Sent to every participant, even ones never reached: a Finish that
        // finds nothing prepared just leaves a tombstone, which makes a late
        // Prepare harmless. Anyone we cannot reach is cleaned up by recovery.
        for partition in &participants {
            for _ in 0..3 {
                let step = Step::Finish {
                    id: &id,
                    commit: committed,
                };

                if self.txn_step(*partition, step).await.is_ok() {
                    break;
                }
            }
        }

        if committed {
            Ok(())
        } else {
            Err(format!(
                "transaction aborted: {}",
                abort_reason
                    .unwrap_or_else(|| "it was aborted by recovery before it decided".to_string())
            ))
        }
    }

    fn next_txn_id(&self) -> String {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis())
            .unwrap_or(0);

        format!(
            "t{}-{millis}-{}",
            self.id,
            self.txn_counter.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// Run one protocol step on whichever server leads `partition`.
    async fn txn_step(&self, partition: PartitionId, step: Step<'_>) -> Result<String, String> {
        let command = step.command(partition)?;
        let mut last_error = "no attempt made".to_string();

        for _ in 0..STEP_ATTEMPTS {
            // Re-read the table each time: a rebalance may have moved it.
            let candidates = self.table().replicas(partition).to_vec();
            let (leader, _) = self.find_leader(partition, &candidates).await?;

            match self.ask_server(leader, &command).await {
                Ok(response) => match decode_value(&response) {
                    Some(value) => return Ok(value),
                    None => last_error = response,
                },
                Err(error) => last_error = error,
            }

            sleep(RETRY_PAUSE).await;
        }

        Err(last_error)
    }

    // ------------------------------------------------- local admin (TXNOP)

    /// `TXNOP <partition> PREPARE|DECIDE|FINISH|STATUS ...`, leader only.
    pub(crate) async fn txn_operation(&self, rest: &str) -> String {
        match self.txn_operation_inner(rest).await {
            Ok(value) => encode_response(Response::Value(value)),
            Err(message) => error_response(message),
        }
    }

    async fn txn_operation_inner(&self, rest: &str) -> Result<String, String> {
        let mut parts = rest.split_whitespace();

        let partition: PartitionId = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or("usage: TXNOP <partition> <step> ...")?;

        let group = self
            .group(partition)
            .ok_or_else(|| format!("partition {partition} is not hosted here"))?;

        let step = parts.next().ok_or("missing step")?;
        let id = parts.next().ok_or("missing transaction id")?.to_string();

        let flag = |word: Option<&str>| match word {
            Some("COMMIT") => Ok(true),
            Some("ABORT") => Ok(false),
            _ => Err("expected COMMIT or ABORT".to_string()),
        };

        match step {
            "PREPARE" => {
                let coordinator = parts
                    .next()
                    .and_then(|part| part.parse().ok())
                    .ok_or("missing coordinator")?;

                let participants = parts
                    .next()
                    .ok_or("missing participants")?
                    .split(',')
                    .map(|part| part.parse())
                    .collect::<Result<Vec<PartitionId>, _>>()
                    .map_err(|_| "bad participants")?;

                let ops = ops_from_hex(parts.next().ok_or("missing writes")?)
                    .map_err(|error| error.to_string())?;

                group
                    .txn_command(TxnCommand::Prepare {
                        id,
                        coordinator,
                        participants,
                        ops,
                    })
                    .await
            }
            "DECIDE" => {
                let commit = flag(parts.next())?;
                group.txn_command(TxnCommand::Decide { id, commit }).await
            }
            "FINISH" => {
                let commit = flag(parts.next())?;
                group.txn_command(TxnCommand::Finish { id, commit }).await
            }
            "STATUS" => Ok(group
                .txn_decision(&id)
                .await?
                .unwrap_or_else(|| "NONE".to_string())),
            other => Err(format!("unknown transaction step {other}")),
        }
    }

    // -------------------------------------------------------------- recovery

    /// Forever: find transactions stuck in "prepared" on partitions we lead
    /// and resolve them from the coordinator partition's decision.
    pub(crate) async fn run_txn_recovery(self: std::sync::Arc<Self>) {
        let pause = (self.txn_timeout / 4).max(Duration::from_millis(100));

        loop {
            sleep(pause).await;

            for partition in self.hosted_partitions() {
                let Some(group) = self.group(partition) else {
                    continue;
                };

                // Only the leader acts, so replicas do not all do the work.
                if group.leader_status().await.0 != Role::Leader {
                    continue;
                }

                for (pending, age) in group.pending_transactions() {
                    if age >= self.txn_timeout {
                        self.resolve_stuck(partition, &pending).await;
                    }
                }
            }
        }
    }

    /// Learn the verdict from the coordinator partition (forcing ABORT if it
    /// has none) and finish the transaction here.
    async fn resolve_stuck(&self, partition: PartitionId, pending: &txn::PendingTxn) {
        let id = pending.id.as_str();

        let known = match self
            .txn_step(pending.coordinator, Step::Status { id })
            .await
        {
            Ok(decision) => decision,
            Err(_) => return, // coordinator partition unreachable: try again later
        };

        let decision = if known == "NONE" {
            // Nobody decided. Record ABORT; if a slow coordinator decided in
            // the meantime, that decision wins and is what we get back.
            match self
                .txn_step(pending.coordinator, Step::Decide { id, commit: false })
                .await
            {
                Ok(decision) => decision,
                Err(_) => return,
            }
        } else {
            known
        };

        let commit = decision == "COMMIT";

        if self
            .txn_step(partition, Step::Finish { id, commit })
            .await
            .is_ok()
        {
            println!(
                "Shard {}: resolved stuck transaction {id} on partition {partition}: {decision}",
                self.id
            );
        }
    }
}

/// `SET a 1 ; DELETE b ; ...` into writes. Rejects reads, reserved keys and
/// the same key twice (its meaning would be ambiguous).
fn parse_writes(text: &str) -> Result<Vec<TxnOp>, String> {
    let mut writes = Vec::new();

    for part in text.split(SEPARATOR) {
        let op = match parse_request(part.trim()) {
            Some(ClientCommand::Set(key, value)) => TxnOp::Set { key, value },
            Some(ClientCommand::Delete(key)) => TxnOp::Delete { key },
            Some(ClientCommand::Get(_)) => {
                return Err("a transaction may only SET and DELETE".to_string());
            }
            None => return Err(format!("invalid transaction write {part:?}")),
        };

        if txn::is_reserved(op.key()) {
            return Err("that key is reserved for transactions".to_string());
        }

        if writes.iter().any(|other: &TxnOp| other.key() == op.key()) {
            return Err(format!(
                "key {:?} appears twice in the transaction",
                op.key()
            ));
        }

        writes.push(op);
    }

    if writes.is_empty() {
        return Err("empty transaction".to_string());
    }

    Ok(writes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_are_parsed_and_validated() {
        let writes = parse_writes("SET a 1 ; DELETE b ; SET c hello world").unwrap();

        assert_eq!(
            writes,
            vec![
                TxnOp::Set {
                    key: "a".into(),
                    value: "1".into()
                },
                TxnOp::Delete { key: "b".into() },
                TxnOp::Set {
                    key: "c".into(),
                    value: "hello world".into()
                },
            ]
        );

        // A value may contain a bare `;`.
        assert_eq!(
            parse_writes("SET a x;y").unwrap(),
            vec![TxnOp::Set {
                key: "a".into(),
                value: "x;y".into()
            }]
        );

        assert!(parse_writes("GET a").is_err(), "reads are not allowed");
        assert!(parse_writes("SET a 1 ; SET a 2").is_err(), "duplicate key");
        assert!(parse_writes("SET a 1 ; NONSENSE").is_err());
        assert!(parse_writes("").is_err());
        assert!(
            parse_writes("SET \u{1}txn:lock:x 1").is_err(),
            "reserved key"
        );
    }
}
