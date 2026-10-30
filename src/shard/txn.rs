//! Atomic writes across partitions (two-phase commit over Raft).

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::time::sleep;

use crate::metrics;
use crate::net::protocol::{Command as ClientCommand, Response, encode_response, parse_request};
use crate::net::request::Session;
use crate::raft::txn::{self, ops_from_hex, ops_to_hex};
use crate::raft::types::{Role, TxnCommand, TxnOp};
use crate::shard::node::{ShardNode, decode_value, error_response};
use crate::shard::routing::PartitionId;

const STEP_ATTEMPTS: usize = 20;
const RETRY_PAUSE: Duration = Duration::from_millis(100);

const SEPARATOR: &str = " ; ";

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
    pub(crate) async fn transaction(&self, text: &str, session: Option<&Session>) -> String {
        match self.run_transaction(text, session).await {
            Ok(()) => {
                metrics::inc("txn_total", &[("outcome", "committed")]);
                encode_response(Response::Ok)
            }
            Err(message) => {
                metrics::inc("txn_total", &[("outcome", "failed")]);
                error_response(message)
            }
        }
    }

    async fn run_transaction(&self, text: &str, session: Option<&Session>) -> Result<(), String> {
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
        let id = match session {
            Some(session) => session.txn_id(),
            None => self.next_txn_id(),
        };

        if session.is_some() {
            match self.txn_step(coordinator, Step::Status { id: &id }).await {
                Ok(verdict) if verdict == "COMMIT" => {
                    metrics::inc("txn_retries_answered_total", &[]);
                    return Ok(());
                }
                Ok(verdict) if verdict == "ABORT" => {
                    return Err(
                        "transaction aborted by an earlier attempt of this request; \
                         retry with a new request number"
                            .to_string(),
                    );
                }
                _ => {}
            }
        }

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

    async fn txn_step(&self, partition: PartitionId, step: Step<'_>) -> Result<String, String> {
        let command = step.command(partition)?;
        let mut last_error = "no attempt made".to_string();

        for _ in 0..STEP_ATTEMPTS {
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

    pub(crate) async fn run_txn_recovery(self: std::sync::Arc<Self>) {
        let pause = (self.txn_timeout / 4).max(Duration::from_millis(100));

        loop {
            sleep(pause).await;

            for partition in self.hosted_partitions() {
                let Some(group) = self.group(partition) else {
                    continue;
                };

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

    async fn resolve_stuck(&self, partition: PartitionId, pending: &txn::PendingTxn) {
        let id = pending.id.as_str();

        let known = match self
            .txn_step(pending.coordinator, Step::Status { id })
            .await
        {
            Ok(decision) => decision,
            Err(_) => return,
        };

        let decision = if known == "NONE" {
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
            tracing::info!(
                "Shard {}: resolved stuck transaction {id} on partition {partition}: {decision}",
                self.id
            );
        }
    }
}

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
