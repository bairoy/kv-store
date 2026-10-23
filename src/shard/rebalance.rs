//! Moving partitions between servers while the cluster keeps serving traffic.
//!
//! "Assignment is not data movement": changing the routing table does not
//! move any data. A partition's data only moves when its Raft group changes
//! *members*, and that is done with the single-server membership changes from
//! `raft::node`. For every partition whose replica set must change, the
//! coordinator (whichever server received `REBALANCE`) does this, in order:
//!
//! ```text
//! for each server joining the partition:
//!     1. HOST p            the server starts an empty Raft group for p
//!     2. ADD_LEARNER       the leader starts sending it the log / a snapshot
//!     3. wait              until the learner has (almost) caught up
//!     4. PROMOTE           the learner becomes a voter
//! for each server leaving the partition:
//!     5. REMOVE            it leaves the group (committed by Raft)
//!     6. UNHOST p          it stops its group and sets the data aside
//! 7. publish the routing table with this partition updated and epoch + 1
//! ```
//!
//! The table is flipped only after step 6, one partition at a time, with a
//! pause in between so the data transfer does not starve live traffic. Every
//! step first looks at the group's current state, so if the coordinator dies
//! half way, running the same `REBALANCE` again simply finishes the job.

use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::net::protocol::{Response, encode_response};
use crate::raft::types::NodeId;
use crate::shard::node::{ShardNode, decode_value, error_response};
use crate::shard::ring::ConsistentHashRing;
use crate::shard::routing::{PartitionId, PartitionMove, RoutingTable};

const LEADER_WAIT: Duration = Duration::from_secs(30);
const CATCH_UP_WAIT: Duration = Duration::from_secs(120);
/// A learner may be promoted once it lacks at most this many committed entries.
const CATCH_UP_LAG: u64 = 2;
const RETRY_PAUSE: Duration = Duration::from_millis(100);
const MAX_ATTEMPTS: usize = 60;

/// What a server reported for one partition (`GROUPINFO`).
#[derive(Debug)]
pub(crate) struct GroupState {
    is_leader: bool,
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
}

impl GroupState {
    fn parse(text: &str) -> Option<Self> {
        if text == "not-hosted" {
            return None;
        }

        let field = |name: &str| {
            text.split_whitespace()
                .find_map(|part| part.strip_prefix(&format!("{name}=")))
        };

        let list = |name: &str| -> Option<Vec<NodeId>> {
            match field(name)? {
                "-" => Some(Vec::new()),
                ids => ids.split(',').map(|id| id.parse().ok()).collect(),
            }
        };

        Some(Self {
            is_leader: field("role")? == "Leader",
            voters: list("voters")?,
            learners: list("learners")?,
        })
    }

    fn is_member(&self, server: NodeId) -> bool {
        self.voters.contains(&server) || self.learners.contains(&server)
    }
}

/// What a membership step is trying to achieve.
#[derive(Debug, Clone, Copy)]
enum Goal {
    Learner,
    Voter,
    Gone,
}

impl ShardNode {
    /// `REBALANCE <ids>`: make exactly these servers hold the data.
    pub(crate) async fn rebalance(&self, servers: &str) -> String {
        match self.rebalance_to(servers).await {
            Ok(message) => encode_response(Response::Value(message)),
            Err(message) => error_response(message),
        }
    }

    async fn rebalance_to(&self, servers: &str) -> Result<String, String> {
        let target: Vec<NodeId> = servers
            .split(',')
            .map(|server| server.trim().parse())
            .collect::<Result<_, _>>()
            .map_err(|_| "usage: REBALANCE <comma-separated server ids>".to_string())?;

        for server in &target {
            if !self.servers.contains_key(server) {
                return Err(format!("server {server} is not in the server list"));
            }
        }

        let _running = self
            .rebalance_lock
            .try_lock()
            .map_err(|_| "a rebalance is already running".to_string())?;

        let mut ring = ConsistentHashRing::new(self.vnodes);
        for server in &target {
            ring.add_node(*server);
        }

        let current = self.table();
        let replication = current.replicas(0).len();

        let ideal = RoutingTable::build(&ring, current.partition_count(), replication, 0)?;
        let moves = current.diff(&ideal)?;

        if moves.is_empty() {
            return Ok(format!(
                "already balanced for servers {target:?}; routing table stays at epoch {}",
                current.epoch
            ));
        }

        println!(
            "Rebalance: {} of {} partitions change replicas",
            moves.len(),
            current.partition_count()
        );

        let mut table = (*current).clone();

        for change in &moves {
            println!(
                "Rebalance: partition {} adds {:?}, removes {:?}",
                change.partition, change.add, change.remove
            );

            self.move_partition(&table, change).await?;

            // The data is in place: now (and only now) flip the routing.
            table = table.with_partition(
                change.partition,
                ideal.replicas(change.partition).to_vec(),
                table.epoch + 1,
            );

            self.publish_table(&table).await;

            // Throttle: leave room for foreground traffic.
            sleep(self.rebalance_pause).await;
        }

        Ok(format!(
            "moved {} partition(s) onto servers {target:?}; routing table is now at epoch {}",
            moves.len(),
            table.epoch
        ))
    }

    /// Steps 1-6 for one partition.
    async fn move_partition(
        &self,
        table: &RoutingTable,
        change: &PartitionMove,
    ) -> Result<(), String> {
        let partition = change.partition;

        let mut candidates = table.replicas(partition).to_vec();
        for server in change.add.iter().chain(&change.remove) {
            if !candidates.contains(server) {
                candidates.push(*server);
            }
        }

        for &joining in &change.add {
            self.expect_ok(joining, &format!("HOST {partition}"))
                .await?;

            self.ensure(partition, &candidates, joining, Goal::Learner)
                .await?;
            self.wait_until_caught_up(partition, &candidates, joining)
                .await?;
            self.ensure(partition, &candidates, joining, Goal::Voter)
                .await?;
        }

        for &leaving in &change.remove {
            self.ensure(partition, &candidates, leaving, Goal::Gone)
                .await?;

            // Best effort: the server may be down, in which case it simply
            // keeps an idle group that nobody talks to.
            let _ = self
                .ask_server(leaving, &format!("UNHOST {partition}"))
                .await;
        }

        Ok(())
    }

    /// Send the new table to every server (this one included).
    async fn publish_table(&self, table: &RoutingTable) {
        if let Err(error) = self.adopt_table(table.clone()) {
            eprintln!("Rebalance: could not save the routing table locally: {error}");
        }

        let command = format!("SETTABLE {}", table.encode());

        for server in self.servers.keys().filter(|server| **server != self.id) {
            // A server that is down misses this; it catches up from a peer
            // the next time they talk (see `sync_table_with`).
            if let Err(error) = self.ask_server(*server, &command).await {
                eprintln!(
                    "Rebalance: could not tell server {server} about epoch {}: {error}",
                    table.epoch
                );
            }
        }
    }

    async fn expect_ok(&self, server: NodeId, command: &str) -> Result<(), String> {
        let response = self.ask_server(server, command).await?;

        if response == "+OK" {
            Ok(())
        } else {
            Err(format!(
                "server {server} answered {command:?} with {response:?}"
            ))
        }
    }

    /// Find whichever candidate currently leads the partition.
    pub(crate) async fn find_leader(
        &self,
        partition: PartitionId,
        candidates: &[NodeId],
    ) -> Result<(NodeId, GroupState), String> {
        let deadline = Instant::now() + LEADER_WAIT;

        loop {
            for &candidate in candidates {
                if let Ok(response) = self
                    .ask_server(candidate, &format!("GROUPINFO {partition}"))
                    .await
                    && let Some(text) = decode_value(&response)
                    && let Some(state) = GroupState::parse(&text)
                    && state.is_leader
                {
                    return Ok((candidate, state));
                }
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "partition {partition}: no leader found among servers {candidates:?}"
                ));
            }

            sleep(RETRY_PAUSE).await;
        }
    }

    /// Repeat one membership change until the group reflects it. Leaders come
    /// and go and a change may be refused (another in flight, no committed
    /// entry in this term yet), so each attempt re-reads the real state.
    async fn ensure(
        &self,
        partition: PartitionId,
        candidates: &[NodeId],
        server: NodeId,
        goal: Goal,
    ) -> Result<(), String> {
        for _ in 0..MAX_ATTEMPTS {
            let (leader, state) = self.find_leader(partition, candidates).await?;

            let satisfied = match goal {
                Goal::Learner => state.is_member(server),
                Goal::Voter => state.voters.contains(&server),
                Goal::Gone => !state.is_member(server),
            };

            if satisfied {
                return Ok(());
            }

            let operation = match goal {
                Goal::Learner => "ADD_LEARNER",
                Goal::Voter => "PROMOTE",
                Goal::Gone => "REMOVE",
            };

            let command = format!("GROUPOP {partition} {operation} {server}");

            // The leader answers +OK only once the change is committed.
            if let Ok(response) = self.ask_server(leader, &command).await
                && response == "+OK"
            {
                return Ok(());
            }

            sleep(RETRY_PAUSE).await;
        }

        Err(format!(
            "partition {partition}: could not {goal:?} server {server} after {MAX_ATTEMPTS} attempts"
        ))
    }

    /// Wait until the learner lacks at most `CATCH_UP_LAG` committed entries.
    async fn wait_until_caught_up(
        &self,
        partition: PartitionId,
        candidates: &[NodeId],
        server: NodeId,
    ) -> Result<(), String> {
        let deadline = Instant::now() + CATCH_UP_WAIT;

        loop {
            let (leader, _) = self.find_leader(partition, candidates).await?;

            if let Ok(response) = self
                .ask_server(leader, &format!("GROUPOP {partition} LAG {server}"))
                .await
                && let Some(lag) = decode_value(&response).and_then(|text| text.parse::<u64>().ok())
                && lag <= CATCH_UP_LAG
            {
                return Ok(());
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "partition {partition}: server {server} did not catch up in time"
                ));
            }

            sleep(RETRY_PAUSE).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_info_lines_are_parsed() {
        let state =
            GroupState::parse("role=Leader leader=2 term=4 commit=10 voters=1,2,3 learners=4")
                .expect("parse");

        assert!(state.is_leader);
        assert_eq!(state.voters, vec![1, 2, 3]);
        assert_eq!(state.learners, vec![4]);
        assert!(state.is_member(4));
        assert!(!state.is_member(9));

        let follower =
            GroupState::parse("role=Follower leader=- term=1 commit=0 voters=1 learners=-")
                .expect("parse");
        assert!(!follower.is_leader);
        assert!(follower.learners.is_empty());

        assert!(GroupState::parse("not-hosted").is_none());
        assert!(GroupState::parse("garbage").is_none());
        assert!(GroupState::parse("role=Leader voters=x learners=-").is_none());
    }
}
