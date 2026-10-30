//! Moving partitions between servers while the cluster keeps serving traffic.

use std::time::{Duration, Instant};

use tokio::time::sleep;

use crate::net::protocol::{Response, encode_response};
use crate::raft::types::NodeId;
use crate::shard::node::{ShardNode, decode_value, error_response};
use crate::shard::ring::ConsistentHashRing;
use crate::shard::routing::{PartitionId, PartitionMove, RoutingTable};

const LEADER_WAIT: Duration = Duration::from_secs(30);
const CATCH_UP_WAIT: Duration = Duration::from_secs(120);
const CATCH_UP_LAG: u64 = 2;
const RETRY_PAUSE: Duration = Duration::from_millis(100);
const MAX_ATTEMPTS: usize = 60;

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

#[derive(Debug, Clone, Copy)]
enum Goal {
    Learner,
    Voter,
    Gone,
}

impl ShardNode {
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

        tracing::info!(
            "Rebalance: {} of {} partitions change replicas",
            moves.len(),
            current.partition_count()
        );

        let mut table = (*current).clone();

        for change in &moves {
            tracing::info!(
                "Rebalance: partition {} adds {:?}, removes {:?}",
                change.partition,
                change.add,
                change.remove
            );

            self.move_partition(&table, change).await?;

            table = table.with_partition(
                change.partition,
                ideal.replicas(change.partition).to_vec(),
                table.epoch + 1,
            );

            self.publish_table(&table).await;

            sleep(self.rebalance_pause).await;
        }

        Ok(format!(
            "moved {} partition(s) onto servers {target:?}; routing table is now at epoch {}",
            moves.len(),
            table.epoch
        ))
    }

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

            let _ = self
                .ask_server(leaving, &format!("UNHOST {partition}"))
                .await;
        }

        Ok(())
    }

    async fn publish_table(&self, table: &RoutingTable) {
        if let Err(error) = self.adopt_table(table.clone()) {
            tracing::warn!("Rebalance: could not save the routing table locally: {error}");
        }

        let command = format!("SETTABLE {}", table.encode());

        for server in self.servers.keys().filter(|server| **server != self.id) {
            if let Err(error) = self.ask_server(*server, &command).await {
                tracing::warn!(
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
