//! The two-level mapping from a key to the servers that store it.

use std::collections::{BTreeSet, HashMap};

use crate::raft::types::NodeId;
use crate::shard::hash::stable_hash;
use crate::shard::ring::ConsistentHashRing;

pub type PartitionId = u32;

pub fn partition_for(key: &str, partition_count: u32) -> PartitionId {
    (stable_hash(key) % u64::from(partition_count.max(1))) as PartitionId
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionInfo {
    pub id: PartitionId,
    pub replicas: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMove {
    pub partition: PartitionId,
    pub add: Vec<NodeId>,
    pub remove: Vec<NodeId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingTable {
    pub epoch: u64,
    partitions: Vec<PartitionInfo>,
}

impl RoutingTable {
    pub fn build(
        ring: &ConsistentHashRing,
        partition_count: u32,
        replication: usize,
        epoch: u64,
    ) -> Result<Self, String> {
        if partition_count == 0 {
            return Err("partition count must be at least 1".to_string());
        }

        if replication == 0 {
            return Err("replication factor must be at least 1".to_string());
        }

        if ring.nodes().len() < replication {
            return Err(format!(
                "replication factor {replication} needs at least {replication} servers, \
                 but the ring has {}",
                ring.nodes().len()
            ));
        }

        let partitions = (0..partition_count)
            .map(|id| PartitionInfo {
                id,
                replicas: ring.replicas_for(&format!("partition-{id}"), replication),
            })
            .collect();

        Ok(Self { epoch, partitions })
    }

    pub fn with_partition(
        &self,
        partition: PartitionId,
        replicas: Vec<NodeId>,
        epoch: u64,
    ) -> Self {
        let mut next = self.clone();

        if let Some(info) = next.partitions.get_mut(partition as usize) {
            info.replicas = replicas;
        }

        next.epoch = epoch;
        next
    }

    pub fn encode(&self) -> String {
        let mut parts = vec![format!("epoch={}", self.epoch)];

        for info in &self.partitions {
            let replicas: Vec<String> = info.replicas.iter().map(|id| id.to_string()).collect();
            parts.push(format!("{}:{}", info.id, replicas.join(",")));
        }

        parts.join(" ")
    }

    pub fn decode(text: &str) -> Result<Self, String> {
        let mut parts = text.split_whitespace();

        let epoch = parts
            .next()
            .and_then(|part| part.strip_prefix("epoch="))
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or("routing table must start with epoch=<number>")?;

        let mut partitions = Vec::new();

        for (expected, part) in parts.enumerate() {
            let (id, replicas) = part
                .split_once(':')
                .ok_or_else(|| format!("bad partition entry {part:?}"))?;

            let id: PartitionId = id
                .parse()
                .map_err(|_| format!("bad partition id in {part:?}"))?;

            if id as usize != expected {
                return Err(format!("partitions must be listed in order; got {id}"));
            }

            let replicas: Vec<NodeId> = replicas
                .split(',')
                .map(|replica| replica.parse::<NodeId>())
                .collect::<Result<_, _>>()
                .map_err(|_| format!("bad replica list in {part:?}"))?;

            if replicas.is_empty() {
                return Err(format!("partition {id} has no replicas"));
            }

            partitions.push(PartitionInfo { id, replicas });
        }

        if partitions.is_empty() {
            return Err("routing table has no partitions".to_string());
        }

        Ok(Self { epoch, partitions })
    }

    pub fn partition_count(&self) -> u32 {
        self.partitions.len() as u32
    }

    pub fn partition_of(&self, key: &str) -> PartitionId {
        partition_for(key, self.partition_count())
    }

    pub fn info(&self, partition: PartitionId) -> Option<&PartitionInfo> {
        self.partitions.get(partition as usize)
    }

    pub fn replicas(&self, partition: PartitionId) -> &[NodeId] {
        self.info(partition)
            .map(|info| info.replicas.as_slice())
            .unwrap_or(&[])
    }

    pub fn partitions(&self) -> &[PartitionInfo] {
        &self.partitions
    }

    pub fn hosted_by(&self, node: NodeId) -> Vec<PartitionId> {
        self.partitions
            .iter()
            .filter(|info| info.replicas.contains(&node))
            .map(|info| info.id)
            .collect()
    }

    pub fn diff(&self, newer: &RoutingTable) -> Result<Vec<PartitionMove>, String> {
        if self.partition_count() != newer.partition_count() {
            return Err("the partition count must never change".to_string());
        }

        let mut moves = Vec::new();

        for (old, new) in self.partitions.iter().zip(&newer.partitions) {
            let old_set: BTreeSet<_> = old.replicas.iter().copied().collect();
            let new_set: BTreeSet<_> = new.replicas.iter().copied().collect();

            if old_set != new_set {
                moves.push(PartitionMove {
                    partition: old.id,
                    add: new_set.difference(&old_set).copied().collect(),
                    remove: old_set.difference(&new_set).copied().collect(),
                });
            }
        }

        Ok(moves)
    }

    pub fn load(&self) -> HashMap<NodeId, usize> {
        let mut load = HashMap::new();

        for info in &self.partitions {
            for replica in &info.replicas {
                *load.entry(*replica).or_insert(0) += 1;
            }
        }

        load
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(nodes: &[NodeId]) -> ConsistentHashRing {
        let mut ring = ConsistentHashRing::new(200);
        for node in nodes {
            ring.add_node(*node);
        }
        ring
    }

    #[test]
    fn a_key_always_lands_in_the_same_in_range_partition() {
        for i in 0..1000 {
            let key = format!("user:{i}");
            let partition = partition_for(&key, 256);

            assert!(partition < 256);
            assert_eq!(partition, partition_for(&key, 256));
        }
    }

    #[test]
    fn adding_a_server_never_changes_which_partition_a_key_belongs_to() {
        let before = RoutingTable::build(&ring(&[1, 2, 3, 4]), 64, 3, 1).unwrap();
        let after = RoutingTable::build(&ring(&[1, 2, 3, 4, 5]), 64, 3, 2).unwrap();

        for i in 0..2000 {
            let key = format!("user:{i}");
            assert_eq!(before.partition_of(&key), after.partition_of(&key));
        }
    }

    #[test]
    fn every_partition_gets_distinct_replicas_and_load_is_spread() {
        let table = RoutingTable::build(&ring(&[1, 2, 3, 4]), 64, 3, 1).unwrap();

        for info in table.partitions() {
            let mut replicas = info.replicas.clone();
            replicas.sort_unstable();
            replicas.dedup();
            assert_eq!(replicas.len(), 3);
        }

        let load = table.load();
        for server in 1..=4 {
            let hosted = load[&server];
            assert!(
                (30..=62).contains(&hosted),
                "server {server} hosts {hosted}"
            );
        }

        assert_eq!(table.hosted_by(1).len(), load[&1]);
    }

    #[test]
    fn building_validates_its_inputs() {
        let small = ring(&[1, 2]);

        assert!(
            RoutingTable::build(&small, 8, 3, 1).is_err(),
            "RF 3 needs 3 servers"
        );
        assert!(RoutingTable::build(&small, 0, 2, 1).is_err());
        assert!(RoutingTable::build(&small, 8, 0, 1).is_err());
        assert!(RoutingTable::build(&ConsistentHashRing::new(10), 8, 1, 1).is_err());
    }

    #[test]
    fn adding_a_server_produces_a_minimal_rebalance_plan() {
        let old = RoutingTable::build(&ring(&[1, 2, 3, 4]), 256, 3, 1).unwrap();
        let new = RoutingTable::build(&ring(&[1, 2, 3, 4, 5]), 256, 3, 2).unwrap();

        let moves = old.diff(&new).unwrap();
        assert!(!moves.is_empty());

        for change in &moves {
            assert_eq!(change.add, vec![5], "only the new server may be added");
            assert_eq!(change.remove.len(), 1, "one old replica steps aside");
            assert!(old.replicas(change.partition).contains(&change.remove[0]));
        }

        let moved_slots = moves.len();
        let total_slots = 256 * 3;
        let fraction = moved_slots as f64 / total_slots as f64;
        assert!(
            (0.10..0.32).contains(&fraction),
            "expected about 20% of replica slots to move, got {:.1}%",
            fraction * 100.0
        );

        let touched: BTreeSet<_> = moves.iter().map(|m| m.partition).collect();
        for info in old.partitions() {
            if !touched.contains(&info.id) {
                let mut a = info.replicas.clone();
                let mut b = new.replicas(info.id).to_vec();
                a.sort_unstable();
                b.sort_unstable();
                assert_eq!(a, b);
            }
        }
    }

    #[test]
    fn tables_round_trip_through_text_and_reject_garbage() {
        let table = RoutingTable::build(&ring(&[1, 2, 3, 4]), 8, 3, 7).unwrap();

        assert_eq!(RoutingTable::decode(&table.encode()).unwrap(), table);

        let moved = table.with_partition(3, vec![4, 1, 2], 8);
        assert_eq!(moved.epoch, 8);
        assert_eq!(moved.replicas(3), &[4, 1, 2]);
        assert_eq!(
            moved.replicas(2),
            table.replicas(2),
            "other partitions untouched"
        );

        assert!(RoutingTable::decode("").is_err());
        assert!(RoutingTable::decode("epoch=x 0:1").is_err());
        assert!(RoutingTable::decode("epoch=1").is_err(), "no partitions");
        assert!(
            RoutingTable::decode("epoch=1 1:1,2").is_err(),
            "must start at 0"
        );
        assert!(
            RoutingTable::decode("epoch=1 0:").is_err(),
            "empty replica list"
        );
        assert!(RoutingTable::decode("epoch=1 0:1,a").is_err());
    }

    #[test]
    fn diff_refuses_a_changed_partition_count() {
        let a = RoutingTable::build(&ring(&[1, 2, 3]), 8, 3, 1).unwrap();
        let b = RoutingTable::build(&ring(&[1, 2, 3]), 16, 3, 2).unwrap();

        assert!(a.diff(&b).is_err());
    }

    #[test]
    fn identical_tables_have_an_empty_plan() {
        let a = RoutingTable::build(&ring(&[1, 2, 3, 4]), 32, 3, 1).unwrap();
        let b = RoutingTable::build(&ring(&[1, 2, 3, 4]), 32, 3, 2).unwrap();

        assert!(a.diff(&b).unwrap().is_empty());
    }
}
