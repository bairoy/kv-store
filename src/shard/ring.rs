//! Consistent hashing with virtual nodes.

use std::collections::{BTreeMap, BTreeSet};

use crate::raft::types::NodeId;
use crate::shard::hash::stable_hash;

#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    ring: BTreeMap<u64, NodeId>,
    vnodes: u32,
}

impl ConsistentHashRing {
    pub fn new(vnodes: u32) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes: vnodes.max(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn nodes(&self) -> Vec<NodeId> {
        self.ring
            .values()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn add_node(&mut self, node: NodeId) {
        for vnode in 0..self.vnodes {
            let mut salt = 0u32;

            loop {
                let name = if salt == 0 {
                    format!("node-{node}-vnode-{vnode}")
                } else {
                    format!("node-{node}-vnode-{vnode}-salt-{salt}")
                };

                let position = stable_hash(&name);

                match self.ring.get(&position) {
                    None => {
                        self.ring.insert(position, node);
                        break;
                    }
                    Some(existing) if *existing == node => break,
                    Some(_) => salt += 1,
                }
            }
        }
    }

    pub fn remove_node(&mut self, node: NodeId) {
        self.ring.retain(|_, owner| *owner != node);
    }

    pub fn owner(&self, key: &str) -> Option<NodeId> {
        let hash = stable_hash(key);

        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, node)| *node)
    }

    pub fn replicas_for(&self, key: &str, count: usize) -> Vec<NodeId> {
        let hash = stable_hash(key);
        let mut found = Vec::new();

        let clockwise = self.ring.range(hash..).chain(self.ring.range(..hash));

        for (_, node) in clockwise {
            if !found.contains(node) {
                found.push(*node);

                if found.len() == count {
                    break;
                }
            }
        }

        found
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn ring_with(nodes: &[NodeId], vnodes: u32) -> ConsistentHashRing {
        let mut ring = ConsistentHashRing::new(vnodes);
        for node in nodes {
            ring.add_node(*node);
        }
        ring
    }

    fn keys(count: usize) -> Vec<String> {
        (0..count).map(|i| format!("user:{i}")).collect()
    }

    #[test]
    fn empty_ring_has_no_owner() {
        let ring = ConsistentHashRing::new(10);

        assert!(ring.is_empty());
        assert_eq!(ring.owner("anything"), None);
        assert!(ring.replicas_for("anything", 3).is_empty());
    }

    #[test]
    fn lookup_walks_clockwise_and_wraps_around() {
        let mut ring = ConsistentHashRing::new(1);
        ring.ring.insert(20, 1);
        ring.ring.insert(50, 2);
        ring.ring.insert(80, 3);

        let owner_at = |position: u64| {
            ring.ring
                .range(position..)
                .next()
                .or_else(|| ring.ring.iter().next())
                .map(|(_, node)| *node)
        };

        assert_eq!(owner_at(10), Some(1));
        assert_eq!(owner_at(35), Some(2));
        assert_eq!(owner_at(50), Some(2), "exactly on a node belongs to it");
        assert_eq!(owner_at(70), Some(3));
        assert_eq!(
            owner_at(90),
            Some(1),
            "past the last node wraps to the first"
        );
    }

    #[test]
    fn replicas_are_distinct_ordered_and_start_with_the_owner() {
        let ring = ring_with(&[1, 2, 3, 4], 100);

        for key in keys(200) {
            let replicas = ring.replicas_for(&key, 3);

            assert_eq!(replicas.len(), 3);
            assert_eq!(Some(replicas[0]), ring.owner(&key));

            let mut sorted = replicas.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), 3, "replicas must be distinct servers");
        }

        assert_eq!(ring.replicas_for("k", 10).len(), 4);
    }

    #[test]
    fn adding_the_same_node_twice_changes_nothing() {
        let mut ring = ring_with(&[1, 2], 50);
        let before: Vec<_> = keys(500).iter().map(|k| ring.owner(k)).collect();

        ring.add_node(2);

        let after: Vec<_> = keys(500).iter().map(|k| ring.owner(k)).collect();
        assert_eq!(before, after);
    }

    #[test]
    fn adding_a_server_moves_about_one_nth_of_the_keys_and_only_to_it() {
        let keys = keys(50_000);
        let mut ring = ring_with(&[0, 1, 2, 3], 200);
        let before: Vec<_> = keys.iter().map(|k| ring.owner(k).unwrap()).collect();

        ring.add_node(4);

        let mut moved = 0;
        for (key, old_owner) in keys.iter().zip(&before) {
            let new_owner = ring.owner(key).unwrap();

            if new_owner != *old_owner {
                moved += 1;
                assert_eq!(new_owner, 4, "a key moved between two OLD servers");
            }
        }

        let fraction = moved as f64 / keys.len() as f64;
        assert!(
            (0.12..0.30).contains(&fraction),
            "expected about 20% to move, got {:.1}%",
            fraction * 100.0
        );

        let modulo_moved = keys
            .iter()
            .filter(|k| stable_hash(k) % 4 != stable_hash(k) % 5)
            .count();
        assert!(modulo_moved as f64 / keys.len() as f64 > 0.7);
    }

    #[test]
    fn removing_a_server_only_moves_the_keys_it_owned() {
        let keys = keys(20_000);
        let mut ring = ring_with(&[0, 1, 2, 3, 4], 200);
        let before: Vec<_> = keys.iter().map(|k| ring.owner(k).unwrap()).collect();

        ring.remove_node(2);

        for (key, old_owner) in keys.iter().zip(&before) {
            let new_owner = ring.owner(key).unwrap();

            if *old_owner == 2 {
                assert_ne!(new_owner, 2);
            } else {
                assert_eq!(new_owner, *old_owner, "an unrelated key moved");
            }
        }
    }

    fn busiest_over_average(ring: &ConsistentHashRing, keys: &[String]) -> f64 {
        let mut load: HashMap<NodeId, usize> = HashMap::new();
        for key in keys {
            *load.entry(ring.owner(key).unwrap()).or_default() += 1;
        }

        let average = keys.len() as f64 / ring.nodes().len() as f64;
        *load.values().max().unwrap() as f64 / average
    }

    #[test]
    fn virtual_nodes_balance_the_load() {
        let keys = keys(50_000);

        let lumpy = busiest_over_average(&ring_with(&[0, 1, 2, 3], 1), &keys);
        let smooth = busiest_over_average(&ring_with(&[0, 1, 2, 3], 200), &keys);

        assert!(
            smooth < 1.25,
            "200 vnodes should be near-even, got {smooth:.2}x"
        );
        assert!(lumpy > smooth, "one position per server should be lumpier");
    }

    #[test]
    fn skewed_key_patterns_still_spread_evenly() {
        let ring = ring_with(&[0, 1, 2, 3], 200);

        let sequential_timestamps: Vec<String> = (0..40_000)
            .map(|i| format!("order:2026-10-22T10:00:{:02}-{i}", i % 60))
            .collect();

        assert!(busiest_over_average(&ring, &sequential_timestamps) < 1.3);
    }
}
