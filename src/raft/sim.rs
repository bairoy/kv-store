//! Deterministic fault-injection harness for Raft (test-only).
//!
//! A `SimCluster` runs real `RaftNode`s (with real on-disk logs) but replaces
//! the network and timers with direct calls driven by a seeded RNG. It can
//! crash and restart nodes, partition the network and drop messages, and after
//! every step it checks the safety properties from the Raft paper:
//!
//! * election safety      - at most one leader per term
//! * log matching         - equal (index, term) implies equal prefix
//! * state machine safety - no two different entries commit at one index
//! * leader completeness  - a leader holds every entry committed in earlier
//!   terms
//! * monotonicity         - terms and commit indexes never go backwards
//!
//! Failures print the seed and the tail of the operation history.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::raft::node::RaftNode;
use crate::raft::types::{Command, LogEntry, NodeId, RequestVoteArgs, Role};

static NEXT_SIM_ID: AtomicU64 = AtomicU64::new(0);

/// Entries per AppendEntries in the simulation; small, to exercise batching.
const SIM_BATCH: usize = 3;
const HISTORY_TAIL: usize = 40;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03 | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn fail(seed: u64, history: &[String], message: &str) -> ! {
    let start = history.len().saturating_sub(HISTORY_TAIL);

    panic!(
        "Raft invariant violated (seed {seed}): {message}\nlast operations:\n  {}",
        history[start..].join("\n  ")
    );
}

pub struct SimCluster {
    seed: u64,
    ids: Vec<NodeId>,
    dir: PathBuf,
    nodes: Vec<Option<RaftNode>>,
    group: Vec<u32>,
    rng: Rng,
    drop_per_mille: usize,
    /// Every entry ever seen committed: index -> (entry, term of the node
    /// that first reported it, i.e. the committing leader's term).
    committed: BTreeMap<u64, (LogEntry, u64)>,
    leader_of_term: HashMap<u64, NodeId>,
    last_term: Vec<u64>,
    last_commit: Vec<u64>,
    writes: u64,
    history: Vec<String>,
}

impl SimCluster {
    pub fn new(name: &str, size: usize, seed: u64) -> Self {
        let unique = NEXT_SIM_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join("kv_store_raft_sim")
            .join(format!("{name}_{}_{unique}", std::process::id()));

        std::fs::create_dir_all(&dir).expect("create simulation directory");

        let ids: Vec<NodeId> = (1..=size as u64).collect();

        let mut sim = Self {
            seed,
            ids,
            dir,
            nodes: (0..size).map(|_| None).collect(),
            group: vec![0; size],
            rng: Rng::new(seed),
            drop_per_mille: 0,
            committed: BTreeMap::new(),
            leader_of_term: HashMap::new(),
            last_term: vec![0; size],
            last_commit: vec![0; size],
            writes: 0,
            history: Vec::new(),
        };

        for i in 0..size {
            sim.nodes[i] = Some(sim.open_node(i));
        }

        sim
    }

    fn open_node(&self, i: usize) -> RaftNode {
        RaftNode::open(
            self.ids[i],
            self.ids.clone(),
            self.dir.join(format!("node{}.log", self.ids[i])),
        )
        .expect("open Raft node")
    }

    // ----------------------------------------------------------- accessors

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn node(&self, i: usize) -> &RaftNode {
        self.nodes[i].as_ref().expect("node is crashed")
    }

    pub fn is_alive(&self, i: usize) -> bool {
        self.nodes[i].is_some()
    }

    fn alive(&self) -> Vec<usize> {
        (0..self.len()).filter(|i| self.is_alive(*i)).collect()
    }

    fn leaders(&self) -> Vec<usize> {
        self.alive()
            .into_iter()
            .filter(|i| self.node(*i).role == Role::Leader)
            .collect()
    }

    fn note(&mut self, text: String) {
        self.history.push(text);
    }

    fn can_talk(&self, a: usize, b: usize) -> bool {
        self.is_alive(a) && self.is_alive(b) && self.group[a] == self.group[b]
    }

    fn message_survives(&mut self) -> bool {
        self.rng.below(1000) >= self.drop_per_mille
    }

    // ---------------------------------------------------------- operations

    /// Node `i`'s election timer fires: it runs for office and asks everyone
    /// it can reach.
    pub fn election_timeout(&mut self, i: usize) {
        if let Some(args) = self.start_candidacy(i) {
            self.solicit_votes(i, &args);
            self.check_invariants();
        }
    }

    /// Two nodes time out together: both become candidates (often for the
    /// same term) before either collects a vote.
    pub fn concurrent_elections(&mut self, a: usize, b: usize) {
        if a == b {
            return;
        }

        let first = self.start_candidacy(a);
        let second = self.start_candidacy(b);

        if let Some(args) = first {
            self.solicit_votes(a, &args);
        }

        if let Some(args) = second {
            self.solicit_votes(b, &args);
        }

        self.check_invariants();
    }

    fn start_candidacy(&mut self, i: usize) -> Option<RequestVoteArgs> {
        if !self.is_alive(i) {
            return None;
        }

        let args = self.nodes[i]
            .as_mut()
            .unwrap()
            .start_election()
            .expect("start election");

        self.note(format!(
            "node {} starts election, term {}",
            self.ids[i], args.term
        ));

        Some(args)
    }

    fn solicit_votes(&mut self, i: usize, args: &RequestVoteArgs) {
        for j in 0..self.len() {
            if j == i || !self.can_talk(i, j) || !self.message_survives() {
                continue;
            }

            let reply = self.nodes[j]
                .as_mut()
                .unwrap()
                .handle_vote_request(args.clone())
                .expect("handle vote request");

            if !self.message_survives() {
                continue;
            }

            let voter = self.ids[j];

            self.nodes[i]
                .as_mut()
                .unwrap()
                .record_vote_response(voter, reply)
                .expect("record vote response");
        }
    }

    /// Leader `i` sends one round of AppendEntries to every reachable peer.
    pub fn replicate(&mut self, i: usize) {
        if !self.is_alive(i) || self.node(i).role != Role::Leader {
            return;
        }

        self.note(format!("leader {} replicates", self.ids[i]));

        for j in 0..self.len() {
            if j == i || !self.can_talk(i, j) {
                continue;
            }

            let peer = self.ids[j];

            let Some(args) = self.nodes[i]
                .as_ref()
                .unwrap()
                .build_append_entries(peer, SIM_BATCH)
            else {
                continue;
            };

            if !self.message_survives() {
                continue;
            }

            // An error here would mean the protocol asked a follower to
            // overwrite a committed entry, which must never happen.
            let reply = self.nodes[j]
                .as_mut()
                .unwrap()
                .handle_append_entries(args.clone())
                .unwrap_or_else(|error| {
                    fail(
                        self.seed,
                        &self.history,
                        &format!("follower {peer} rejected leader input: {error}"),
                    )
                });

            if !self.message_survives() {
                continue;
            }

            self.nodes[i]
                .as_mut()
                .unwrap()
                .handle_append_entries_reply(peer, &args, reply)
                .expect("handle AppendEntries reply");
        }

        self.check_invariants();
    }

    /// A client write arrives at node `i` (accepted only if it is a leader).
    pub fn client_write(&mut self, i: usize) -> bool {
        if !self.is_alive(i) || self.node(i).role != Role::Leader {
            return false;
        }

        self.writes += 1;

        let command = Command::Set {
            key: format!("key{}", self.writes),
            value: format!("value{}", self.writes),
        };

        let accepted = self.nodes[i]
            .as_mut()
            .unwrap()
            .append_client_command(command)
            .is_ok();

        self.note(format!(
            "client write {} at node {}",
            self.writes, self.ids[i]
        ));
        self.check_invariants();

        accepted
    }

    /// Lose all volatile state; the disk survives.
    pub fn crash(&mut self, i: usize) {
        if self.nodes[i].take().is_some() {
            self.note(format!("node {} crashes", self.ids[i]));
        }
    }

    pub fn restart(&mut self, i: usize) {
        if self.is_alive(i) {
            return;
        }

        self.nodes[i] = Some(self.open_node(i));
        // commit_index is volatile in Raft, so monotonicity restarts here.
        self.last_commit[i] = self.node(i).commit_index;

        self.note(format!("node {} restarts", self.ids[i]));
        self.check_invariants();
    }

    /// Split the network: nodes can only talk within their own group.
    pub fn partition(&mut self, groups: &[&[usize]]) {
        for (number, members) in groups.iter().enumerate() {
            for member in *members {
                self.group[*member] = number as u32;
            }
        }

        self.note(format!("partition {groups:?}"));
    }

    pub fn heal(&mut self) {
        self.group = vec![0; self.len()];
        self.note("network healed".to_string());
    }

    pub fn set_message_loss(&mut self, per_mille: usize) {
        self.drop_per_mille = per_mille;
        self.note(format!("message loss {per_mille} per mille"));
    }

    /// Run one random fault or protocol step.
    pub fn random_step(&mut self) {
        let n = self.len();
        let pick = self.rng.below(n);

        match self.rng.below(100) {
            0..=7 => self.election_timeout(pick),
            8..=13 => {
                let other = self.rng.below(n);
                self.concurrent_elections(pick, other);
            }
            14..=49 => {
                let leaders = self.leaders();

                if !leaders.is_empty() {
                    let leader = leaders[self.rng.below(leaders.len())];
                    self.replicate(leader);
                }
            }
            50..=74 => {
                // Writes go to a leader when there is one, so they pile up.
                let leaders = self.leaders();

                if leaders.is_empty() {
                    self.client_write(pick);
                } else {
                    let leader = leaders[self.rng.below(leaders.len())];
                    self.client_write(leader);
                }
            }
            75..=80 => self.crash(pick),
            81..=88 => self.restart(pick),
            89..=93 => {
                let assignment: Vec<usize> = (0..n).map(|_| self.rng.below(2)).collect();
                let left: Vec<usize> = (0..n).filter(|i| assignment[*i] == 0).collect();
                let right: Vec<usize> = (0..n).filter(|i| assignment[*i] == 1).collect();

                self.partition(&[&left, &right]);
            }
            94..=96 => self.heal(),
            _ => {
                let loss = [0, 100, 300][self.rng.below(3)];
                self.set_message_loss(loss);
            }
        }
    }

    /// Replicate from leader `i` until every node has the same log and the
    /// same commit index. Panics if that does not happen.
    pub fn replicate_until_converged(&mut self, i: usize) {
        for _ in 0..500 {
            self.replicate(i);

            if self.converged(i) {
                return;
            }
        }

        fail(self.seed, &self.history, "cluster did not converge");
    }

    fn converged(&self, leader: usize) -> bool {
        let reference = self.node(leader);

        self.alive().into_iter().all(|i| {
            let node = self.node(i);

            node.log.last_index() == reference.log.last_index()
                && node.commit_index == reference.log.last_index()
        })
    }

    /// Heal everything, restart everyone, and require the cluster to elect a
    /// leader and commit a fresh write everywhere (liveness after faults).
    pub fn settle(&mut self) {
        self.heal();
        self.set_message_loss(0);

        for i in 0..self.len() {
            self.restart(i);
        }

        for _ in 0..400 {
            // Stale leaders discover the newer term when they talk.
            for leader in self.leaders() {
                self.replicate(leader);
            }

            let leaders = self.leaders();

            match leaders.as_slice() {
                [] => {
                    let candidate = self.rng.below(self.len());
                    self.election_timeout(candidate);
                }
                [leader] => {
                    let leader = *leader;

                    self.client_write(leader);
                    self.replicate_until_converged(leader);
                    self.check_logs_identical();
                    return;
                }
                _ => {}
            }
        }

        fail(self.seed, &self.history, "no stable leader after healing");
    }

    pub fn check_logs_identical(&self) {
        let alive = self.alive();
        let reference = self.node(alive[0]);

        for i in &alive[1..] {
            let node = self.node(*i);

            if node.log.last_index() != reference.log.last_index() {
                fail(self.seed, &self.history, "logs differ in length");
            }

            for index in 1..=reference.log.last_index() {
                if node.log.entry_at(index) != reference.log.entry_at(index) {
                    fail(self.seed, &self.history, "logs differ in content");
                }
            }
        }
    }

    pub fn log_contains_key(&self, i: usize, key: &str) -> bool {
        let log = &self.node(i).log;

        (1..=log.last_index()).any(|index| {
            matches!(
                log.entry_at(index).map(|entry| &entry.command),
                Some(Command::Set { key: k, .. }) if k == key
            )
        })
    }

    // ---------------------------------------------------------- invariants

    pub fn check_invariants(&mut self) {
        let seed = self.seed;

        for i in 0..self.len() {
            let Some(node) = self.nodes[i].as_ref() else {
                continue;
            };

            if node.current_term < self.last_term[i] {
                fail(seed, &self.history, "a node's term went backwards");
            }
            self.last_term[i] = node.current_term;

            if node.commit_index < self.last_commit[i] {
                fail(seed, &self.history, "a node's commit index went backwards");
            }
            self.last_commit[i] = node.commit_index;

            if node.commit_index > node.log.last_index() {
                fail(seed, &self.history, "commit index is beyond the log");
            }

            // Election safety.
            if node.role == Role::Leader {
                match self.leader_of_term.get(&node.current_term) {
                    Some(other) if *other != node.id => fail(
                        seed,
                        &self.history,
                        &format!(
                            "two leaders in term {}: nodes {} and {}",
                            node.current_term, other, node.id
                        ),
                    ),
                    _ => {
                        self.leader_of_term.insert(node.current_term, node.id);
                    }
                }
            }

            // State machine safety.
            for index in 1..=node.commit_index {
                let Some(entry) = node.log.entry_at(index) else {
                    fail(seed, &self.history, "committed entry missing from log");
                };

                match self.committed.get(&index) {
                    Some((existing, _)) if existing != entry => fail(
                        seed,
                        &self.history,
                        &format!("different entries committed at index {index}"),
                    ),
                    Some(_) => {}
                    None => {
                        self.committed
                            .insert(index, (entry.clone(), node.current_term));
                    }
                }
            }
        }

        // Leader completeness: a leader of a term at or after the term that
        // committed an entry must hold it. (A stale leader from an earlier
        // term legitimately does not.)
        for i in self.leaders() {
            let node = self.node(i);

            for (index, (entry, term)) in &self.committed {
                if node.current_term >= *term && node.log.entry_at(*index) != Some(entry) {
                    fail(
                        seed,
                        &self.history,
                        &format!(
                            "leader {} (term {}) lacks entry {index} committed in term {term}",
                            node.id, node.current_term
                        ),
                    );
                }
            }
        }

        // Log matching.
        let alive = self.alive();

        for (position, a) in alive.iter().enumerate() {
            for b in &alive[position + 1..] {
                let (first, last) = (self.node(*a), self.node(*b));
                let low = first.log.first_index().max(last.log.first_index());
                let high = first.log.last_index().min(last.log.last_index());

                let Some(agree) = (low..=high)
                    .rev()
                    .find(|index| first.log.term_at(*index) == last.log.term_at(*index))
                else {
                    continue;
                };

                for index in low..=agree {
                    if first.log.entry_at(index) != last.log.entry_at(index) {
                        fail(
                            seed,
                            &self.history,
                            &format!(
                                "logs of nodes {} and {} agree at index {agree} but differ at {index}",
                                first.id, last.id
                            ),
                        );
                    }
                }
            }
        }
    }
}

impl Drop for SimCluster {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_random(seed: u64, size: usize, steps: usize) {
        let mut sim = SimCluster::new("fuzz", size, seed);

        for _ in 0..steps {
            sim.random_step();
        }

        // After arbitrary faults the cluster must still be able to recover.
        sim.settle();
    }

    #[test]
    fn minority_partitioned_leader_cannot_commit_and_is_overwritten() {
        let mut sim = SimCluster::new("minority_leader", 5, 1);

        sim.election_timeout(0);
        assert_eq!(sim.node(0).role, Role::Leader);

        sim.client_write(0);
        sim.replicate_until_converged(0);
        let committed_before = sim.node(0).commit_index;
        assert!(committed_before >= 2);

        // The old leader keeps only one follower and keeps accepting writes.
        sim.partition(&[&[0, 1], &[2, 3, 4]]);
        sim.client_write(0);
        sim.client_write(0);
        for _ in 0..10 {
            sim.replicate(0);
        }
        assert_eq!(
            sim.node(0).commit_index,
            committed_before,
            "a minority must not commit"
        );
        assert!(sim.node(0).log.last_index() > committed_before);
        assert!(sim.log_contains_key(0, "key2"));

        // The majority side elects a new leader and commits.
        sim.election_timeout(2);
        assert_eq!(sim.node(2).role, Role::Leader);
        sim.client_write(2);
        for _ in 0..10 {
            sim.replicate(2);
        }
        assert!(sim.node(2).commit_index > committed_before);

        // After healing, the old leader steps down and its writes vanish.
        sim.settle();

        assert_ne!(sim.node(0).role, Role::Leader);
        for i in 0..sim.len() {
            assert!(!sim.log_contains_key(i, "key2"), "stale write survived");
            assert!(!sim.log_contains_key(i, "key3"), "stale write survived");
        }
    }

    #[test]
    fn entry_on_a_majority_survives_the_leader_crashing_before_the_ack() {
        let mut sim = SimCluster::new("crash_after_majority", 3, 2);

        sim.election_timeout(0);
        sim.replicate_until_converged(0);

        // The leader replicates one entry to a majority (itself and node 1),
        // so it counts as committed, then crashes before telling anyone.
        sim.partition(&[&[0, 1], &[2]]);
        assert!(sim.client_write(0));
        sim.replicate(0);
        assert!(sim.log_contains_key(1, "key1"));
        assert!(!sim.log_contains_key(2, "key1"));
        sim.crash(0);
        sim.heal();

        // Node 2 is behind, so node 1 must refuse it.
        sim.election_timeout(2);
        assert_ne!(sim.node(2).role, Role::Leader);

        // Node 1 has the entry and wins; the entry must reach node 2.
        sim.election_timeout(1);
        assert_eq!(sim.node(1).role, Role::Leader);
        sim.replicate_until_converged(1);

        assert!(sim.log_contains_key(2, "key1"));

        // The crashed old leader rejoins as a follower and converges.
        sim.settle();
        assert!(sim.log_contains_key(0, "key1"));
    }

    #[test]
    fn leader_with_lost_messages_still_converges_once_the_network_recovers() {
        let mut sim = SimCluster::new("lossy", 3, 3);

        sim.set_message_loss(400);

        for _ in 0..200 {
            sim.random_step();
        }

        sim.settle();
    }

    #[test]
    fn random_faults_never_break_safety() {
        for seed in 0..8 {
            run_random(seed, 3, 250);
        }

        for seed in 100..104 {
            run_random(seed, 5, 250);
        }
    }

    /// Longer soak run: `cargo test --lib raft::sim -- --ignored`
    #[test]
    #[ignore]
    fn random_faults_soak() {
        for seed in 1000..1040 {
            run_random(seed, 3, 1500);
            run_random(seed + 10_000, 5, 1500);
        }
    }
}
