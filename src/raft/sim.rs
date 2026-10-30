//! Deterministic fault-injection harness for Raft (test-only).

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::raft::node::RaftNode;
use crate::raft::types::{
    ClusterConfig, Command, ConfigChange, LogEntry, NodeId, RequestVoteArgs, Role,
};

static NEXT_SIM_ID: AtomicU64 = AtomicU64::new(0);

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

fn changed(a: &[NodeId], b: &[NodeId]) -> usize {
    a.iter().filter(|id| !b.contains(id)).count() + b.iter().filter(|id| !a.contains(id)).count()
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
    initial_voters: usize,
    dir: PathBuf,
    nodes: Vec<Option<RaftNode>>,
    group: Vec<u32>,
    rng: Rng,
    drop_per_mille: usize,
    committed: BTreeMap<u64, (LogEntry, u64)>,
    leader_of_term: HashMap<u64, NodeId>,
    last_term: Vec<u64>,
    last_commit: Vec<u64>,
    writes: u64,
    history: Vec<String>,
}

impl SimCluster {
    pub fn new(name: &str, size: usize, seed: u64) -> Self {
        Self::elastic(name, size, 0, seed)
    }

    pub fn elastic(name: &str, voters: usize, spares: usize, seed: u64) -> Self {
        let size = voters + spares;
        let unique = NEXT_SIM_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join("kv_store_raft_sim")
            .join(format!("{name}_{}_{unique}", std::process::id()));

        std::fs::create_dir_all(&dir).expect("create simulation directory");

        let ids: Vec<NodeId> = (1..=size as u64).collect();

        let mut sim = Self {
            seed,
            ids,
            initial_voters: voters,
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
        let path = self.dir.join(format!("node{}.log", self.ids[i]));

        if i >= self.initial_voters {
            RaftNode::open_joining(self.ids[i], path).expect("open joining node")
        } else {
            RaftNode::open(self.ids[i], self.ids[..self.initial_voters].to_vec(), path)
                .expect("open Raft node")
        }
    }

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

    pub fn election_timeout(&mut self, i: usize) {
        if let Some(args) = self.start_candidacy(i) {
            self.solicit_votes(i, &args);
            self.check_invariants();
        }
    }

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

        let args = self.nodes[i].as_mut().unwrap().start_election().ok()?;

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

    pub fn propose(&mut self, i: usize, change: ConfigChange) -> std::io::Result<u64> {
        let result = self.nodes[i]
            .as_mut()
            .expect("node is crashed")
            .propose_config_change(change);

        self.note(format!(
            "node {} proposes {change:?}: {result:?}",
            self.ids[i]
        ));
        self.check_invariants();

        result
    }

    pub fn random_config_change(&mut self, i: usize) {
        if !self.is_alive(i) || self.node(i).role != Role::Leader {
            return;
        }

        let config = self.node(i).config().clone();
        let mut options = Vec::new();

        for id in &self.ids {
            if !config.is_member(*id) {
                options.push(ConfigChange::AddLearner(*id));
            }
        }
        for learner in &config.learners {
            options.push(ConfigChange::Promote(*learner));
        }
        for member in config.members() {
            options.push(ConfigChange::Remove(member));
        }

        if options.is_empty() {
            return;
        }

        let change = options[self.rng.below(options.len())];
        let _ = self.propose(i, change);
    }

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
        self.last_commit[i] = self.node(i).commit_index;

        self.note(format!("node {} restarts", self.ids[i]));
        self.check_invariants();
    }

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

    pub fn random_step(&mut self) {
        let n = self.len();
        let pick = self.rng.below(n);

        match self.rng.below(100) {
            0..=7 => self.election_timeout(pick),
            8..=13 => {
                let other = self.rng.below(n);
                self.concurrent_elections(pick, other);
            }
            14..=46 => {
                let leaders = self.leaders();

                if !leaders.is_empty() {
                    let leader = leaders[self.rng.below(leaders.len())];
                    self.replicate(leader);
                }
            }
            47..=51 => {
                let leaders = self.leaders();

                if !leaders.is_empty() {
                    let leader = leaders[self.rng.below(leaders.len())];
                    self.random_config_change(leader);
                }
            }
            52..=74 => {
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

    pub fn replicate_until_converged(&mut self, i: usize) {
        if !self.replicate_until_converged_or_deposed(i) {
            fail(
                self.seed,
                &self.history,
                "the leader lost its leadership while converging",
            );
        }
    }

    fn replicate_until_converged_or_deposed(&mut self, i: usize) -> bool {
        for _ in 0..500 {
            self.replicate(i);

            if self.node(i).role != Role::Leader {
                return false;
            }

            if self.converged(i) {
                return true;
            }
        }

        fail(self.seed, &self.history, "cluster did not converge");
    }

    fn converged(&self, leader: usize) -> bool {
        let reference = self.node(leader);
        let members = reference.config().members();

        self.alive()
            .into_iter()
            .filter(|i| members.contains(&self.ids[*i]) || *i == leader)
            .all(|i| {
                let node = self.node(i);

                node.log.last_index() == reference.log.last_index()
                    && node.commit_index == reference.log.last_index()
            })
    }

    pub fn settle(&mut self) {
        self.heal();
        self.set_message_loss(0);

        for i in 0..self.len() {
            self.restart(i);
        }

        for _ in 0..400 {
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

                    if self.replicate_until_converged_or_deposed(leader) {
                        self.check_logs_identical(leader);
                        return;
                    }
                }
                _ => {}
            }
        }

        let state = self.describe();
        fail(
            self.seed,
            &self.history,
            &format!("no stable leader after healing\n{state}"),
        );
    }

    fn describe(&self) -> String {
        (0..self.len())
            .map(|i| match &self.nodes[i] {
                None => format!("node {}: crashed", self.ids[i]),
                Some(node) => format!(
                    "node {}: {:?} term {} log {}/commit {} voted_for {:?} last_term {} config {:?}",
                    node.id,
                    node.role,
                    node.current_term,
                    node.log.last_index(),
                    node.commit_index,
                    node.voted_for,
                    node.log.last_term(),
                    node.config(),
                ),
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn check_logs_identical(&self, leader: usize) {
        let reference = self.node(leader);
        let members = reference.config().members();

        let others: Vec<usize> = self
            .alive()
            .into_iter()
            .filter(|i| *i != leader && members.contains(&self.ids[*i]))
            .collect();

        for i in &others {
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

        for i in self.alive() {
            let node = self.node(i);

            if *node.config() != node.log.latest_config().1 {
                fail(
                    seed,
                    &self.history,
                    &format!(
                        "node {} caches a config that its log does not hold",
                        node.id
                    ),
                );
            }
        }

        let mut previous = ClusterConfig::new(self.ids[..self.initial_voters].to_vec());

        for (index, (entry, _)) in &self.committed {
            if let Command::Config(next) = &entry.command {
                let voter_change = changed(&previous.voters, &next.voters);
                let member_change = changed(&previous.members(), &next.members());

                if voter_change > 1 || member_change > 1 {
                    fail(
                        seed,
                        &self.history,
                        &format!(
                            "config at index {index} changes more than one server: \
                             {previous:?} -> {next:?}"
                        ),
                    );
                }

                previous = next.clone();
            }
        }

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

        sim.election_timeout(2);
        assert_eq!(sim.node(2).role, Role::Leader);
        sim.client_write(2);
        for _ in 0..10 {
            sim.replicate(2);
        }
        assert!(sim.node(2).commit_index > committed_before);

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

        sim.partition(&[&[0, 1], &[2]]);
        assert!(sim.client_write(0));
        sim.replicate(0);
        assert!(sim.log_contains_key(1, "key1"));
        assert!(!sim.log_contains_key(2, "key1"));
        sim.crash(0);
        sim.heal();

        sim.election_timeout(2);
        assert_ne!(sim.node(2).role, Role::Leader);

        sim.election_timeout(1);
        assert_eq!(sim.node(1).role, Role::Leader);
        sim.replicate_until_converged(1);

        assert!(sim.log_contains_key(2, "key1"));

        sim.settle();
        assert!(sim.log_contains_key(0, "key1"));
    }

    fn id_index(sim: &SimCluster, id: NodeId) -> usize {
        (0..sim.len())
            .find(|i| sim.ids[*i] == id)
            .expect("known id")
    }

    #[test]
    fn grow_from_three_to_five_servers_then_survive_two_failures() {
        let mut sim = SimCluster::elastic("grow", 3, 2, 11);

        sim.election_timeout(0);
        assert_eq!(sim.node(0).role, Role::Leader);
        sim.replicate_until_converged(0);

        for id in [4, 5] {
            sim.propose(0, ConfigChange::AddLearner(id))
                .expect("add learner");
            sim.replicate_until_converged(0);

            sim.propose(0, ConfigChange::Promote(id)).expect("promote");
            sim.replicate_until_converged(0);
        }

        assert_eq!(sim.node(0).config().voters, vec![1, 2, 3, 4, 5]);
        assert_eq!(sim.node(0).quorum_size(), 3);
        sim.check_logs_identical(0);

        sim.crash(1);
        sim.crash(2);
        let before = sim.node(0).commit_index;
        assert!(sim.client_write(0));
        for _ in 0..10 {
            sim.replicate(0);
        }
        assert!(sim.node(0).commit_index > before, "3 of 5 is a majority");

        sim.crash(3);
        let stuck = sim.node(0).commit_index;
        assert!(sim.client_write(0));
        for _ in 0..10 {
            sim.replicate(0);
        }
        assert_eq!(sim.node(0).commit_index, stuck, "2 of 5 must not commit");

        sim.settle();
    }

    #[test]
    fn shrink_from_five_servers_to_three_and_stop_sending_to_the_removed() {
        let mut sim = SimCluster::elastic("shrink", 3, 2, 12);

        sim.election_timeout(0);
        sim.replicate_until_converged(0);

        for id in [4, 5] {
            sim.propose(0, ConfigChange::AddLearner(id))
                .expect("add learner");
            sim.replicate_until_converged(0);
            sim.propose(0, ConfigChange::Promote(id)).expect("promote");
            sim.replicate_until_converged(0);
        }

        for id in [4, 5] {
            sim.propose(0, ConfigChange::Remove(id)).expect("remove");
            sim.replicate_until_converged(0);
        }

        assert_eq!(sim.node(0).config().voters, vec![1, 2, 3]);

        let removed_before = sim.node(id_index(&sim, 5)).log.last_index();
        sim.client_write(0);
        sim.replicate_until_converged(0);
        assert_eq!(sim.node(id_index(&sim, 5)).log.last_index(), removed_before);

        sim.crash(1);
        let before = sim.node(0).commit_index;
        assert!(sim.client_write(0));
        for _ in 0..10 {
            sim.replicate(0);
        }
        assert!(sim.node(0).commit_index > before);

        sim.settle();
    }

    #[test]
    fn a_config_change_lost_with_its_leader_is_rolled_back_everywhere() {
        let mut sim = SimCluster::elastic("config_lost", 3, 1, 13);

        sim.election_timeout(0);
        sim.replicate_until_converged(0);

        sim.propose(0, ConfigChange::AddLearner(4))
            .expect("add learner");
        assert!(sim.node(0).config().learners == vec![4]);
        sim.crash(0);

        sim.election_timeout(1);
        assert_eq!(sim.node(1).role, Role::Leader);
        assert!(sim.node(1).config().learners.is_empty());
        sim.replicate_until_converged(1);

        sim.restart(0);
        assert_eq!(
            sim.node(0).config().learners,
            vec![4],
            "still has its stale entry"
        );
        sim.settle();

        assert!(sim.node(0).config().learners.is_empty());
        assert!(!sim.node(0).config().is_member(4));
    }

    #[test]
    fn a_leader_that_crashes_while_removing_itself_does_not_strand_the_group() {
        let mut sim = SimCluster::elastic("remove_self_crash", 2, 0, 14);

        sim.election_timeout(1);
        assert_eq!(sim.node(1).role, Role::Leader);
        sim.replicate_until_converged(1);

        sim.propose(1, ConfigChange::Remove(2))
            .expect("leader removes itself");
        assert_eq!(sim.node(1).config().voters, vec![1]);

        sim.crash(1);
        sim.restart(1);

        sim.election_timeout(0);
        assert_ne!(sim.node(0).role, Role::Leader);
        assert!(
            sim.node(1).can_campaign(),
            "still a voter in the committed config"
        );

        sim.election_timeout(1);
        assert_eq!(sim.node(1).role, Role::Leader);

        sim.settle();
        assert_eq!(sim.node(0).config().voters, vec![1]);
    }

    #[test]
    fn a_voter_that_knows_no_config_yet_still_helps_elect_a_leader() {
        let mut sim = SimCluster::elastic("empty_voter", 2, 1, 15);

        sim.election_timeout(0);
        sim.replicate_until_converged(0);

        sim.propose(0, ConfigChange::AddLearner(3))
            .expect("add learner");
        sim.partition(&[&[0, 1], &[2]]);
        for _ in 0..5 {
            sim.replicate(0);
        }

        sim.propose(0, ConfigChange::Promote(3)).expect("promote");
        for _ in 0..5 {
            sim.replicate(0);
        }

        assert_eq!(sim.node(0).config().voters, vec![1, 2, 3]);
        assert!(
            sim.node(2).config().voters.is_empty(),
            "server 3 knows nothing"
        );

        sim.crash(0);
        sim.heal();
        sim.election_timeout(1);
        assert_eq!(sim.node(1).role, Role::Leader);

        sim.settle();
    }

    #[test]
    fn random_faults_and_membership_changes_never_break_safety() {
        for seed in 0..8 {
            let mut sim = SimCluster::elastic("fuzz_members", 3, 2, 500 + seed);

            for _ in 0..250 {
                sim.random_step();
            }

            sim.settle();
        }

        for seed in 0..4 {
            let mut sim = SimCluster::elastic("fuzz_members5", 5, 1, 900 + seed);

            for _ in 0..250 {
                sim.random_step();
            }

            sim.settle();
        }
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

    #[test]
    #[ignore]
    fn replay_soak_seed() {
        let seed: u64 = std::env::var("SIM_SEED")
            .ok()
            .and_then(|value| value.parse().ok())
            .expect("set SIM_SEED");

        let mut sim = SimCluster::elastic("replay", 3, 2, seed);

        for _ in 0..1200 {
            sim.random_step();
        }

        sim.settle();
    }

    #[test]
    #[ignore]
    fn random_faults_soak() {
        for seed in 1000..1030 {
            run_random(seed, 3, 1200);
            run_random(seed + 10_000, 5, 1200);

            let mut sim = SimCluster::elastic("soak_members", 3, 2, seed + 20_000);
            for _ in 0..1200 {
                sim.random_step();
            }
            sim.settle();
        }
    }
}
