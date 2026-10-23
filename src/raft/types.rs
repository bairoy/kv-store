use std::fmt;

pub type NodeId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Appended by a new leader so entries from older terms can be committed.
    Noop,
    /// One step of a cross-partition transaction (two-phase commit); the
    /// logic lives in `raft::txn`.
    Txn(TxnCommand),
    /// A change of cluster membership. A server uses the *latest* config entry
    /// in its log as soon as the entry is appended (committed or not).
    Config(ClusterConfig),
}

/// One write inside a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnOp {
    Set { key: String, value: String },
    Delete { key: String },
}

impl TxnOp {
    pub fn key(&self) -> &str {
        match self {
            TxnOp::Set { key, .. } | TxnOp::Delete { key } => key,
        }
    }
}

/// The replicated steps of a two-phase commit. Each is an ordinary Raft log
/// entry, so every step survives crashes and leader changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnCommand {
    /// Phase 1 in one partition: lock `ops`' keys and vote.
    Prepare {
        id: String,
        /// The partition whose log holds the commit/abort decision.
        coordinator: u32,
        /// Every partition taking part (so a stuck one can be recovered).
        participants: Vec<u32>,
        /// This partition's share of the writes.
        ops: Vec<TxnOp>,
    },
    /// The decision, written in the coordinator partition. The first one for
    /// a transaction wins; later ones are ignored.
    Decide { id: String, commit: bool },
    /// Phase 2 in one partition: apply (or discard) the writes, release locks.
    Finish { id: String, commit: bool },
}

/// Who is in a Raft group.
///
/// * **voters** count toward elections and commitment;
/// * **learners** receive the log (to catch up) but never vote or count.
///
/// Lists are kept sorted and duplicate-free so equal configs encode equally.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterConfig {
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
}

impl ClusterConfig {
    pub fn new(voters: Vec<NodeId>) -> Self {
        Self::normalized(voters, Vec::new())
    }

    fn normalized(mut voters: Vec<NodeId>, mut learners: Vec<NodeId>) -> Self {
        voters.sort_unstable();
        voters.dedup();
        learners.sort_unstable();
        learners.dedup();
        learners.retain(|learner| !voters.contains(learner));

        Self { voters, learners }
    }

    pub fn is_voter(&self, id: NodeId) -> bool {
        self.voters.contains(&id)
    }

    /// Voter or learner.
    pub fn is_member(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.learners.contains(&id)
    }

    /// Votes (or replicas) needed for a majority of the voters.
    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    /// Every server that must be sent the log: voters and learners.
    pub fn members(&self) -> Vec<NodeId> {
        let mut all = self.voters.clone();
        all.extend_from_slice(&self.learners);
        all.sort_unstable();
        all
    }
}

/// One step of changing a group's membership. Each changes the voter set by
/// at most one server, so any majority of the old voters overlaps any
/// majority of the new voters (Raft thesis, section 4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChange {
    /// Start replicating to a new server without letting it vote.
    AddLearner(NodeId),
    /// Make a caught-up learner a voter.
    Promote(NodeId),
    /// Remove a voter or a learner.
    Remove(NodeId),
}

impl ConfigChange {
    /// The configuration that results from applying this change to `current`.
    pub fn apply(&self, current: &ClusterConfig) -> Result<ClusterConfig, String> {
        let mut voters = current.voters.clone();
        let mut learners = current.learners.clone();

        match *self {
            ConfigChange::AddLearner(id) => {
                if current.is_member(id) {
                    return Err(format!("server {id} is already a member"));
                }
                learners.push(id);
            }
            ConfigChange::Promote(id) => {
                if !learners.contains(&id) {
                    return Err(format!("server {id} is not a learner"));
                }
                learners.retain(|learner| *learner != id);
                voters.push(id);
            }
            ConfigChange::Remove(id) => {
                if !current.is_member(id) {
                    return Err(format!("server {id} is not a member"));
                }
                if current.is_voter(id) && voters.len() == 1 {
                    return Err("cannot remove the last voter".to_string());
                }
                voters.retain(|voter| *voter != id);
                learners.retain(|learner| *learner != id);
            }
        }

        Ok(ClusterConfig::normalized(voters, learners))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub term: u64,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: NodeId,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
}

/// One chunk of a snapshot streamed from the leader to a lagging follower.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    /// Byte offset of `data` within the encoded snapshot.
    pub offset: u64,
    pub data: Vec<u8>,
    /// True on the final chunk.
    pub done: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotReply {
    pub term: u64,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
