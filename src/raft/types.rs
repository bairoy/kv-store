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
    Noop,
    Txn(TxnCommand),
    Config(ClusterConfig),
    Session {
        client: String,
        seq: u64,
        op: SessionOp,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionOp {
    Set { key: String, value: String },
    Delete { key: String },
    Incr { key: String },
}

impl SessionOp {
    pub fn key(&self) -> &str {
        match self {
            SessionOp::Set { key, .. } | SessionOp::Delete { key } | SessionOp::Incr { key } => key,
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnCommand {
    Prepare {
        id: String,
        coordinator: u32,
        participants: Vec<u32>,
        ops: Vec<TxnOp>,
    },
    Decide {
        id: String,
        commit: bool,
    },
    Finish {
        id: String,
        commit: bool,
    },
}

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

    pub fn is_member(&self, id: NodeId) -> bool {
        self.is_voter(id) || self.learners.contains(&id)
    }

    pub fn quorum(&self) -> usize {
        self.voters.len() / 2 + 1
    }

    pub fn members(&self) -> Vec<NodeId> {
        let mut all = self.voters.clone();
        all.extend_from_slice(&self.learners);
        all.sort_unstable();
        all
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigChange {
    AddLearner(NodeId),
    Promote(NodeId),
    Remove(NodeId),
}

impl ConfigChange {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstallSnapshotArgs {
    pub term: u64,
    pub leader_id: NodeId,
    pub last_included_index: u64,
    pub last_included_term: u64,
    pub offset: u64,
    pub data: Vec<u8>,
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
