use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};

use crate::raft::{
    log::RaftLog,
    persistence::PersistentState,
    snapshot::RaftSnapshot,
    types::{
        AppendEntriesArgs, AppendEntriesReply, ClusterConfig, Command, ConfigChange, LogEntry,
        NodeId, RequestVoteArgs, RequestVoteReply, Role,
    },
};

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[derive(Debug)]
pub struct RaftNode {
    pub id: NodeId,
    pub role: Role,
    pub current_term: u64,
    pub voted_for: Option<NodeId>,
    pub log: RaftLog,
    pub commit_index: u64,
    pub last_applied: u64,
    pub leader_id: Option<NodeId>,

    config: ClusterConfig,
    config_index: u64,
    votes_received: HashSet<NodeId>,
    state_path: PathBuf,
    snapshot_path: PathBuf,

    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
}

impl RaftNode {
    pub fn open(id: NodeId, members: Vec<NodeId>, log_path: impl AsRef<Path>) -> io::Result<Self> {
        if members.is_empty() {
            return Err(invalid_input("cluster must have members"));
        }

        if !members.contains(&id) {
            return Err(invalid_input("members must include this node"));
        }

        let mut unique = HashSet::new();
        if !members.iter().all(|member| unique.insert(*member)) {
            return Err(invalid_input("member IDs must be unique"));
        }

        Self::open_with(id, ClusterConfig::new(members), log_path)
    }

    pub fn open_joining(id: NodeId, log_path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with(id, ClusterConfig::default(), log_path)
    }

    fn open_with(
        id: NodeId,
        bootstrap: ClusterConfig,
        log_path: impl AsRef<Path>,
    ) -> io::Result<Self> {
        let log_path = log_path.as_ref().to_path_buf();
        let state_path = log_path.with_extension("state");
        let snapshot_path = log_path.with_extension("snapshot");
        let persistent = PersistentState::load(&state_path)?;
        let log = RaftLog::open_with_config(&log_path, bootstrap)?;

        if log.base_index() > 0 {
            let (index, _) = RaftSnapshot::read_meta(&snapshot_path).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("Raft log is compacted but its snapshot is unusable: {error}"),
                )
            })?;

            if index < log.base_index() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Raft snapshot is older than the compacted log",
                ));
            }
        }

        let snapshot_index = log.base_index();
        let (config_index, config) = log.latest_config();

        Ok(Self {
            id,
            role: Role::Follower,
            current_term: persistent.current_term,
            voted_for: persistent.voted_for,
            log,
            commit_index: snapshot_index,
            last_applied: snapshot_index,
            leader_id: None,
            config,
            config_index,
            votes_received: HashSet::new(),
            state_path,
            snapshot_path,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
        })
    }

    fn persist_state(&self) -> io::Result<()> {
        PersistentState {
            current_term: self.current_term,
            voted_for: self.voted_for,
        }
        .save(&self.state_path)
    }

    fn step_down_for_higher_term(&mut self, term: u64) -> io::Result<()> {
        if term > self.current_term {
            let old_term = self.current_term;
            let old_vote = self.voted_for;

            self.current_term = term;
            self.voted_for = None;

            if let Err(error) = self.persist_state() {
                self.current_term = old_term;
                self.voted_for = old_vote;
                return Err(error);
            }
        }

        self.role = Role::Follower;
        self.votes_received.clear();
        self.leader_id = None;
        Ok(())
    }

    pub fn quorum_size(&self) -> usize {
        self.config.quorum()
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    pub fn is_voter(&self) -> bool {
        self.config.is_voter(self.id)
    }

    pub fn has_pending_config(&self) -> bool {
        self.config_index > self.commit_index
    }

    pub fn config_as_of(&self, index: u64) -> ClusterConfig {
        self.log.config_as_of(index).1
    }

    pub fn match_index_of(&self, peer: NodeId) -> Option<u64> {
        self.match_index.get(&peer).copied()
    }

    pub fn can_campaign(&self) -> bool {
        self.config.is_voter(self.id)
            || self.log.config_as_of(self.commit_index).1.is_voter(self.id)
    }

    fn counted_votes(&self) -> usize {
        self.votes_received
            .iter()
            .filter(|voter| self.config.is_voter(**voter))
            .count()
    }

    pub fn start_election(&mut self) -> io::Result<RequestVoteArgs> {
        if !self.can_campaign() {
            return Err(io::Error::other("not a voting member of this group"));
        }

        let next_term = self
            .current_term
            .checked_add(1)
            .ok_or_else(|| io::Error::other("Raft term overflow"))?;

        let old_term = self.current_term;
        let old_vote = self.voted_for;

        self.current_term = next_term;
        self.voted_for = Some(self.id);

        if let Err(error) = self.persist_state() {
            self.current_term = old_term;
            self.voted_for = old_vote;
            return Err(error);
        }

        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes_received.clear();
        self.votes_received.insert(self.id);

        if self.counted_votes() >= self.quorum_size() {
            self.become_leader()?;
        }

        Ok(RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        })
    }

    pub fn handle_vote_request(&mut self, args: RequestVoteArgs) -> io::Result<RequestVoteReply> {
        let knows_voters = !self.config.voters.is_empty();

        if knows_voters
            && !self.config.is_voter(args.candidate_id)
            && !self
                .log
                .config_as_of(self.commit_index)
                .1
                .is_voter(args.candidate_id)
        {
            return Ok(RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            });
        }

        if args.term < self.current_term {
            return Ok(RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            });
        }

        if args.term > self.current_term {
            self.step_down_for_higher_term(args.term)?;
        }

        let my_last_term = self.log.last_term();
        let my_last_index = self.log.last_index();

        let candidate_is_up_to_date = args.last_log_term > my_last_term
            || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);

        let vote_granted = can_vote && candidate_is_up_to_date;

        if vote_granted && self.voted_for != Some(args.candidate_id) {
            let old_vote = self.voted_for;
            self.voted_for = Some(args.candidate_id);

            if let Err(error) = self.persist_state() {
                self.voted_for = old_vote;
                return Err(error);
            }
        }

        Ok(RequestVoteReply {
            term: self.current_term,
            vote_granted,
        })
    }

    pub fn record_vote_response(
        &mut self,
        voter_id: NodeId,
        reply: RequestVoteReply,
    ) -> io::Result<bool> {
        if reply.term > self.current_term {
            self.step_down_for_higher_term(reply.term)?;
            return Ok(false);
        }

        if self.role != Role::Candidate || reply.term != self.current_term {
            return Ok(false);
        }

        if !reply.vote_granted || !self.config.is_voter(voter_id) {
            return Ok(false);
        }

        self.votes_received.insert(voter_id);

        if self.counted_votes() >= self.quorum_size() {
            self.become_leader()?;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn append_client_command(&mut self, command: Command) -> io::Result<u64> {
        if self.role != Role::Leader {
            return Err(io::Error::other("not leader"));
        }

        if matches!(command, Command::Config(_)) {
            return Err(io::Error::other(
                "membership changes go through propose_config_change",
            ));
        }

        let index = self.log.append(LogEntry {
            term: self.current_term,
            command,
        })?;

        self.advance_commit_from_progress();

        Ok(index)
    }

    pub fn propose_config_change(&mut self, change: ConfigChange) -> io::Result<u64> {
        if self.role != Role::Leader {
            return Err(io::Error::other("not leader"));
        }

        if self.has_pending_config() {
            return Err(io::Error::other(
                "a configuration change is already in progress",
            ));
        }

        if self.log.term_at(self.commit_index) != Some(self.current_term) {
            return Err(io::Error::other(
                "leader has not committed an entry in its term yet",
            ));
        }

        let new_config = change
            .apply(&self.config)
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;

        let index = self.log.append(LogEntry {
            term: self.current_term,
            command: Command::Config(new_config.clone()),
        })?;

        self.config = new_config;
        self.config_index = index;

        for peer in self.peers() {
            self.next_index.entry(peer).or_insert(1);
            self.match_index.entry(peer).or_insert(0);
        }

        self.advance_commit_from_progress();

        Ok(index)
    }

    fn refresh_config(&mut self) {
        let (index, config) = self.log.latest_config();

        self.config_index = index;
        self.config = config;
    }

    fn after_commit(&mut self) {
        if self.role != Role::Leader || self.has_pending_config() {
            return;
        }

        if !self.config.is_member(self.id) {
            self.role = Role::Follower;
            self.leader_id = None;
            self.votes_received.clear();
            self.next_index.clear();
            self.match_index.clear();
            return;
        }

        let members = self.config.members();
        self.next_index.retain(|peer, _| members.contains(peer));
        self.match_index.retain(|peer, _| members.contains(peer));
    }

    pub fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
    ) -> io::Result<AppendEntriesReply> {
        if args.term < self.current_term {
            return Ok(AppendEntriesReply {
                term: self.current_term,
                success: false,
            });
        }

        if args.term > self.current_term {
            self.step_down_for_higher_term(args.term)?;
        }

        self.role = Role::Follower;
        self.votes_received.clear();
        self.leader_id = Some(args.leader_id);

        let success = self.log.append_from_leader(
            args.prev_log_index,
            args.prev_log_term,
            &args.entries,
            self.commit_index,
        )?;

        if success && (!args.entries.is_empty()) {
            self.refresh_config();
        }

        if success {
            let last_new_index = u64::try_from(args.entries.len())
                .ok()
                .and_then(|count| args.prev_log_index.checked_add(count))
                .ok_or_else(|| io::Error::other("Raft log index overflow"))?;

            self.commit_index = self
                .commit_index
                .max(args.leader_commit.min(last_new_index));
        }

        self.after_commit();

        Ok(AppendEntriesReply {
            term: self.current_term,
            success,
        })
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    pub fn compaction_point(&self, threshold: u64) -> Option<(u64, u64)> {
        if threshold == 0 || self.last_applied < self.log.base_index() + threshold {
            return None;
        }

        let term = self.log.term_at(self.last_applied)?;

        Some((self.last_applied, term))
    }

    pub fn compact_log_to(&mut self, index: u64, term: u64) -> io::Result<()> {
        if index > self.last_applied {
            return Err(io::Error::other("cannot compact past the applied index"));
        }

        let config = self.log.config_as_of(index).1;

        self.log.compact_to(index, term, config)
    }

    pub fn needs_snapshot(&self, peer: NodeId) -> bool {
        self.role == Role::Leader
            && self
                .next_index
                .get(&peer)
                .is_some_and(|next| *next < self.log.first_index())
    }

    pub fn observe_leader(&mut self, term: u64, leader_id: NodeId) -> io::Result<bool> {
        if term < self.current_term {
            return Ok(false);
        }

        if term > self.current_term {
            self.step_down_for_higher_term(term)?;
        }

        self.role = Role::Follower;
        self.votes_received.clear();
        self.leader_id = Some(leader_id);

        Ok(true)
    }

    pub fn should_install_snapshot(&self, index: u64) -> bool {
        index > self.commit_index
    }

    pub fn finish_snapshot_install(
        &mut self,
        index: u64,
        term: u64,
        config: ClusterConfig,
    ) -> io::Result<()> {
        self.log.compact_to(index, term, config)?;
        self.refresh_config();

        self.commit_index = self.commit_index.max(index);
        self.last_applied = index;

        Ok(())
    }

    pub fn handle_install_snapshot_reply(
        &mut self,
        peer: NodeId,
        last_included_index: u64,
        sent_term: u64,
        reply_term: u64,
    ) -> io::Result<()> {
        if reply_term > self.current_term {
            return self.step_down_for_higher_term(reply_term);
        }

        if self.role != Role::Leader
            || sent_term != self.current_term
            || reply_term != self.current_term
            || !self.next_index.contains_key(&peer)
        {
            return Ok(());
        }

        let matched = self.match_index.entry(peer).or_insert(0);
        *matched = (*matched).max(last_included_index);
        let matched = *matched;

        let next = self.next_index.entry(peer).or_insert(1);
        *next = (*next).max(matched + 1);

        self.advance_commit_from_progress();

        Ok(())
    }

    pub fn peers(&self) -> Vec<NodeId> {
        self.config
            .members()
            .into_iter()
            .filter(|member| *member != self.id)
            .collect()
    }

    pub fn voter_peers(&self) -> Vec<NodeId> {
        self.config
            .voters
            .iter()
            .copied()
            .filter(|voter| *voter != self.id)
            .collect()
    }

    fn become_leader(&mut self) -> io::Result<()> {
        let next = self.log.last_index() + 1;

        self.log.append(LogEntry {
            term: self.current_term,
            command: Command::Noop,
        })?;

        self.next_index = self.peers().into_iter().map(|peer| (peer, next)).collect();
        self.match_index = self.peers().into_iter().map(|peer| (peer, 0)).collect();

        self.role = Role::Leader;
        self.leader_id = Some(self.id);

        self.advance_commit_from_progress();

        Ok(())
    }

    fn advance_commit_from_progress(&mut self) {
        let progress = self.match_index.clone();
        self.advance_commit_index(&progress);
    }

    pub fn build_append_entries(
        &self,
        peer: NodeId,
        max_entries: usize,
    ) -> Option<AppendEntriesArgs> {
        if self.role != Role::Leader {
            return None;
        }

        let next = *self.next_index.get(&peer)?;
        let prev_log_index = next.checked_sub(1)?;
        let prev_log_term = self.log.term_at(prev_log_index)?;

        let mut entries = self.log.entries_from(next);
        entries.truncate(max_entries);

        Some(AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        })
    }

    pub fn handle_append_entries_reply(
        &mut self,
        peer: NodeId,
        sent: &AppendEntriesArgs,
        reply: AppendEntriesReply,
    ) -> io::Result<()> {
        if reply.term > self.current_term {
            return self.step_down_for_higher_term(reply.term);
        }

        if self.role != Role::Leader
            || sent.term != self.current_term
            || reply.term != self.current_term
            || !self.next_index.contains_key(&peer)
        {
            return Ok(());
        }

        if reply.success {
            let replicated = sent
                .prev_log_index
                .checked_add(sent.entries.len() as u64)
                .ok_or_else(|| io::Error::other("Raft log index overflow"))?;

            let matched = self.match_index.entry(peer).or_insert(0);
            *matched = (*matched).max(replicated);
            let matched = *matched;

            let next = self.next_index.entry(peer).or_insert(1);
            *next = (*next).max(matched + 1);

            self.advance_commit_from_progress();
        } else if let Some(next) = self.next_index.get_mut(&peer) {
            *next = (*next).min(sent.prev_log_index).max(1);
        }

        Ok(())
    }

    pub fn entries_to_apply(&self) -> Vec<(u64, LogEntry)> {
        (self.last_applied + 1..=self.commit_index)
            .filter_map(|index| self.log.entry_at(index).map(|entry| (index, entry.clone())))
            .collect()
    }

    pub fn mark_applied(&mut self, index: u64) {
        self.last_applied = self.last_applied.max(index.min(self.commit_index));
    }

    pub fn advance_commit_index(&mut self, match_index: &HashMap<NodeId, u64>) {
        if self.role != Role::Leader {
            return;
        }

        let last_index = self.log.last_index();

        for index in (self.commit_index + 1..=last_index).rev() {
            if self.log.term_at(index) != Some(self.current_term) {
                continue;
            }

            let replicated_count = self
                .config
                .voters
                .iter()
                .filter(|member| {
                    if **member == self.id {
                        last_index >= index
                    } else {
                        match_index
                            .get(member)
                            .is_some_and(|replicated| *replicated >= index)
                    }
                })
                .count();

            if replicated_count >= self.quorum_size() {
                self.commit_index = index;
                break;
            }
        }

        self.after_commit();
    }
}
