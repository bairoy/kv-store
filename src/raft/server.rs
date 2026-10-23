//! Async Raft server: peer RPCs, election/heartbeat timers, client commands.
//!
//! `RaftNode` holds the pure protocol state; this module drives it with
//! timers and TCP, and applies committed entries to the `LsmStore`.
//!
//! Known simplification: one TCP connection per RPC. Reads are linearizable
//! (see `read_barrier`).
//!
//! One `RaftServer` is one Raft *group*. A stand-alone node (V9) runs a single
//! group; a sharded node (V10, `shard::node`) runs one per partition and
//! shares one TCP port between them, which is why every peer message is
//! wrapped in a group-id envelope.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, timeout};

use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Command as ClientCommand, Response, encode_response, parse_request};
use crate::raft::codec::invalid_data;
use crate::raft::node::RaftNode;
use crate::raft::rpc::{RaftMessage, decode_envelope, encode_envelope};
use crate::raft::snapshot::RaftSnapshot;
use crate::raft::txn::{self, PendingTxn, TxnEffect};
use crate::raft::types::{
    ClusterConfig, Command, ConfigChange, InstallSnapshotArgs, InstallSnapshotReply, NodeId, Role,
    TxnCommand,
};
use crate::storage::lsm::LsmStore;

const MAX_ENTRIES_PER_MESSAGE: usize = 64;
const MAX_REPLICATION_ROUNDS: usize = 256;
const TICK_INTERVAL: Duration = Duration::from_millis(20);
const APPLY_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_INCOMING_SNAPSHOT_BYTES: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Peer RPC address of every server that could ever be in this group
    /// (an address book). Who is actually *in* the group right now is decided
    /// by the node's membership config, which changes at runtime.
    pub peers: HashMap<NodeId, String>,
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub rpc_timeout: Duration,
    /// How long a client write waits for its entry to commit and apply.
    pub commit_timeout: Duration,
    /// Compact the log once this many applied entries follow the last
    /// snapshot. Zero disables compaction.
    pub snapshot_threshold: u64,
    /// Bytes of snapshot per InstallSnapshot message (must fit one frame).
    pub snapshot_chunk_size: usize,
    /// Confirm leadership with a majority before serving a read. Turning
    /// this off is faster but a deposed leader may return stale data.
    pub linearizable_reads: bool,
    /// Which Raft group this is when several share one TCP port (0 if alone).
    pub group_id: u32,
    /// Shown in log lines when several groups run in one process (e.g. `p3`).
    pub group_name: Option<String>,
}

impl RaftConfig {
    pub fn new(peers: HashMap<NodeId, String>) -> Self {
        Self {
            peers,
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(600),
            heartbeat_interval: Duration::from_millis(100),
            rpc_timeout: Duration::from_millis(200),
            commit_timeout: Duration::from_secs(3),
            snapshot_threshold: 1000,
            snapshot_chunk_size: 256 * 1024,
            linearizable_reads: true,
            group_id: 0,
            group_name: None,
        }
    }
}

/// What a server knows about one Raft group right now.
#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub role: Role,
    pub leader: Option<NodeId>,
    pub term: u64,
    pub commit_index: u64,
    pub config: ClusterConfig,
}

/// A snapshot being received chunk by chunk from the leader.
struct IncomingSnapshot {
    last_included_index: u64,
    last_included_term: u64,
    data: Vec<u8>,
}

pub struct RaftServer {
    id: NodeId,
    /// Prefix for log lines, e.g. `Raft 2` or `Raft 2/p3`.
    label: String,
    node: Mutex<RaftNode>,
    store: Mutex<LsmStore>,
    /// Serialises applying committed entries to the store.
    apply_lock: Mutex<()>,
    incoming_snapshot: Mutex<Option<IncomingSnapshot>>,
    config: RaftConfig,
    election_deadline: StdMutex<Instant>,
    /// When we last heard from a legitimate leader (for ignoring disruptive
    /// vote requests from servers that were removed from the group).
    last_leader_contact: StdMutex<Instant>,
    /// Set when this server stops hosting the group.
    stopped: AtomicBool,
    /// Transactions prepared here and not finished yet, with when we first
    /// saw them (kept in step with the store; used to recover stuck ones).
    pending_txns: StdMutex<HashMap<String, (PendingTxn, Instant)>>,
}

impl RaftServer {
    pub fn new(node: RaftNode, store: LsmStore, config: RaftConfig) -> Arc<Self> {
        let label = match &config.group_name {
            Some(name) => format!("Raft {}/{name}", node.id),
            None => format!("Raft {}", node.id),
        };

        // Transactions that were prepared before a restart still hold locks.
        let pending_txns = match txn::scan_pending(&store) {
            Ok(found) => found
                .into_iter()
                .map(|pending| (pending.id.clone(), (pending, Instant::now())))
                .collect(),
            Err(error) => {
                eprintln!("{label}: could not scan for prepared transactions: {error}");
                HashMap::new()
            }
        };

        let server = Arc::new(Self {
            id: node.id,
            label,
            node: Mutex::new(node),
            store: Mutex::new(store),
            apply_lock: Mutex::new(()),
            incoming_snapshot: Mutex::new(None),
            config,
            election_deadline: StdMutex::new(Instant::now()),
            last_leader_contact: StdMutex::new(Instant::now() - Duration::from_secs(3600)),
            stopped: AtomicBool::new(false),
            pending_txns: StdMutex::new(pending_txns),
        });

        server.reset_election_timer();
        server
    }

    /// Run the peer listener, client listener and timers until one fails.
    pub async fn run(self: Arc<Self>, peer_addr: &str, client_addr: &str) -> io::Result<()> {
        tokio::try_join!(
            self.clone().run_peer_server(peer_addr),
            self.clone().run_client_server(client_addr),
            self.clone().run_ticker(),
        )?;

        Ok(())
    }

    /// Stop hosting this group: the ticker exits and requests are refused.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    fn note_leader_contact(&self) {
        if let Ok(mut guard) = self.last_leader_contact.lock() {
            *guard = Instant::now();
        }
    }

    /// A vote request is ignored while we are the leader, or have heard from
    /// one recently: a server that was removed from the group (and does not
    /// know it) keeps timing out and must not be able to depose a healthy
    /// leader by sending ever-higher terms (Raft thesis, section 4.2.3).
    async fn ignores_vote_requests(&self) -> bool {
        let (role, leader) = self.leader_status().await;

        if role == Role::Leader {
            return true;
        }

        let recent = self
            .last_leader_contact
            .lock()
            .map(|contact| contact.elapsed() < self.config.election_timeout_min / 2)
            .unwrap_or(false);

        leader.is_some() && recent
    }

    /// Current role and best-known leader (for routing and status output).
    pub async fn leader_status(&self) -> (Role, Option<NodeId>) {
        let node = self.node.lock().await;

        (node.role, node.leader_id)
    }

    // ---------------------------------------------------------------- timers

    fn random_election_timeout(&self) -> Duration {
        let min = self.config.election_timeout_min.as_millis() as u64;
        let max = self.config.election_timeout_max.as_millis() as u64;
        let span = max.saturating_sub(min).max(1);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(0);

        // xorshift mixes the clock with the node id so peers rarely tie.
        let group = u64::from(self.config.group_id).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        let mut x = now ^ self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ group | 1;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;

        Duration::from_millis(min + x % span)
    }

    fn reset_election_timer(&self) {
        let deadline = Instant::now() + self.random_election_timeout();

        if let Ok(mut guard) = self.election_deadline.lock() {
            *guard = deadline;
        }
    }

    fn election_timer_expired(&self) -> bool {
        self.election_deadline
            .lock()
            .map(|deadline| Instant::now() >= *deadline)
            .unwrap_or(false)
    }

    pub async fn run_ticker(self: Arc<Self>) -> io::Result<()> {
        let mut last_heartbeat = Instant::now();
        let mut was_leader = false;

        loop {
            sleep(TICK_INTERVAL).await;

            if self.is_stopped() {
                return Ok(());
            }

            let is_leader = self.node.lock().await.role == Role::Leader;

            if is_leader {
                if last_heartbeat.elapsed() >= self.config.heartbeat_interval {
                    last_heartbeat = Instant::now();
                    self.broadcast_append().await;

                    // A leader without followers (a single-voter group) gets
                    // no replies that would trigger applying, so do it here.
                    if let Err(error) = self.apply_committed().await {
                        eprintln!("{}: apply failed: {error}", self.label);
                    }
                }
            } else {
                if was_leader {
                    // Just lost leadership: don't start an election instantly.
                    self.reset_election_timer();
                }

                if self.election_timer_expired() {
                    // Learners, joining servers and removed servers never
                    // campaign; just wait for a leader to contact them.
                    if self.node.lock().await.can_campaign() {
                        self.start_election().await;
                    } else {
                        self.reset_election_timer();
                    }
                }
            }

            was_leader = is_leader;
        }
    }

    // -------------------------------------------------------------- election

    async fn start_election(self: &Arc<Self>) {
        self.reset_election_timer();

        let (args, became_leader, peers) = {
            let mut node = self.node.lock().await;

            match node.start_election() {
                Ok(args) => (args, node.role == Role::Leader, node.voter_peers()),
                Err(error) => {
                    eprintln!("{}: cannot start election: {error}", self.label);
                    return;
                }
            }
        };

        println!("{}: starting election for term {}", self.label, args.term);

        if became_leader {
            println!("{}: became leader for term {}", self.label, args.term);
            self.broadcast_append().await;
            return;
        }

        for peer in peers {
            let server = Arc::clone(self);
            let args = args.clone();

            tokio::spawn(async move { server.request_vote(peer, args).await });
        }
    }

    async fn request_vote(
        self: Arc<Self>,
        peer: NodeId,
        args: crate::raft::types::RequestVoteArgs,
    ) {
        let term = args.term;

        let reply = match self.rpc(peer, &RaftMessage::RequestVote(args)).await {
            Ok(RaftMessage::RequestVoteReply(reply)) => reply,
            Ok(_) | Err(_) => return,
        };

        let became_leader = {
            let mut node = self.node.lock().await;

            match node.record_vote_response(peer, reply) {
                Ok(became_leader) => became_leader,
                Err(error) => {
                    eprintln!("{}: vote handling failed: {error}", self.label);
                    return;
                }
            }
        };

        if became_leader {
            println!("{}: became leader for term {term}", self.label);
            self.broadcast_append().await;
        }
    }

    // ----------------------------------------------------------- replication

    async fn broadcast_append(self: &Arc<Self>) {
        let peers = self.node.lock().await.peers();

        for peer in peers {
            let server = Arc::clone(self);

            tokio::spawn(async move { server.replicate_to(peer).await });
        }
    }

    /// Send entries (or a heartbeat) to `peer`, backing up on log mismatch
    /// and continuing while there is more to send.
    async fn replicate_to(self: Arc<Self>, peer: NodeId) {
        for _ in 0..MAX_REPLICATION_ROUNDS {
            let args = {
                let node = self.node.lock().await;

                if node.needs_snapshot(peer) {
                    None
                } else {
                    node.build_append_entries(peer, MAX_ENTRIES_PER_MESSAGE)
                }
            };

            let Some(args) = args else {
                // Either not the leader any more, or the peer needs entries
                // that were compacted away: send the snapshot instead.
                let needs_snapshot = self.node.lock().await.needs_snapshot(peer);

                if needs_snapshot && self.send_snapshot(peer).await {
                    continue;
                }

                return;
            };

            let reply = match self
                .rpc(peer, &RaftMessage::AppendEntries(args.clone()))
                .await
            {
                Ok(RaftMessage::AppendEntriesReply(reply)) => reply,
                Ok(_) | Err(_) => return,
            };

            let keep_going =
                reply.term == args.term && (!reply.success || !args.entries.is_empty());

            {
                let mut node = self.node.lock().await;

                if let Err(error) = node.handle_append_entries_reply(peer, &args, reply) {
                    eprintln!("{}: reply handling failed: {error}", self.label);
                    return;
                }
            }

            if let Err(error) = self.apply_committed().await {
                eprintln!("{}: apply failed: {error}", self.label);
                return;
            }

            if !keep_going {
                return;
            }
        }
    }

    /// Apply every committed-but-unapplied entry to the store, in order.
    async fn apply_committed(&self) -> io::Result<()> {
        let _apply_guard = self.apply_lock.lock().await;

        let pending = self.node.lock().await.entries_to_apply();

        let Some(last_index) = pending.last().map(|(index, _)| *index) else {
            return Ok(());
        };

        {
            let mut store = self.store.lock().await;

            for (_, entry) in pending {
                match entry.command {
                    Command::Set { key, value } => store.set(key, value)?,
                    Command::Delete { key } => {
                        store.delete(key)?;
                    }
                    // Membership is handled inside RaftNode; nothing to do
                    // to the key-value data.
                    Command::Noop | Command::Config(_) => {}
                    Command::Txn(step) => {
                        let effect = crate::raft::txn::apply(&mut store, &step)?;
                        self.note_txn_effect(effect);
                    }
                }
            }
        }

        self.node.lock().await.mark_applied(last_index);

        self.compact_if_needed().await
    }

    /// Snapshot the state machine and drop the covered log prefix once enough
    /// entries have piled up. Runs under `apply_lock`, so the store matches
    /// `last_applied` exactly while it is read.
    async fn compact_if_needed(&self) -> io::Result<()> {
        let point = {
            let node = self.node.lock().await;
            node.compaction_point(self.config.snapshot_threshold)
        };

        let Some((index, term)) = point else {
            return Ok(());
        };

        let entries = self.store.lock().await.snapshot_entries()?;

        let config = self.node.lock().await.config_as_of(index);

        let snapshot = RaftSnapshot {
            last_included_index: index,
            last_included_term: term,
            config,
            entries,
        };

        let mut node = self.node.lock().await;

        // The snapshot must be durable before any log entry is discarded.
        snapshot.write_atomic(node.snapshot_path())?;
        node.compact_log_to(index, term)?;

        println!(
            "{}: snapshot at index {index}; log now starts at {}",
            self.label,
            node.log.first_index()
        );

        Ok(())
    }

    // -------------------------------------------------------------- snapshots

    /// Leader: stream our latest snapshot to `peer`. Returns true if the peer
    /// acknowledged all of it.
    async fn send_snapshot(&self, peer: NodeId) -> bool {
        match self.try_send_snapshot(peer).await {
            Ok(sent) => sent,
            Err(error) => {
                eprintln!("{}: snapshot to node {peer} failed: {error}", self.label);
                false
            }
        }
    }

    async fn try_send_snapshot(&self, peer: NodeId) -> io::Result<bool> {
        let (term, path) = {
            let node = self.node.lock().await;

            if node.role != Role::Leader {
                return Ok(false);
            }

            (node.current_term, node.snapshot_path().to_path_buf())
        };

        let snapshot = RaftSnapshot::read(&path)?;
        let bytes = snapshot.encode()?;
        let chunk_size = self.config.snapshot_chunk_size.max(1);

        let mut offset = 0usize;
        let mut reply_term = term;

        while offset < bytes.len() {
            let end = (offset + chunk_size).min(bytes.len());

            let message = RaftMessage::InstallSnapshot(InstallSnapshotArgs {
                term,
                leader_id: self.id,
                last_included_index: snapshot.last_included_index,
                last_included_term: snapshot.last_included_term,
                offset: offset as u64,
                data: bytes[offset..end].to_vec(),
                done: end == bytes.len(),
            });

            reply_term = match self.rpc(peer, &message).await? {
                RaftMessage::InstallSnapshotReply(reply) => reply.term,
                _ => return Err(invalid_data("unexpected reply to InstallSnapshot")),
            };

            if reply_term != term {
                break;
            }

            // Only announce once the peer is reachable and took a chunk, so a
            // down follower doesn't spam the log on every heartbeat.
            if offset == 0 {
                println!(
                    "{}: sending snapshot (index {}, {} bytes) to node {peer}",
                    self.label,
                    snapshot.last_included_index,
                    bytes.len()
                );
            }

            offset = end;
        }

        let mut node = self.node.lock().await;

        node.handle_install_snapshot_reply(peer, snapshot.last_included_index, term, reply_term)?;

        Ok(reply_term == term)
    }

    /// Follower: accept one snapshot chunk; install the snapshot on the last.
    async fn handle_install_snapshot(
        &self,
        args: InstallSnapshotArgs,
    ) -> io::Result<InstallSnapshotReply> {
        let term = {
            let mut node = self.node.lock().await;
            let current = node.observe_leader(args.term, args.leader_id)?;

            if !current {
                return Ok(InstallSnapshotReply {
                    term: node.current_term,
                });
            }

            node.current_term
        };

        self.reset_election_timer();
        self.note_leader_contact();

        let finished = {
            let mut incoming = self.incoming_snapshot.lock().await;

            if args.offset == 0 {
                *incoming = Some(IncomingSnapshot {
                    last_included_index: args.last_included_index,
                    last_included_term: args.last_included_term,
                    data: Vec::new(),
                });
            }

            let Some(current) = incoming.as_mut() else {
                return Err(invalid_data("snapshot chunk without a start"));
            };

            let in_order = current.last_included_index == args.last_included_index
                && current.last_included_term == args.last_included_term
                && current.data.len() as u64 == args.offset
                && current.data.len() + args.data.len() <= MAX_INCOMING_SNAPSHOT_BYTES;

            if !in_order {
                *incoming = None;
                return Err(invalid_data("snapshot chunk out of order"));
            }

            current.data.extend_from_slice(&args.data);

            if args.done { incoming.take() } else { None }
        };

        if let Some(snapshot) = finished {
            self.install_received_snapshot(snapshot).await?;
        }

        Ok(InstallSnapshotReply { term })
    }

    /// Save the snapshot, replace the state machine, then drop the log prefix.
    /// If we crash part-way the leader simply sends the snapshot again.
    async fn install_received_snapshot(&self, received: IncomingSnapshot) -> io::Result<()> {
        let snapshot = RaftSnapshot::decode(&received.data)?;

        if snapshot.last_included_index != received.last_included_index
            || snapshot.last_included_term != received.last_included_term
        {
            return Err(invalid_data("snapshot header does not match its chunks"));
        }

        let _apply_guard = self.apply_lock.lock().await;

        let path = {
            let node = self.node.lock().await;

            if !node.should_install_snapshot(snapshot.last_included_index) {
                return Ok(());
            }

            node.snapshot_path().to_path_buf()
        };

        snapshot.write_atomic(&path)?;

        self.store
            .lock()
            .await
            .install_snapshot(&snapshot.entries, snapshot.last_included_index)?;

        self.node.lock().await.finish_snapshot_install(
            snapshot.last_included_index,
            snapshot.last_included_term,
            snapshot.config.clone(),
        )?;

        self.rebuild_pending_transactions().await;

        println!(
            "{}: installed snapshot through index {}",
            self.label, snapshot.last_included_index
        );

        Ok(())
    }

    // ------------------------------------------------------------- peer RPCs

    async fn rpc(&self, peer: NodeId, message: &RaftMessage) -> io::Result<RaftMessage> {
        let address = self
            .config
            .peers
            .get(&peer)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "unknown peer"))?;

        let payload = encode_envelope(self.config.group_id, message)?;
        let frame = encode_frame(&payload)?;

        let exchange = async {
            let mut stream = TcpStream::connect(address).await?;
            stream.write_all(&frame).await?;

            let mut decoder = FrameDecoder::new();

            read_frame(&mut stream, &mut decoder)
                .await?
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed"))
        };

        let reply = timeout(self.config.rpc_timeout, exchange)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Raft RPC timed out"))??;

        let (group_id, message) = decode_envelope(&reply)?;

        if group_id != self.config.group_id {
            return Err(invalid_data("reply is from a different Raft group"));
        }

        Ok(message)
    }

    pub async fn run_peer_server(self: Arc<Self>, address: &str) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let server = Arc::clone(&self);

            tokio::spawn(async move {
                if let Err(error) = server.handle_peer(stream).await
                    && error.kind() != io::ErrorKind::UnexpectedEof
                {
                    eprintln!("{}: peer connection error: {error}", server.label);
                }
            });
        }
    }

    async fn handle_peer(&self, mut stream: TcpStream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();

        while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {
            let (group_id, message) = decode_envelope(&payload)?;

            if group_id != self.config.group_id {
                return Err(invalid_data("message is for a different Raft group"));
            }

            let reply = self.handle_peer_message(message).await?;
            let frame = encode_frame(&encode_envelope(group_id, &reply)?)?;

            stream.write_all(&frame).await?;
        }

        Ok(())
    }

    pub async fn handle_peer_message(&self, message: RaftMessage) -> io::Result<RaftMessage> {
        if self.is_stopped() {
            return Err(invalid_data("this server no longer hosts the Raft group"));
        }

        match message {
            RaftMessage::RequestVote(args) => {
                if self.ignores_vote_requests().await {
                    let term = self.node.lock().await.current_term;

                    return Ok(RaftMessage::RequestVoteReply(
                        crate::raft::types::RequestVoteReply {
                            term,
                            vote_granted: false,
                        },
                    ));
                }

                let reply = self.node.lock().await.handle_vote_request(args)?;

                if reply.vote_granted {
                    self.reset_election_timer();
                }

                Ok(RaftMessage::RequestVoteReply(reply))
            }

            RaftMessage::AppendEntries(args) => {
                let term = args.term;
                let reply = self.node.lock().await.handle_append_entries(args)?;

                // Only a request from a current-or-newer leader resets the timer.
                if reply.term == term {
                    self.reset_election_timer();
                    self.note_leader_contact();
                }

                self.apply_committed().await?;

                Ok(RaftMessage::AppendEntriesReply(reply))
            }

            RaftMessage::InstallSnapshot(args) => Ok(RaftMessage::InstallSnapshotReply(
                self.handle_install_snapshot(args).await?,
            )),

            _ => Err(invalid_data("unexpected Raft message from peer")),
        }
    }

    // --------------------------------------------------------------- clients

    pub async fn run_client_server(self: Arc<Self>, address: &str) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let server = Arc::clone(&self);

            tokio::spawn(async move {
                if let Err(error) = server.handle_client(stream).await {
                    eprintln!("{}: client connection error: {error}", server.label);
                }
            });
        }
    }

    async fn handle_client(self: &Arc<Self>, mut stream: TcpStream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();

        while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {
            let request = std::str::from_utf8(&payload)
                .map_err(|_| invalid_data("client request is not UTF-8"))?;

            let response = match parse_request(request) {
                Some(command) => self.execute_command(command).await,
                None => Response::Error("invalid command".to_string()),
            };

            let frame = encode_frame(encode_response(response).as_bytes())?;

            stream.write_all(&frame).await?;
        }

        Ok(())
    }

    pub async fn execute_command(self: &Arc<Self>, command: ClientCommand) -> Response {
        let key = match &command {
            ClientCommand::Get(key) | ClientCommand::Set(key, _) | ClientCommand::Delete(key) => {
                key
            }
        };

        if txn::is_reserved(key) {
            return Response::Error("that key is reserved for transactions".to_string());
        }

        match command {
            ClientCommand::Get(key) => {
                if let Err(response) = self.read_barrier().await {
                    return response;
                }

                match self.store.lock().await.get(&key) {
                    Some(value) => Response::Value(value),
                    None => Response::NotFound,
                }
            }

            ClientCommand::Set(key, value) => {
                self.replicate_write(Command::Set { key, value }).await
            }

            ClientCommand::Delete(key) => {
                // The existence check is a read, so it needs the same barrier.
                if let Err(response) = self.read_barrier().await {
                    return response;
                }

                if self.store.lock().await.get(&key).is_none() {
                    return Response::NotFound;
                }

                self.replicate_write(Command::Delete { key }).await
            }
        }
    }

    /// Make a local read linearizable (Raft's "ReadIndex"):
    ///
    /// 1. we must be leader and have committed an entry from our own term,
    ///    otherwise our commit index may lag the true one;
    /// 2. a majority must still acknowledge us as leader right now, so no
    ///    newer leader can exist;
    /// 3. the store must have applied everything committed when we started.
    async fn read_barrier(self: &Arc<Self>) -> Result<(), Response> {
        let (term, read_index, quorum, peers, self_votes) = {
            let node = self.node.lock().await;

            if node.role != Role::Leader {
                return Err(not_leader_response(&node));
            }

            if !self.config.linearizable_reads {
                return Ok(());
            }

            if node.log.term_at(node.commit_index) != Some(node.current_term) {
                return Err(Response::Error(
                    "leader has not committed an entry in its term yet; retry".to_string(),
                ));
            }

            (
                node.current_term,
                node.commit_index,
                node.quorum_size(),
                node.voter_peers(),
                usize::from(node.is_voter()),
            )
        };

        // Only voters count (never learners); we count ourselves if we are one.
        let mut acknowledgements = self_votes;

        if acknowledgements < quorum {
            let tasks: Vec<_> = peers
                .into_iter()
                .map(|peer| {
                    let server = Arc::clone(self);

                    tokio::spawn(async move { server.heartbeat_acknowledged(peer, term).await })
                })
                .collect();

            for task in tasks {
                if let Ok(true) = task.await {
                    acknowledgements += 1;
                }
            }
        }

        if acknowledgements < quorum {
            return Err(Response::Error(
                "could not confirm leadership with a majority".to_string(),
            ));
        }

        let deadline = Instant::now() + self.config.commit_timeout;

        loop {
            {
                let node = self.node.lock().await;

                if node.role != Role::Leader || node.current_term != term {
                    return Err(not_leader_response(&node));
                }

                if node.last_applied >= read_index {
                    return Ok(());
                }
            }

            if Instant::now() >= deadline {
                return Err(Response::Error(
                    "timed out waiting for the store to catch up".to_string(),
                ));
            }

            sleep(APPLY_POLL_INTERVAL).await;
        }
    }

    /// Send `peer` a heartbeat; true if it recognises us as leader of `term`.
    async fn heartbeat_acknowledged(&self, peer: NodeId, term: u64) -> bool {
        let args = {
            let node = self.node.lock().await;

            if node.needs_snapshot(peer) {
                return false;
            }

            node.build_append_entries(peer, 0)
        };

        let Some(args) = args else {
            return false;
        };

        let reply = match self
            .rpc(peer, &RaftMessage::AppendEntries(args.clone()))
            .await
        {
            Ok(RaftMessage::AppendEntriesReply(reply)) => reply,
            Ok(_) | Err(_) => return false,
        };

        // Even a log-mismatch rejection acknowledges our leadership, as long
        // as the peer is in the same term.
        let acknowledged = reply.term == term;

        let mut node = self.node.lock().await;

        if let Err(error) = node.handle_append_entries_reply(peer, &args, reply) {
            eprintln!("{}: reply handling failed: {error}", self.label);
        }

        acknowledged
    }

    // ----------------------------------------------------------- membership

    /// Leader: propose one membership change and wait until it is committed.
    pub async fn change_membership(self: &Arc<Self>, change: ConfigChange) -> Result<(), String> {
        let (index, term) = {
            let mut node = self.node.lock().await;

            if node.role != Role::Leader {
                return Err(match node.leader_id {
                    Some(leader) => format!("not leader; leader is node {leader}"),
                    None => "not leader; no leader elected yet".to_string(),
                });
            }

            let index = node
                .propose_config_change(change)
                .map_err(|error| error.to_string())?;

            (index, node.current_term)
        };

        self.broadcast_append().await;

        if let Err(error) = self.apply_committed().await {
            return Err(error.to_string());
        }

        let deadline = Instant::now() + self.config.commit_timeout;

        loop {
            {
                let node = self.node.lock().await;

                // A leader that removed itself steps down once the change is
                // committed, so check the log, not the role.
                if node.commit_index >= index {
                    return if node.log.term_at(index) == Some(term) {
                        Ok(())
                    } else {
                        Err("the change was overwritten by a new leader".to_string())
                    };
                }

                if node.current_term != term {
                    return Err("leadership changed before the change committed".to_string());
                }
            }

            if Instant::now() >= deadline {
                return Err("timed out waiting for the change to commit".to_string());
            }

            sleep(APPLY_POLL_INTERVAL).await;
        }
    }

    /// Leader: how many committed entries `peer` still lacks.
    pub async fn replication_lag(&self, peer: NodeId) -> Option<u64> {
        let node = self.node.lock().await;

        if node.role != Role::Leader {
            return None;
        }

        node.match_index_of(peer)
            .map(|matched| node.commit_index.saturating_sub(matched))
    }

    /// A snapshot of this group's state for status output and for the
    /// rebalancer (which has to find the leader and see the membership).
    pub async fn info(&self) -> GroupInfo {
        let node = self.node.lock().await;

        GroupInfo {
            role: node.role,
            leader: node.leader_id,
            term: node.current_term,
            commit_index: node.commit_index,
            config: node.config().clone(),
        }
    }

    /// Read straight from this server's own store, with no leadership check.
    /// For diagnostics and tests only: it can be stale on a follower.
    pub async fn local_get(&self, key: &str) -> Option<String> {
        self.store.lock().await.get(key)
    }

    /// The membership this server currently believes in.
    pub async fn membership(&self) -> ClusterConfig {
        self.node.lock().await.config().clone()
    }

    /// Append to the log, replicate, and wait until the entry is committed
    /// and applied locally.
    async fn replicate_write(self: &Arc<Self>, command: Command) -> Response {
        match self.append_and_wait(command).await {
            Ok(()) => Response::Ok,
            Err(message) => Response::Error(message),
        }
    }

    /// Append `command` as leader, replicate it, and wait until it has been
    /// committed *and applied* here.
    async fn append_and_wait(self: &Arc<Self>, command: Command) -> Result<(), String> {
        let (index, term) = {
            let mut node = self.node.lock().await;

            if node.role != Role::Leader {
                return Err(not_leader_message(&node));
            }

            match node.append_client_command(command) {
                Ok(index) => (index, node.current_term),
                Err(error) => return Err(error.to_string()),
            }
        };

        self.broadcast_append().await;

        // The entry may already be committed (single-voter group).
        if let Err(error) = self.apply_committed().await {
            return Err(error.to_string());
        }

        let deadline = Instant::now() + self.config.commit_timeout;

        loop {
            {
                let node = self.node.lock().await;

                if node.role != Role::Leader || node.current_term != term {
                    return Err(
                        "leadership changed before the write committed; outcome unknown"
                            .to_string(),
                    );
                }

                if node.last_applied >= index {
                    return Ok(());
                }
            }

            if Instant::now() >= deadline {
                return Err("timed out waiting for a majority".to_string());
            }

            sleep(APPLY_POLL_INTERVAL).await;
        }
    }

    // ------------------------------------------------------- transactions

    /// Leader: replicate one transaction step and return what applying it
    /// produced (the vote, the winning decision, or `OK`).
    pub async fn txn_command(self: &Arc<Self>, step: TxnCommand) -> Result<String, String> {
        self.append_and_wait(Command::Txn(step.clone())).await?;

        Ok(txn::outcome(&*self.store.lock().await, &step))
    }

    /// Leader: the recorded decision for `id` (this must be the coordinator
    /// partition), read linearizably. `None` means no decision yet.
    pub async fn txn_decision(self: &Arc<Self>, id: &str) -> Result<Option<String>, String> {
        self.read_barrier()
            .await
            .map_err(|response| match response {
                Response::Error(message) => message,
                _ => "could not confirm leadership".to_string(),
            })?;

        Ok(txn::decision(&*self.store.lock().await, id))
    }

    /// Transactions prepared here and not finished, with how long ago we
    /// first saw each.
    pub fn pending_transactions(&self) -> Vec<(PendingTxn, Duration)> {
        self.pending_txns
            .lock()
            .map(|pending| {
                pending
                    .values()
                    .map(|(txn, since)| (txn.clone(), since.elapsed()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn note_txn_effect(&self, effect: TxnEffect) {
        let Ok(mut pending) = self.pending_txns.lock() else {
            return;
        };

        match effect {
            TxnEffect::Nothing => {}
            TxnEffect::Prepared(txn) => {
                pending
                    .entry(txn.id.clone())
                    .or_insert_with(|| (txn, Instant::now()));
            }
            TxnEffect::Finished(id) => {
                pending.remove(&id);
            }
        }
    }

    /// After a snapshot replaced the store, re-read which transactions are
    /// in flight (applying never ran for the entries the snapshot covered).
    async fn rebuild_pending_transactions(&self) {
        let found = match txn::scan_pending(&*self.store.lock().await) {
            Ok(found) => found,
            Err(error) => {
                eprintln!("{}: could not rescan transactions: {error}", self.label);
                return;
            }
        };

        if let Ok(mut pending) = self.pending_txns.lock() {
            let now = Instant::now();
            let mut rebuilt = HashMap::new();

            for txn in found {
                let since = pending.get(&txn.id).map_or(now, |(_, since)| *since);
                rebuilt.insert(txn.id.clone(), (txn, since));
            }

            *pending = rebuilt;
        }
    }
}

/// Read one complete frame, or `None` if the peer closed the connection.
pub(crate) async fn read_frame(
    stream: &mut TcpStream,
    decoder: &mut FrameDecoder,
) -> io::Result<Option<Vec<u8>>> {
    let mut buffer = [0u8; 4096];

    loop {
        if let Some(frame) = decoder.next_frame()? {
            return Ok(Some(frame));
        }

        let count = stream.read(&mut buffer).await?;

        if count == 0 {
            return Ok(None);
        }

        decoder.push_bytes(&buffer[..count]);
    }
}

fn not_leader_message(node: &RaftNode) -> String {
    match node.leader_id {
        Some(leader) => format!("not leader; leader is node {leader}"),
        None => "not leader; no leader elected yet".to_string(),
    }
}

fn not_leader_response(node: &RaftNode) -> Response {
    Response::Error(not_leader_message(node))
}
