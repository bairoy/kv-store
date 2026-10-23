//! One server of a sharded cluster ("multi-Raft").
//!
//! A `ShardNode` hosts a Raft group for **every partition it holds a replica
//! of**, each with its own log, term, leader, timers and `LsmStore`. All the
//! groups share two TCP ports:
//!
//! * the **peer port**: Raft messages from other servers, tagged with the id
//!   of the group (= partition) they belong to;
//! * the **client port**: the key-value protocol. Any server accepts any
//!   command. It hashes the key to a partition, executes locally if it leads
//!   that partition, and otherwise *forwards* the command to the server that
//!   does, so clients never need to know the layout.
//!
//! ```text
//! client ──SET k v──► server A ──partition_of(k)──► hosted here, I lead? ──yes──► Raft group p
//!                                     │                         │no
//!                                     │not hosted               └─► forward to the leader
//!                                     └─► forward to a replica of p ─► (it forwards to its leader)
//! ```
//!
//! Extra client commands handled here:
//!
//! | command | meaning |
//! |---|---|
//! | `ROUTE <key>` | where does this key live? |
//! | `STATUS` | which partitions does this server hold, and who leads them? |
//! | `REBALANCE <ids>` | move partitions so exactly these servers hold data (see `rebalance.rs`) |
//! | `TXN SET a 1 ; SET b 2 ; DELETE c` | write several keys atomically, even across partitions (see `txn.rs`) |
//!
//! and the *local* admin commands the rebalancer sends to individual servers
//! (they are never forwarded): `GROUPINFO <p>`, `GROUPOP <p> <op> [n]`,
//! `HOST <p>`, `UNHOST <p>`, `TXNOP <p> ...`, `GETTABLE`, `SETTABLE <table>`.
//!
//! Forwarded commands travel as `FORWARD <hops-left> <epoch> <command>`. The
//! epoch must match the receiver's routing table. On a mismatch the two
//! servers compare tables and the older one adopts the newer, so a server
//! that missed an update heals itself the next time it talks to a peer.

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Command as ClientCommand, Response, encode_response, parse_request};
use crate::raft::codec::invalid_data;
use crate::raft::fsutil::write_file_atomic;
use crate::raft::node::RaftNode;
use crate::raft::rpc::{decode_envelope, encode_envelope};
use crate::raft::server::{RaftConfig, RaftServer, read_frame};
use crate::raft::types::{ConfigChange, NodeId, Role};
use crate::shard::routing::{PartitionId, RoutingTable};
use crate::storage::lsm::LsmStore;

/// How many servers a command may pass through before it must be executed.
/// Client -> any server -> a replica -> that partition's leader needs 2.
const INITIAL_HOPS: u8 = 2;
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ServerAddress {
    /// Where this server accepts Raft messages from other servers.
    pub peer: String,
    /// Where this server accepts client commands.
    pub client: String,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// This server's id.
    pub id: NodeId,
    /// The routing table to start from. A newer table saved on disk wins.
    pub table: RoutingTable,
    /// Every server that may ever take part, including this one and servers
    /// that are not hosting anything yet.
    pub servers: HashMap<NodeId, ServerAddress>,
    /// Data lives in `<data_dir>/node<id>/p<partition>/`.
    pub data_dir: PathBuf,
    /// Override how often each group snapshots (None = Raft default).
    pub snapshot_threshold: Option<u64>,
    /// Virtual nodes per server on the placement ring (must match the value
    /// the initial table was built with, or a rebalance moves too much).
    pub vnodes: u32,
    /// Pause between partition moves: rebalancing competes with live traffic
    /// for bandwidth, so it is deliberately slowed down.
    pub rebalance_pause: Duration,
    /// A transaction prepared this long ago without a verdict is considered
    /// abandoned; the recovery loop resolves it.
    pub txn_timeout: Duration,
}

pub struct ShardNode {
    pub(crate) id: NodeId,
    /// The current routing table; replaced (never mutated) when it changes.
    table: RwLock<Arc<RoutingTable>>,
    pub(crate) servers: HashMap<NodeId, ServerAddress>,
    /// One Raft group per partition this server currently hosts.
    groups: RwLock<HashMap<PartitionId, Arc<RaftServer>>>,
    node_dir: PathBuf,
    snapshot_threshold: Option<u64>,
    pub(crate) vnodes: u32,
    pub(crate) rebalance_pause: Duration,
    /// Only one rebalance may run at a time.
    pub(crate) rebalance_lock: tokio::sync::Mutex<()>,
    pub(crate) txn_timeout: Duration,
    pub(crate) txn_counter: std::sync::atomic::AtomicU64,
}

impl ShardNode {
    /// Open (or recover) a Raft group and store for every hosted partition.
    ///
    /// Hosted = the partitions the table assigns to this server, plus any
    /// partition whose data directory already exists here (a move that was
    /// in progress when the server last stopped).
    pub fn open(config: ShardConfig) -> io::Result<Arc<Self>> {
        let ShardConfig {
            id,
            table,
            servers,
            data_dir,
            snapshot_threshold,
            vnodes,
            rebalance_pause,
            txn_timeout,
        } = config;

        if !servers.contains_key(&id) {
            return Err(invalid_input("this server is not in the server list"));
        }

        let node_dir = data_dir.join(format!("node{id}"));
        std::fs::create_dir_all(&node_dir)?;

        // A table saved by an earlier run is newer than the startup one if a
        // rebalance happened since.
        let table = match load_table(&node_dir)? {
            Some(saved) if saved.epoch >= table.epoch => saved,
            _ => table,
        };

        let mut hosted: BTreeSet<PartitionId> = table.hosted_by(id).into_iter().collect();

        for entry in std::fs::read_dir(&node_dir)? {
            let entry = entry?;

            let partition = entry
                .file_name()
                .to_str()
                .and_then(|name| name.strip_prefix('p'))
                .and_then(|number| number.parse::<PartitionId>().ok());

            if let Some(partition) = partition
                && entry.path().is_dir()
                && partition < table.partition_count()
            {
                hosted.insert(partition);
            }
        }

        let mut groups = HashMap::new();

        for partition in hosted {
            let group = open_group(
                &node_dir,
                id,
                partition,
                &table,
                &servers,
                snapshot_threshold,
            )?;
            groups.insert(partition, group);
        }

        Ok(Arc::new(Self {
            id,
            table: RwLock::new(Arc::new(table)),
            servers,
            groups: RwLock::new(groups),
            node_dir,
            snapshot_threshold,
            vnodes,
            rebalance_pause,
            rebalance_lock: tokio::sync::Mutex::new(()),
            txn_timeout,
            txn_counter: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// This server's id.
    pub fn id(&self) -> NodeId {
        self.id
    }

    /// A key read straight from this server's copy of a partition (no
    /// routing, no leadership check). For diagnostics and tests.
    pub async fn local_get(&self, partition: PartitionId, key: &str) -> Option<String> {
        self.group(partition)?.local_get(key).await
    }

    /// The routing table in force right now.
    pub fn table(&self) -> Arc<RoutingTable> {
        Arc::clone(&self.table.read().unwrap_or_else(PoisonError::into_inner))
    }

    pub(crate) fn group(&self, partition: PartitionId) -> Option<Arc<RaftServer>> {
        self.groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&partition)
            .cloned()
    }

    /// The partitions this server holds, in order.
    pub fn hosted_partitions(&self) -> Vec<PartitionId> {
        let mut partitions: Vec<_> = self
            .groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .keys()
            .copied()
            .collect();

        partitions.sort_unstable();
        partitions
    }

    /// Run every group's timers plus the two listeners until one fails.
    pub async fn run(self: Arc<Self>) -> io::Result<()> {
        let address = self
            .servers
            .get(&self.id)
            .cloned()
            .ok_or_else(|| invalid_input("this server is not in the server list"))?;

        let groups: Vec<_> = self
            .groups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .cloned()
            .collect();

        for group in groups {
            tokio::spawn(group.run_ticker());
        }

        tokio::spawn(Arc::clone(&self).run_txn_recovery());

        tokio::try_join!(
            Arc::clone(&self).run_peer_server(&address.peer),
            Arc::clone(&self).run_client_server(&address.client),
        )?;

        Ok(())
    }

    // ------------------------------------------------------ table handling

    /// Install `newer` if it really is newer, and remember it on disk.
    pub(crate) fn adopt_table(&self, newer: RoutingTable) -> io::Result<bool> {
        let mut current = self.table.write().unwrap_or_else(PoisonError::into_inner);

        if newer.epoch <= current.epoch || newer.partition_count() != current.partition_count() {
            return Ok(false);
        }

        write_file_atomic(
            &self.node_dir.join("routing.table"),
            newer.encode().as_bytes(),
        )?;

        *current = Arc::new(newer);

        Ok(true)
    }

    /// Compare tables with the server at `address`; the older side adopts the
    /// newer table. This is how a server that missed an update catches up.
    async fn sync_table_with(&self, address: &str) {
        let Ok(Ok(response)) = timeout(ADMIN_TIMEOUT, send_request(address, "GETTABLE")).await
        else {
            return;
        };

        let Some(text) = decode_value(&response) else {
            return;
        };

        let Ok(theirs) = RoutingTable::decode(&text) else {
            return;
        };

        let ours = self.table();

        if theirs.epoch > ours.epoch {
            let _ = self.adopt_table(theirs);
        } else if theirs.epoch < ours.epoch {
            let _ = timeout(
                ADMIN_TIMEOUT,
                send_request(address, &format!("SETTABLE {}", ours.encode())),
            )
            .await;
        }
    }

    // ------------------------------------------------- hosting partitions

    /// Start hosting a partition (as a joining member unless the table says
    /// this server is already a replica). Used by the rebalancer.
    fn host_partition(&self, partition: PartitionId) -> Result<(), String> {
        let table = self.table();

        if partition >= table.partition_count() {
            return Err(format!("partition {partition} does not exist"));
        }

        if self.group(partition).is_some() {
            return Ok(());
        }

        let group = open_group(
            &self.node_dir,
            self.id,
            partition,
            &table,
            &self.servers,
            self.snapshot_threshold,
        )
        .map_err(|error| error.to_string())?;

        self.groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(partition, Arc::clone(&group));

        tokio::spawn(group.run_ticker());

        Ok(())
    }

    /// Stop hosting a partition and set its data aside (never deleted, so a
    /// mistaken removal can be recovered by hand).
    fn unhost_partition(&self, partition: PartitionId) -> Result<(), String> {
        let removed = self
            .groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&partition);

        let Some(group) = removed else {
            return Ok(());
        };

        group.stop();

        let directory = self.node_dir.join(format!("p{partition}"));

        if directory.exists() {
            let mut archive = self
                .node_dir
                .join(format!("p{partition}.removed-e{}", self.table().epoch));

            if archive.exists() {
                archive = self.node_dir.join(format!(
                    "p{partition}.removed-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|elapsed| elapsed.as_millis())
                        .unwrap_or(0)
                ));
            }

            std::fs::rename(&directory, &archive).map_err(|error| error.to_string())?;
        }

        Ok(())
    }

    // ------------------------------------------------------------ peer side

    async fn run_peer_server(self: Arc<Self>, address: &str) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let node = Arc::clone(&self);

            tokio::spawn(async move {
                if let Err(error) = node.handle_peer(stream).await
                    && error.kind() != io::ErrorKind::UnexpectedEof
                {
                    eprintln!("Shard {}: peer connection error: {error}", node.id);
                }
            });
        }
    }

    /// Read group-tagged Raft messages and hand each to its group.
    async fn handle_peer(&self, mut stream: TcpStream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();

        while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {
            let (group_id, message) = decode_envelope(&payload)?;

            let group = self.group(group_id).ok_or_else(|| {
                invalid_data("message for a Raft group this server does not host")
            })?;

            let reply = group.handle_peer_message(message).await?;
            let frame = encode_frame(&encode_envelope(group_id, &reply)?)?;

            stream.write_all(&frame).await?;
        }

        Ok(())
    }

    // ---------------------------------------------------------- client side

    async fn run_client_server(self: Arc<Self>, address: &str) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;

        loop {
            let (stream, _) = listener.accept().await?;
            let node = Arc::clone(&self);

            tokio::spawn(async move {
                if let Err(error) = node.handle_client(stream).await {
                    eprintln!("Shard {}: client connection error: {error}", node.id);
                }
            });
        }
    }

    async fn handle_client(&self, mut stream: TcpStream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();

        while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {
            let request = std::str::from_utf8(&payload)
                .map_err(|_| invalid_data("client request is not UTF-8"))?;

            let response = self.process(request).await;

            stream
                .write_all(&encode_frame(response.as_bytes())?)
                .await?;
        }

        Ok(())
    }

    /// Turn one request line into one encoded response.
    pub async fn process(&self, request: &str) -> String {
        if let Some(servers) = request.strip_prefix("REBALANCE ") {
            return self.rebalance(servers).await;
        }

        if let Some(operations) = request.strip_prefix("TXN ") {
            return self.transaction(operations).await;
        }

        self.process_local(request).await
    }

    /// Everything except `REBALANCE` (which itself sends these to servers).
    pub(crate) async fn process_local(&self, request: &str) -> String {
        let request = request.trim_end();
        let (verb, rest) = request.split_once(' ').unwrap_or((request, ""));

        match verb {
            "FORWARD" => self.process_forwarded(rest).await,
            "ROUTE" => self.describe_route(rest.trim()),
            "STATUS" => self.status().await,
            "GROUPINFO" => self.group_info(rest.trim()).await,
            "GROUPOP" => self.group_operation(rest.trim()).await,
            "HOST" => self.admin_partition(rest.trim(), |node, p| node.host_partition(p)),
            "UNHOST" => self.admin_partition(rest.trim(), |node, p| node.unhost_partition(p)),
            "TXNOP" => self.txn_operation(rest.trim()).await,
            "GETTABLE" => encode_response(Response::Value(self.table().encode())),
            "SETTABLE" => self.set_table(rest),
            _ => self.route(request, INITIAL_HOPS).await,
        }
    }

    /// `FORWARD <hops-left> <epoch> <command>`
    async fn process_forwarded(&self, rest: &str) -> String {
        let mut parts = rest.splitn(3, ' ');

        let (Some(hops), Some(epoch), Some(command)) = (parts.next(), parts.next(), parts.next())
        else {
            return error_response("malformed FORWARD");
        };

        let (Ok(hops), Ok(epoch)) = (hops.parse::<u8>(), epoch.parse::<u64>()) else {
            return error_response("malformed FORWARD");
        };

        let ours = self.table().epoch;

        if epoch != ours {
            return error_response(format!(
                "STALE_EPOCH: sender is at epoch {epoch}, this server is at epoch {ours}"
            ));
        }

        self.route(command, hops).await
    }

    /// Execute a key-value command here, or send it where it belongs.
    async fn route(&self, request: &str, hops: u8) -> String {
        let Some(command) = parse_request(request) else {
            return error_response("invalid command");
        };

        let table = self.table();
        let partition = table.partition_of(command_key(&command));

        let Some(group) = self.group(partition) else {
            // Not a replica of this partition: pass it to one that is.
            let replicas = table.replicas(partition).to_vec();
            return self.forward(&replicas, request, hops, partition).await;
        };

        let (role, leader) = group.leader_status().await;

        if role == Role::Leader {
            return encode_response(group.execute_command(command).await);
        }

        match leader {
            Some(leader) if leader != self.id => {
                self.forward(&[leader], request, hops, partition).await
            }
            _ => error_response(format!(
                "partition {partition}: no leader elected yet, retry shortly"
            )),
        }
    }

    /// Send `request` to the first reachable server in `targets`. If the
    /// other side reports a different routing-table epoch, sync tables and
    /// try once more.
    async fn forward(
        &self,
        targets: &[NodeId],
        request: &str,
        hops: u8,
        partition: PartitionId,
    ) -> String {
        if hops == 0 {
            return error_response(format!("partition {partition}: too many redirects"));
        }

        let mut last_error = "no other replica to ask".to_string();

        for target in targets.iter().filter(|target| **target != self.id) {
            let Some(address) = self.servers.get(target) else {
                continue;
            };

            for attempt in 0..2 {
                let payload = format!("FORWARD {} {} {request}", hops - 1, self.table().epoch);

                match timeout(FORWARD_TIMEOUT, send_request(&address.client, &payload)).await {
                    Ok(Ok(response)) => {
                        if attempt == 0 && response.contains("STALE_EPOCH") {
                            self.sync_table_with(&address.client).await;
                            continue;
                        }

                        return response;
                    }
                    Ok(Err(error)) => last_error = format!("node {target}: {error}"),
                    Err(_) => last_error = format!("node {target}: timed out"),
                }

                break;
            }
        }

        error_response(format!("partition {partition} unavailable ({last_error})"))
    }

    // ------------------------------------------------------- introspection

    /// `ROUTE <key>`: which partition and servers hold this key?
    fn describe_route(&self, key: &str) -> String {
        let table = self.table();
        let partition = table.partition_of(key);

        encode_response(Response::Value(format!(
            "key={key} partition={partition} epoch={} replicas={:?} hosted_here={}",
            table.epoch,
            table.replicas(partition),
            self.group(partition).is_some(),
        )))
    }

    /// `STATUS`: the role of this server in each partition it hosts.
    async fn status(&self) -> String {
        let table = self.table();

        let mut lines = vec![format!(
            "server {} | epoch {} | {} partitions total | hosting {}",
            self.id,
            table.epoch,
            table.partition_count(),
            self.hosted_partitions().len()
        )];

        for partition in self.hosted_partitions() {
            let Some(group) = self.group(partition) else {
                continue;
            };

            let info = group.info().await;
            let leader = info
                .leader
                .map_or("?".to_string(), |leader| leader.to_string());

            lines.push(format!(
                "  p{partition}: {:<9} leader={leader} voters={:?} learners={:?}",
                format!("{:?}", info.role),
                info.config.voters,
                info.config.learners,
            ));
        }

        encode_response(Response::Value(lines.join("\n")))
    }

    // --------------------------------------------------------- admin (local)

    /// `GROUPINFO <p>`: `role=Leader leader=1 term=3 commit=14 voters=1,2,3 learners=-`
    async fn group_info(&self, partition: &str) -> String {
        let Ok(partition) = partition.parse::<PartitionId>() else {
            return error_response("usage: GROUPINFO <partition>");
        };

        let Some(group) = self.group(partition) else {
            return encode_response(Response::Value("not-hosted".to_string()));
        };

        let info = group.info().await;
        let list = |ids: &[NodeId]| {
            if ids.is_empty() {
                "-".to_string()
            } else {
                ids.iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            }
        };

        encode_response(Response::Value(format!(
            "role={:?} leader={} term={} commit={} voters={} learners={}",
            info.role,
            info.leader
                .map_or("-".to_string(), |leader| leader.to_string()),
            info.term,
            info.commit_index,
            list(&info.config.voters),
            list(&info.config.learners),
        )))
    }

    /// `GROUPOP <p> ADD_LEARNER|PROMOTE|REMOVE|LAG <server>`: leader only.
    async fn group_operation(&self, rest: &str) -> String {
        let mut parts = rest.split_whitespace();

        let (Some(partition), Some(operation), Some(server)) =
            (parts.next(), parts.next(), parts.next())
        else {
            return error_response("usage: GROUPOP <partition> <operation> <server>");
        };

        let (Ok(partition), Ok(server)) =
            (partition.parse::<PartitionId>(), server.parse::<NodeId>())
        else {
            return error_response("usage: GROUPOP <partition> <operation> <server>");
        };

        let Some(group) = self.group(partition) else {
            return error_response(format!("partition {partition} is not hosted here"));
        };

        let change = match operation {
            "ADD_LEARNER" => ConfigChange::AddLearner(server),
            "PROMOTE" => ConfigChange::Promote(server),
            "REMOVE" => ConfigChange::Remove(server),
            "LAG" => {
                return match group.replication_lag(server).await {
                    Some(lag) => encode_response(Response::Value(lag.to_string())),
                    None => error_response("not the leader, or the server is not a member"),
                };
            }
            other => return error_response(format!("unknown operation {other}")),
        };

        match group.change_membership(change).await {
            Ok(()) => encode_response(Response::Ok),
            Err(message) => error_response(message),
        }
    }

    fn admin_partition(
        &self,
        argument: &str,
        action: impl FnOnce(&Self, PartitionId) -> Result<(), String>,
    ) -> String {
        match argument.parse::<PartitionId>() {
            Ok(partition) => match action(self, partition) {
                Ok(()) => encode_response(Response::Ok),
                Err(message) => error_response(message),
            },
            Err(_) => error_response("usage: <HOST|UNHOST> <partition>"),
        }
    }

    /// `SETTABLE <table>`: adopt a newer routing table.
    fn set_table(&self, text: &str) -> String {
        let newer = match RoutingTable::decode(text) {
            Ok(table) => table,
            Err(message) => return error_response(format!("bad table: {message}")),
        };

        let ours = self.table();

        match self.adopt_table(newer.clone()) {
            Ok(true) => encode_response(Response::Ok),
            Ok(false) if newer == *ours => encode_response(Response::Ok),
            Ok(false) => error_response(format!(
                "STALE_EPOCH: offered epoch {}, this server is at epoch {}",
                newer.epoch, ours.epoch
            )),
            Err(error) => error_response(error.to_string()),
        }
    }

    /// Send one local admin command to `target` (this server included).
    pub(crate) async fn ask_server(&self, target: NodeId, command: &str) -> Result<String, String> {
        if target == self.id {
            return Ok(self.process_local(command).await);
        }

        let address = self
            .servers
            .get(&target)
            .ok_or_else(|| format!("server {target} is not in the server list"))?;

        match timeout(ADMIN_TIMEOUT, send_request(&address.client, command)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(format!("server {target}: {error}")),
            Err(_) => Err(format!("server {target}: timed out")),
        }
    }
}

/// Open the Raft group and store for one partition in `node_dir/p<n>/`.
fn open_group(
    node_dir: &Path,
    id: NodeId,
    partition: PartitionId,
    table: &RoutingTable,
    servers: &HashMap<NodeId, ServerAddress>,
    snapshot_threshold: Option<u64>,
) -> io::Result<Arc<RaftServer>> {
    let directory = node_dir.join(format!("p{partition}"));
    std::fs::create_dir_all(&directory)?;

    let store = LsmStore::open(directory.join("sstables"), directory.join("wal.log"))?;
    let log_path = directory.join("raft.log");

    // A server the table lists as a replica starts from that membership; any
    // other server joins empty and learns the membership from the leader. A
    // log that already holds config entries overrides both.
    let replicas = table.replicas(partition);

    let node = if replicas.contains(&id) {
        RaftNode::open(id, replicas.to_vec(), &log_path)?
    } else {
        RaftNode::open_joining(id, &log_path)?
    };

    // Address book of every server: who is actually in the group is decided
    // by the group's own (changing) membership, not by this map.
    let peers = servers
        .iter()
        .map(|(server, address)| (*server, address.peer.clone()))
        .collect();

    let mut raft_config = RaftConfig::new(peers);
    raft_config.group_id = partition;
    raft_config.group_name = Some(format!("p{partition}"));

    if let Some(threshold) = snapshot_threshold {
        raft_config.snapshot_threshold = threshold;
    }

    Ok(RaftServer::new(node, store, raft_config))
}

fn load_table(node_dir: &Path) -> io::Result<Option<RoutingTable>> {
    match std::fs::read_to_string(node_dir.join("routing.table")) {
        Ok(text) => RoutingTable::decode(&text)
            .map(Some)
            .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn command_key(command: &ClientCommand) -> &str {
    match command {
        ClientCommand::Set(key, _) | ClientCommand::Get(key) | ClientCommand::Delete(key) => key,
    }
}

pub(crate) fn error_response(message: impl Into<String>) -> String {
    encode_response(Response::Error(message.into()))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// The text of an encoded `Response::Value` (`$<len>\r\n<text>\r\n`).
pub(crate) fn decode_value(response: &str) -> Option<String> {
    let body = response.strip_prefix('$')?;
    let (length, rest) = body.split_once("\r\n")?;

    if length == "-1" {
        return None;
    }

    Some(rest.strip_suffix("\r\n").unwrap_or(rest).to_string())
}

/// One request, one response, on a fresh connection to a server's client port.
pub(crate) async fn send_request(address: &str, request: &str) -> io::Result<String> {
    let mut stream = TcpStream::connect(address).await?;
    stream.write_all(&encode_frame(request.as_bytes())?).await?;

    let mut decoder = FrameDecoder::new();

    let payload = read_frame(&mut stream, &mut decoder)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))?;

    String::from_utf8(payload).map_err(|_| invalid_data("response is not UTF-8"))
}
