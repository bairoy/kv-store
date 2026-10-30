//! One server of a sharded cluster ("multi-Raft").

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::time::timeout;

use crate::metrics;
use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Response, encode_response};
use crate::net::request::{self, Session, split_session};
use crate::net::security::{Security, Stream};
use crate::raft::codec::invalid_data;
use crate::raft::fsutil::write_file_atomic;
use crate::raft::node::RaftNode;
use crate::raft::rpc::{decode_envelope, encode_envelope};
use crate::raft::server::{RaftConfig, RaftServer, read_frame, reject_busy};
use crate::raft::types::{ConfigChange, NodeId, Role};
use crate::shard::routing::{PartitionId, RoutingTable};
use crate::storage::lsm::LsmStore;
use tracing::Instrument;

const INITIAL_HOPS: u8 = 2;
const FORWARD_TIMEOUT: Duration = Duration::from_secs(5);
const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ServerAddress {
    pub peer: String,
    pub client: String,
}

#[derive(Debug, Clone)]
pub struct ShardConfig {
    pub id: NodeId,
    pub table: RoutingTable,
    pub servers: HashMap<NodeId, ServerAddress>,
    pub data_dir: PathBuf,
    pub snapshot_threshold: Option<u64>,
    pub vnodes: u32,
    pub rebalance_pause: Duration,
    pub txn_timeout: Duration,
    pub max_connections: usize,
    pub max_pending_writes: usize,
    pub bind: Option<ServerAddress>,
    pub security: Arc<Security>,
}

pub struct ShardNode {
    pub(crate) id: NodeId,
    table: RwLock<Arc<RoutingTable>>,
    pub(crate) servers: HashMap<NodeId, ServerAddress>,
    groups: RwLock<HashMap<PartitionId, Arc<RaftServer>>>,
    node_dir: PathBuf,
    snapshot_threshold: Option<u64>,
    max_pending_writes: usize,
    pub(crate) vnodes: u32,
    pub(crate) rebalance_pause: Duration,
    pub(crate) rebalance_lock: tokio::sync::Mutex<()>,
    pub(crate) txn_timeout: Duration,
    pub(crate) txn_counter: std::sync::atomic::AtomicU64,
    max_connections: usize,
    bind: ServerAddress,
    pub(crate) security: Arc<Security>,
}

impl ShardNode {
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
            max_connections,
            max_pending_writes,
            bind,
            security,
        } = config;

        if !servers.contains_key(&id) {
            return Err(invalid_input("this server is not in the server list"));
        }

        let bind = bind.unwrap_or_else(|| servers[&id].clone());
        let node_dir = data_dir.join(format!("node{id}"));
        std::fs::create_dir_all(&node_dir)?;

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
                max_pending_writes,
                &security,
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
            max_pending_writes,
            vnodes,
            rebalance_pause,
            rebalance_lock: tokio::sync::Mutex::new(()),
            txn_timeout,
            txn_counter: std::sync::atomic::AtomicU64::new(0),
            max_connections,
            bind,
            security,
        }))
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub async fn local_get(&self, partition: PartitionId, key: &str) -> Option<String> {
        self.group(partition)?.local_get(key).await
    }

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

    pub async fn run(self: Arc<Self>) -> io::Result<()> {
        let address = self.bind.clone();

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

    async fn sync_table_with(&self, address: &str) {
        let Ok(Ok(response)) = timeout(
            ADMIN_TIMEOUT,
            send_request_with(&self.security, address, "GETTABLE"),
        )
        .await
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
                send_request_with(
                    &self.security,
                    address,
                    &format!("SETTABLE {}", ours.encode()),
                ),
            )
            .await;
        }
    }

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
            self.max_pending_writes,
            &self.security,
        )
        .map_err(|error| error.to_string())?;

        self.groups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(partition, Arc::clone(&group));

        tokio::spawn(group.run_ticker());

        Ok(())
    }

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

    async fn run_peer_server(self: Arc<Self>, address: &str) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;

        loop {
            let (tcp, _) = listener.accept().await?;
            let node = Arc::clone(&self);

            tokio::spawn(async move {
                let stream = match node.security.accept(tcp).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::warn!("Shard {}: peer handshake failed: {error}", node.id);
                        return;
                    }
                };

                if let Err(error) = node.handle_peer(stream).await
                    && error.kind() != io::ErrorKind::UnexpectedEof
                {
                    tracing::warn!("Shard {}: peer connection error: {error}", node.id);
                }
            });
        }
    }

    async fn handle_peer(&self, mut stream: Stream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();
        let mut authenticated = false;

        while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {
            if self.security.peer_gate(&mut authenticated, &payload)? {
                continue;
            }

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

    async fn run_client_server(self: Arc<Self>, address: &str) -> io::Result<()> {
        let listener = TcpListener::bind(address).await?;
        let slots = Arc::new(tokio::sync::Semaphore::new(self.max_connections));

        loop {
            let (tcp, _) = listener.accept().await?;

            let Ok(permit) = Arc::clone(&slots).try_acquire_owned() else {
                metrics::inc("connections_rejected_total", &[]);

                if !self.security.tls_enabled() {
                    tokio::spawn(reject_busy(tcp, "too many connections"));
                }

                continue;
            };

            let node = Arc::clone(&self);

            tokio::spawn(async move {
                let _permit = permit;
                metrics::inc("connections_accepted_total", &[]);

                let stream = match node.security.accept(tcp).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        tracing::warn!("Shard {}: client handshake failed: {error}", node.id);
                        return;
                    }
                };

                if let Err(error) = node.handle_client(stream).await {
                    tracing::warn!("Shard {}: client connection error: {error}", node.id);
                }
            });
        }
    }

    async fn handle_client(&self, mut stream: Stream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();
        let mut authenticated = false;

        while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {
            let request = std::str::from_utf8(&payload)
                .map_err(|_| invalid_data("client request is not UTF-8"))?;

            if let Some(reply) = self.security.client_gate(&mut authenticated, request).await {
                stream.write_all(&encode_frame(reply.as_bytes())?).await?;
                continue;
            }

            let span = request_span(self.id, request_session(request).as_ref());
            let started = std::time::Instant::now();

            let response = self.process(request).instrument(span).await;

            record_response(&response, started.elapsed());

            stream
                .write_all(&encode_frame(response.as_bytes())?)
                .await?;
        }

        Ok(())
    }

    pub async fn process(&self, request: &str) -> String {
        if let Some(servers) = request.strip_prefix("REBALANCE ") {
            return self.rebalance(servers).await;
        }

        if let Some(operations) = request.strip_prefix("TXN ") {
            return self.transaction(operations, None).await;
        }

        if let Ok(Some((session, rest))) = split_session(request)
            && let Some(operations) = rest.strip_prefix("TXN ")
        {
            return self.transaction(operations, Some(&session)).await;
        }

        self.process_local(request).await
    }

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

    async fn route(&self, request: &str, hops: u8) -> String {
        let command = match request::parse(request) {
            Ok(command) => command,
            Err(message) => return error_response(message),
        };

        let table = self.table();
        let partition = table.partition_of(command.key());

        tracing::debug!("server {}: routing to partition {partition}", self.id);

        let Some(group) = self.group(partition) else {
            let replicas = table.replicas(partition).to_vec();
            return self.forward(&replicas, request, hops, partition).await;
        };

        let (role, leader) = group.leader_status().await;

        if role == Role::Leader {
            return encode_response(group.execute_request(command).await);
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

            tracing::debug!("server {}: forwarding to server {target}", self.id);

            for attempt in 0..2 {
                let payload = format!("FORWARD {} {} {request}", hops - 1, self.table().epoch);

                match timeout(
                    FORWARD_TIMEOUT,
                    send_request_with(&self.security, &address.client, &payload),
                )
                .await
                {
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

    pub(crate) async fn ask_server(&self, target: NodeId, command: &str) -> Result<String, String> {
        if target == self.id {
            return Ok(self.process_local(command).await);
        }

        let address = self
            .servers
            .get(&target)
            .ok_or_else(|| format!("server {target} is not in the server list"))?;

        match timeout(
            ADMIN_TIMEOUT,
            send_request_with(&self.security, &address.client, command),
        )
        .await
        {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(format!("server {target}: {error}")),
            Err(_) => Err(format!("server {target}: timed out")),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn open_group(
    node_dir: &Path,
    id: NodeId,
    partition: PartitionId,
    table: &RoutingTable,
    servers: &HashMap<NodeId, ServerAddress>,
    snapshot_threshold: Option<u64>,
    max_pending_writes: usize,
    security: &Arc<Security>,
) -> io::Result<Arc<RaftServer>> {
    let directory = node_dir.join(format!("p{partition}"));
    std::fs::create_dir_all(&directory)?;

    let store = LsmStore::open(directory.join("sstables"), directory.join("wal.log"))?;
    let log_path = directory.join("raft.log");

    let replicas = table.replicas(partition);

    let node = if replicas.contains(&id) {
        RaftNode::open(id, replicas.to_vec(), &log_path)?
    } else {
        RaftNode::open_joining(id, &log_path)?
    };

    let peers = servers
        .iter()
        .map(|(server, address)| (*server, address.peer.clone()))
        .collect();

    let mut raft_config = RaftConfig::new(peers);
    raft_config.group_id = partition;
    raft_config.group_name = Some(format!("p{partition}"));
    raft_config.max_pending_writes = max_pending_writes;
    raft_config.security = Arc::clone(security);

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

pub(crate) fn error_response(message: impl Into<String>) -> String {
    encode_response(Response::Error(message.into()))
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(crate) fn decode_value(response: &str) -> Option<String> {
    let body = response.strip_prefix('$')?;
    let (length, rest) = body.split_once("\r\n")?;

    if length == "-1" {
        return None;
    }

    Some(rest.strip_suffix("\r\n").unwrap_or(rest).to_string())
}

#[cfg(test)]
pub(crate) async fn send_request(address: &str, request: &str) -> io::Result<String> {
    send_request_with(&Security::plain(), address, request).await
}

pub(crate) async fn send_request_with(
    security: &Security,
    address: &str,
    request: &str,
) -> io::Result<String> {
    let mut stream = security.connect_client(address).await?;
    stream.write_all(&encode_frame(request.as_bytes())?).await?;

    let mut decoder = FrameDecoder::new();

    let payload = read_frame(&mut stream, &mut decoder)
        .await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))?;

    String::from_utf8(payload).map_err(|_| invalid_data("response is not UTF-8"))
}

fn request_session(request: &str) -> Option<Session> {
    let request = request.trim_end();

    let inner = match request.strip_prefix("FORWARD ") {
        Some(rest) => rest.splitn(3, ' ').nth(2)?,
        None => request,
    };

    split_session(inner)
        .ok()
        .flatten()
        .map(|(session, _)| session)
}

fn request_span(server: NodeId, session: Option<&Session>) -> tracing::Span {
    crate::raft::server::request_span(server, session)
}

fn record_response(encoded: &str, elapsed: Duration) {
    let outcome = if encoded.starts_with("-ERR BUSY") {
        "busy"
    } else if encoded.contains("not leader") {
        "not_leader"
    } else if encoded.starts_with("-ERR") {
        "error"
    } else if encoded.starts_with("$-1") {
        "not_found"
    } else {
        "ok"
    };

    metrics::inc("requests_total", &[("outcome", outcome)]);
    metrics::observe_duration("request_seconds", &[("outcome", outcome)], elapsed);
}
