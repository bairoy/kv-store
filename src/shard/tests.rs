//! End-to-end tests of the sharded cluster: real servers on localhost
//! sharing nothing but TCP.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::raft::server::RaftServer;
use crate::raft::types::{NodeId, Role, TxnCommand, TxnOp};
use crate::shard::node::{ServerAddress, ShardConfig, ShardNode, send_request};
use crate::shard::ring::ConsistentHashRing;
use crate::shard::routing::{PartitionId, RoutingTable};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir()
        .join("kv_store_shard_tests")
        .join(format!("{name}_{}_{id}", std::process::id()))
}

/// Bind port 0 to let the OS pick a free port, then release it.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("local addr")
        .port()
}

/// `initial` servers form the starting table; `spares` can be added later.
fn cluster_config(
    name: &str,
    initial: &[NodeId],
    spares: &[NodeId],
    partitions: u32,
    replication: usize,
) -> (Vec<ShardConfig>, PathBuf) {
    let mut ring = ConsistentHashRing::new(100);
    for server in initial {
        ring.add_node(*server);
    }

    let table = RoutingTable::build(&ring, partitions, replication, 1).expect("table");

    let everyone: Vec<NodeId> = initial.iter().chain(spares).copied().collect();

    let addresses: HashMap<NodeId, ServerAddress> = everyone
        .iter()
        .map(|id| {
            (
                *id,
                ServerAddress {
                    peer: format!("127.0.0.1:{}", free_port()),
                    client: format!("127.0.0.1:{}", free_port()),
                },
            )
        })
        .collect();

    let data_dir = temp_dir(name);

    let configs = everyone
        .iter()
        .map(|id| ShardConfig {
            id: *id,
            table: table.clone(),
            servers: addresses.clone(),
            data_dir: data_dir.clone(),
            snapshot_threshold: None,
            vnodes: 100,
            rebalance_pause: Duration::from_millis(5),
            txn_timeout: Duration::from_millis(600),
        })
        .collect();

    (configs, data_dir)
}

/// Give every server the same transaction-recovery timeout.
fn with_txn_timeout(mut configs: Vec<ShardConfig>, timeout: Duration) -> Vec<ShardConfig> {
    for config in &mut configs {
        config.txn_timeout = timeout;
    }

    configs
}

async fn ask(address: &str, request: &str) -> String {
    send_request(address, request).await.expect("request")
}

/// Retry until the cluster has elected leaders and the command succeeds.
async fn ask_until_ok(address: &str, request: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let response = match send_request(address, request).await {
            Ok(response) => response,
            Err(error) => format!("-ERR {error}"),
        };

        if !response.starts_with("-ERR") || Instant::now() >= deadline {
            return response;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn bulk(value: &str) -> String {
    format!("${}\r\n{value}\r\n", value.len())
}

/// A running cluster: handles to every server plus where to reach them.
struct Cluster {
    nodes: Vec<Arc<ShardNode>>,
    clients: Vec<String>,
    data_dir: PathBuf,
    tasks: Vec<tokio::task::JoinHandle<std::io::Result<()>>>,
}

impl Cluster {
    fn start(configs: Vec<ShardConfig>, data_dir: PathBuf) -> Self {
        let clients = configs
            .iter()
            .map(|config| config.servers[&config.id].client.clone())
            .collect();

        let mut nodes = Vec::new();
        let mut tasks = Vec::new();

        for config in configs {
            let node = ShardNode::open(config).expect("open");
            tasks.push(tokio::spawn(Arc::clone(&node).run()));
            nodes.push(node);
        }

        Self {
            nodes,
            clients,
            data_dir,
            tasks,
        }
    }

    /// Index of the server with this id.
    fn at(&self, id: NodeId) -> usize {
        self.nodes
            .iter()
            .position(|node| node.id() == id)
            .expect("server exists")
    }

    async fn write_keys(&self, prefix: &str, range: std::ops::Range<usize>) {
        for i in range {
            let entry = &self.clients[i % self.clients.len()];
            let response = ask_until_ok(entry, &format!("SET {prefix}{i} value-{i}")).await;

            assert_eq!(response, "+OK", "SET {prefix}{i} via {entry}");
        }
    }

    /// Every key must be readable, with the right value, through every server.
    async fn assert_keys(&self, prefix: &str, range: std::ops::Range<usize>, via: &[NodeId]) {
        for i in range {
            for id in via {
                let entry = &self.clients[self.at(*id)];
                let response = ask_until_ok(entry, &format!("GET {prefix}{i}")).await;

                assert_eq!(
                    response,
                    bulk(&format!("value-{i}")),
                    "GET {prefix}{i} via server {id}"
                );
            }
        }
    }
}

impl Cluster {
    /// Every server the *current* table lists as a replica of a key's
    /// partition must hold that key in its own store. This is what proves the
    /// data really moved, not merely that requests get routed.
    async fn assert_every_replica_holds(&self, prefix: &str, range: std::ops::Range<usize>) {
        let table = self.nodes[0].table();

        for i in range {
            let key = format!("{prefix}{i}");
            let partition = table.partition_of(&key);

            for server in table.replicas(partition) {
                let node = &self.nodes[self.at(*server)];
                let deadline = Instant::now() + Duration::from_secs(10);

                loop {
                    // A follower applies a commit one heartbeat after the
                    // leader does, so give it a moment.
                    if node.local_get(partition, &key).await == Some(format!("value-{i}")) {
                        break;
                    }

                    assert!(
                        Instant::now() < deadline,
                        "server {server} (a replica of partition {partition}) lacks {key}"
                    );

                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

impl Cluster {
    /// For every partition, every replica the table lists must report the
    /// same Raft membership: voters == the table's replicas, no learners.
    async fn assert_membership_matches_table(&self) {
        let table = self.nodes[0].table();

        for info in table.partitions() {
            let mut expected = info.replicas.clone();
            expected.sort_unstable();
            let expected = expected
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",");

            for server in &info.replicas {
                let client = &self.clients[self.at(*server)];
                let deadline = Instant::now() + Duration::from_secs(10);

                loop {
                    let response = ask(client, &format!("GROUPINFO {}", info.id)).await;
                    let text = crate::shard::node::decode_value(&response).unwrap_or_default();

                    if text.contains(&format!("voters={expected} learners=-")) {
                        break;
                    }

                    assert!(
                        Instant::now() < deadline,
                        "server {server} sees partition {} as {text:?}, table says voters {expected}",
                        info.id
                    );

                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }

        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

#[tokio::test]
async fn introspection_and_forward_validation_work_without_a_cluster() {
    let (mut configs, data_dir) = cluster_config("introspect", &[1, 2, 3], &[], 4, 2);
    let node = ShardNode::open(configs.remove(0)).expect("open");

    // ROUTE names the partition and replicas without touching Raft.
    let route = node.process("ROUTE user:1").await;
    assert!(route.contains("partition="));
    assert!(route.contains("epoch=1"));

    // STATUS lists exactly the hosted partitions.
    let status = node.process("STATUS").await;
    for partition in node.hosted_partitions() {
        assert!(status.contains(&format!("p{partition}:")));
    }

    // Bad input is rejected, never executed.
    assert!(
        node.process("BOGUS")
            .await
            .starts_with("-ERR invalid command")
    );
    assert!(node.process("FORWARD nonsense").await.contains("malformed"));
    assert!(
        node.process("FORWARD 1 99 GET x")
            .await
            .contains("STALE_EPOCH")
    );

    // With no hops left a command must not be forwarded again.
    let exhausted = node.process("FORWARD 0 1 GET x").await;
    assert!(
        exhausted.contains("too many redirects") || exhausted.contains("no leader"),
        "unexpected response: {exhausted}"
    );

    // Admin commands validate their arguments.
    assert!(node.process("GROUPINFO x").await.starts_with("-ERR"));
    assert!(node.process("GROUPOP 0 FLY 2").await.starts_with("-ERR"));
    assert!(
        node.process("HOST 99").await.starts_with("-ERR"),
        "no such partition"
    );
    assert!(node.process("SETTABLE garbage").await.starts_with("-ERR"));
    assert!(node.process("REBALANCE a,b").await.starts_with("-ERR"));
    assert!(node.process("REBALANCE 1,2,99").await.starts_with("-ERR"));

    let _ = std::fs::remove_dir_all(data_dir);
}

#[tokio::test]
async fn each_server_hosts_exactly_the_partitions_the_table_assigns_it() {
    let (configs, data_dir) = cluster_config("hosting", &[1, 2, 3, 4], &[], 8, 3);

    for config in configs {
        let id = config.id;
        let expected = config.table.hosted_by(id);
        let node = ShardNode::open(config).expect("open");

        assert_eq!(node.hosted_partitions(), expected);
    }

    let _ = std::fs::remove_dir_all(data_dir);
}

/// Three real servers on localhost, several partitions, replication factor 2:
/// every server holds only some partitions, so commands sent to *any* server
/// must be routed to the right leader and the data must be readable through
/// any other server.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sharded_cluster_routes_every_key_to_its_partition() {
    let (configs, data_dir) = cluster_config("cluster", &[1, 2, 3], &[], 4, 2);
    let table = configs[0].table.clone();
    let cluster = Cluster::start(configs, data_dir);

    cluster.write_keys("user:", 0..40).await;
    cluster.assert_keys("user:", 0..40, &[1, 2, 3]).await;

    // Keys really are spread over several partitions.
    let used: std::collections::BTreeSet<_> = (0..40)
        .map(|i| table.partition_of(&format!("user:{i}")))
        .collect();
    assert!(
        used.len() >= 3,
        "keys should spread over partitions: {used:?}"
    );

    // DELETE works through a server that may not even host the partition.
    assert_eq!(
        ask_until_ok(&cluster.clients[2], "DELETE user:0").await,
        "+OK"
    );
    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET user:0").await,
        "$-1\r\n"
    );
}

/// A server that missed a table update must not misroute: it is told it is
/// stale, the two servers compare tables, and the older one adopts the newer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_with_a_stale_routing_table_is_healed_on_contact() {
    let (configs, data_dir) = cluster_config("stale_epoch", &[1, 2, 3], &[], 4, 2);
    let table = configs[0].table.clone();
    let cluster = Cluster::start(configs, data_dir);

    // Warm up: make sure leaders exist.
    cluster.write_keys("warm:", 0..8).await;

    // Server 1 learns of epoch 2 (same placement); servers 2 and 3 do not.
    let newer = table.with_partition(0, table.replicas(0).to_vec(), 2);
    assert_eq!(
        ask(&cluster.clients[0], &format!("SETTABLE {}", newer.encode())).await,
        "+OK"
    );
    assert_eq!(cluster.nodes[0].table().epoch, 2);
    assert_eq!(cluster.nodes[1].table().epoch, 1);

    // Traffic through server 1 for partitions it does not host is forwarded
    // with epoch 2, rejected by the others, and healed.
    cluster.write_keys("after:", 0..30).await;
    cluster.assert_keys("after:", 0..30, &[1, 2, 3]).await;

    for node in &cluster.nodes {
        assert_eq!(
            node.table().epoch,
            2,
            "every server must converge to epoch 2"
        );
    }
}

/// The headline V10 feature: add a server and later remove another, with
/// every partition's data moving through Raft membership changes, while no
/// key is ever lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adding_and_removing_servers_moves_partitions_without_losing_data() {
    // Servers 1-3 start with every partition (replication 3); 4 is a spare.
    let (configs, data_dir) = cluster_config("rebalance", &[1, 2, 3], &[4], 4, 3);
    let cluster = Cluster::start(configs, data_dir);

    cluster.write_keys("k", 0..30).await;
    assert!(
        cluster.nodes[3].hosted_partitions().is_empty(),
        "the spare holds nothing yet"
    );

    // ---- add server 4 ----------------------------------------------------
    let response = tokio::time::timeout(
        Duration::from_secs(180),
        send_request(&cluster.clients[0], "REBALANCE 1,2,3,4"),
    )
    .await
    .expect("rebalance finished in time")
    .expect("request");

    assert!(
        response.contains("moved"),
        "unexpected response: {response}"
    );

    let table = cluster.nodes[0].table();
    assert!(table.epoch > 1, "the epoch must advance");

    // Every server agrees on the new table, and hosts exactly its partitions.
    for node in &cluster.nodes {
        assert_eq!(node.table().epoch, table.epoch);
        assert_eq!(node.hosted_partitions(), table.hosted_by(node.id()));
    }

    assert!(
        !cluster.nodes[3].hosted_partitions().is_empty(),
        "server 4 must now hold data"
    );

    // Nothing was lost, the new replicas physically hold the old data, and
    // the cluster still takes writes.
    cluster.assert_every_replica_holds("k", 0..30).await;
    cluster.assert_membership_matches_table().await;
    cluster.assert_keys("k", 0..30, &[1, 2, 3, 4]).await;
    cluster.write_keys("m", 0..20).await;
    cluster.assert_keys("m", 0..20, &[1, 2, 3, 4]).await;

    // Running it again is a no-op.
    let again = ask(&cluster.clients[1], "REBALANCE 1,2,3,4").await;
    assert!(
        again.contains("already balanced"),
        "unexpected response: {again}"
    );

    // ---- remove server 1 -------------------------------------------------
    let response = tokio::time::timeout(
        Duration::from_secs(180),
        send_request(&cluster.clients[2], "REBALANCE 2,3,4"),
    )
    .await
    .expect("rebalance finished in time")
    .expect("request");

    assert!(
        response.contains("moved"),
        "unexpected response: {response}"
    );

    let table = cluster.nodes[1].table();
    assert!(
        cluster.nodes[0].hosted_partitions().is_empty(),
        "server 1 holds nothing now"
    );

    for id in [2, 3, 4] {
        let node = &cluster.nodes[cluster.at(id)];
        assert_eq!(node.table().epoch, table.epoch);
        assert_eq!(node.hosted_partitions(), table.hosted_by(id));
    }

    // Still no data lost, readable even through the departed server (it just
    // forwards), and still writable.
    cluster.assert_every_replica_holds("k", 0..30).await;
    cluster.assert_every_replica_holds("m", 0..20).await;
    cluster.assert_membership_matches_table().await;
    cluster.assert_keys("k", 0..30, &[1, 2, 3, 4]).await;
    cluster.assert_keys("m", 0..20, &[2, 3, 4]).await;
    cluster.write_keys("z", 0..10).await;
    cluster.assert_keys("z", 0..10, &[2, 3, 4]).await;
    cluster.assert_every_replica_holds("z", 0..10).await;
}

// ---------------------------------------------------------------------------
// Cross-partition transactions
// ---------------------------------------------------------------------------

/// Keys that land in `count` different partitions, in partition order.
fn keys_in_distinct_partitions(table: &RoutingTable, count: usize) -> Vec<(String, PartitionId)> {
    let mut found: std::collections::BTreeMap<PartitionId, String> = Default::default();

    for i in 0.. {
        let key = format!("txn-key-{i}");
        found.entry(table.partition_of(&key)).or_insert(key);

        if found.len() >= count {
            break;
        }
    }

    found
        .into_iter()
        .map(|(partition, key)| (key, partition))
        .collect()
}

fn put(key: &str, value: &str) -> TxnOp {
    TxnOp::Set {
        key: key.into(),
        value: value.into(),
    }
}

impl Cluster {
    /// Whichever server currently leads `partition`.
    async fn leader_group(&self, partition: PartitionId) -> Arc<RaftServer> {
        let deadline = Instant::now() + Duration::from_secs(30);

        loop {
            for node in &self.nodes {
                if let Some(group) = node.group(partition)
                    && group.leader_status().await.0 == Role::Leader
                {
                    return group;
                }
            }

            assert!(
                Instant::now() < deadline,
                "no leader for partition {partition}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Run a protocol step by hand, as a coordinator that may "crash" between
    /// steps would have.
    async fn step(&self, partition: PartitionId, step: TxnCommand) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);

        loop {
            let group = self.leader_group(partition).await;

            match group.txn_command(step.clone()).await {
                Ok(outcome) => return outcome,
                Err(error) => assert!(Instant::now() < deadline, "step failed: {error}"),
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait for `key` to read `expected` through server `via` (a value
    /// appearing, or disappearing, is how recovery shows up to a client).
    async fn eventually(&self, via: usize, key: &str, expected: &str) {
        let deadline = Instant::now() + Duration::from_secs(20);

        loop {
            let response = ask(&self.clients[via], &format!("GET {key}")).await;

            if response == expected {
                return;
            }

            assert!(
                Instant::now() < deadline,
                "GET {key} still answers {response:?}, expected {expected:?}"
            );

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_writes_several_partitions_atomically() {
    let (configs, data_dir) = cluster_config("txn_commit", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 3);
    assert_eq!(keys.len(), 3, "the keys must span three partitions");

    cluster.write_keys("warm", 0..4).await;
    ask_until_ok(&cluster.clients[0], &format!("SET {} old", keys[2].0)).await;

    // Three partitions, one atomic commit (including a delete).
    let request = format!(
        "TXN SET {} one ; SET {} two ; DELETE {}",
        keys[0].0, keys[1].0, keys[2].0
    );
    assert_eq!(ask_until_ok(&cluster.clients[0], &request).await, "+OK");

    for via in 0..3 {
        cluster.eventually(via, &keys[0].0, &bulk("one")).await;
        cluster.eventually(via, &keys[1].0, &bulk("two")).await;
        cluster.eventually(via, &keys[2].0, "$-1\r\n").await;
    }

    // Locks were released: the same keys can be written by another
    // transaction, and by plain writes.
    let again = format!("TXN SET {} uno ; SET {} dos", keys[0].0, keys[1].0);
    assert_eq!(ask_until_ok(&cluster.clients[1], &again).await, "+OK");
    cluster.eventually(2, &keys[0].0, &bulk("uno")).await;
    assert_eq!(
        ask_until_ok(&cluster.clients[2], &format!("SET {} plain", keys[0].0)).await,
        "+OK"
    );

    // Bad input never reaches Raft.
    assert!(
        ask(&cluster.clients[0], "TXN GET a")
            .await
            .starts_with("-ERR")
    );
    assert!(
        ask(&cluster.clients[0], "TXN SET a 1 ; SET a 2")
            .await
            .starts_with("-ERR")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_transaction_aborts_and_leaves_no_trace() {
    let (configs, data_dir) = cluster_config("txn_conflict", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    // The holder must stay open for the whole test: recovery may not touch it.
    let configs = with_txn_timeout(configs, Duration::from_secs(300));
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);
    let (locked, locked_partition) = (&keys[0].0, keys[0].1);
    let (other, _) = (&keys[1].0, keys[1].1);

    // Another transaction has prepared (locked) `locked` and is still open.
    let vote = cluster
        .step(
            locked_partition,
            TxnCommand::Prepare {
                id: "holder".into(),
                coordinator: locked_partition,
                participants: vec![locked_partition],
                ops: vec![put(locked, "held")],
            },
        )
        .await;
    assert_eq!(vote, "YES");

    // A rival that needs `locked` plus an unrelated key must abort entirely.
    let rival = format!("TXN SET {other} should-not-appear ; SET {locked} rival");
    let response = ask_until_ok(&cluster.clients[0], &rival).await;

    assert!(
        response.starts_with("-ERR") && response.contains("aborted"),
        "unexpected response: {response}"
    );
    assert_eq!(
        ask_until_ok(&cluster.clients[0], &format!("GET {other}")).await,
        "$-1\r\n"
    );
    assert_eq!(
        ask_until_ok(&cluster.clients[0], &format!("GET {locked}")).await,
        "$-1\r\n"
    );

    // Release the holder; now the same rival succeeds (its aborted attempt
    // left nothing behind).
    cluster
        .step(
            locked_partition,
            TxnCommand::Finish {
                id: "holder".into(),
                commit: false,
            },
        )
        .await;

    assert_eq!(ask_until_ok(&cluster.clients[1], &rival).await, "+OK");
    cluster
        .eventually(2, other, &bulk("should-not-appear"))
        .await;
    cluster.eventually(2, locked, &bulk("rival")).await;
}

/// The coordinator recorded COMMIT and then "died" before phase 2. Nothing
/// is applied yet and the keys are locked; recovery must finish the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_completes_a_transaction_whose_coordinator_died_after_deciding() {
    let (configs, data_dir) = cluster_config("txn_recover_commit", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    let configs = with_txn_timeout(configs, Duration::from_millis(1500));
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);
    let participants: Vec<PartitionId> = keys.iter().map(|(_, partition)| *partition).collect();
    let coordinator = participants[0];

    // Let every partition elect a leader first, so the hand-driven steps
    // below run well inside the recovery timeout.
    for (_, partition) in &keys {
        cluster.leader_group(*partition).await;
    }

    for (key, partition) in &keys {
        let vote = cluster
            .step(
                *partition,
                TxnCommand::Prepare {
                    id: "orphan-commit".into(),
                    coordinator,
                    participants: participants.clone(),
                    ops: vec![put(key, "committed-by-recovery")],
                },
            )
            .await;
        assert_eq!(vote, "YES");
    }

    // The commit point: the decision is durable, but nobody tells the
    // participants. Their keys are still locked and unwritten.
    let decision = cluster
        .step(
            coordinator,
            TxnCommand::Decide {
                id: "orphan-commit".into(),
                commit: true,
            },
        )
        .await;
    assert_eq!(decision, "COMMIT");

    // Recovery reads the decision and finishes it on every participant.
    for (key, _) in &keys {
        cluster
            .eventually(1, key, &bulk("committed-by-recovery"))
            .await;
    }

    // ...and the locks are gone.
    let next = format!("TXN SET {} again ; SET {} again", keys[0].0, keys[1].0);
    assert_eq!(ask_until_ok(&cluster.clients[2], &next).await, "+OK");
}

/// The coordinator died before deciding anything. Recovery must abort (first
/// decision wins) and a late attempt to commit must lose.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_aborts_an_undecided_transaction_and_a_late_commit_loses() {
    let (configs, data_dir) = cluster_config("txn_recover_abort", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    let configs = with_txn_timeout(configs, Duration::from_millis(1500));
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);
    let participants: Vec<PartitionId> = keys.iter().map(|(_, partition)| *partition).collect();
    let coordinator = participants[0];

    // Let every partition elect a leader first, so the hand-driven steps
    // below run well inside the recovery timeout.
    for (_, partition) in &keys {
        cluster.leader_group(*partition).await;
    }

    for (key, partition) in &keys {
        let vote = cluster
            .step(
                *partition,
                TxnCommand::Prepare {
                    id: "orphan-abort".into(),
                    coordinator,
                    participants: participants.clone(),
                    ops: vec![put(key, "must-never-appear")],
                },
            )
            .await;
        assert_eq!(vote, "YES");
    }

    // No decision is ever sent. After the timeout recovery decides ABORT and
    // releases the locks: a new transaction on the same keys goes through.
    let next = format!("TXN SET {} fresh ; SET {} fresh", keys[0].0, keys[1].0);
    assert_eq!(ask_until_ok(&cluster.clients[0], &next).await, "+OK");
    cluster.eventually(1, &keys[0].0, &bulk("fresh")).await;
    cluster.eventually(2, &keys[1].0, &bulk("fresh")).await;

    // The original coordinator wakes up late and tries to commit: it is told
    // the transaction was already aborted.
    let late = cluster
        .step(
            coordinator,
            TxnCommand::Decide {
                id: "orphan-abort".into(),
                commit: true,
            },
        )
        .await;
    assert_eq!(late, "ABORT", "the first decision must win");
}
