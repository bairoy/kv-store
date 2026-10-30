//! End-to-end tests of the sharded cluster.

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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("local addr")
        .port()
}

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
            max_connections: 1024,
            max_pending_writes: 1024,
            bind: None,
            security: crate::net::security::Security::plain(),
        })
        .collect();

    (configs, data_dir)
}

fn with_txn_timeout(mut configs: Vec<ShardConfig>, timeout: Duration) -> Vec<ShardConfig> {
    for config in &mut configs {
        config.txn_timeout = timeout;
    }

    configs
}

async fn ask(address: &str, request: &str) -> String {
    send_request(address, request).await.expect("request")
}

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
    async fn assert_every_replica_holds(&self, prefix: &str, range: std::ops::Range<usize>) {
        let table = self.nodes[0].table();

        for i in range {
            let key = format!("{prefix}{i}");
            let partition = table.partition_of(&key);

            for server in table.replicas(partition) {
                let node = &self.nodes[self.at(*server)];
                let deadline = Instant::now() + Duration::from_secs(10);

                loop {
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

    let route = node.process("ROUTE user:1").await;
    assert!(route.contains("partition="));
    assert!(route.contains("epoch=1"));

    let status = node.process("STATUS").await;
    for partition in node.hosted_partitions() {
        assert!(status.contains(&format!("p{partition}:")));
    }

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

    let exhausted = node.process("FORWARD 0 1 GET x").await;
    assert!(
        exhausted.contains("too many redirects") || exhausted.contains("no leader"),
        "unexpected response: {exhausted}"
    );

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sharded_cluster_routes_every_key_to_its_partition() {
    let (configs, data_dir) = cluster_config("cluster", &[1, 2, 3], &[], 4, 2);
    let table = configs[0].table.clone();
    let cluster = Cluster::start(configs, data_dir);

    cluster.write_keys("user:", 0..40).await;
    cluster.assert_keys("user:", 0..40, &[1, 2, 3]).await;

    let used: std::collections::BTreeSet<_> = (0..40)
        .map(|i| table.partition_of(&format!("user:{i}")))
        .collect();
    assert!(
        used.len() >= 3,
        "keys should spread over partitions: {used:?}"
    );

    assert_eq!(
        ask_until_ok(&cluster.clients[2], "DELETE user:0").await,
        "+OK"
    );
    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET user:0").await,
        "$-1\r\n"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_with_a_stale_routing_table_is_healed_on_contact() {
    let (configs, data_dir) = cluster_config("stale_epoch", &[1, 2, 3], &[], 4, 2);
    let table = configs[0].table.clone();
    let cluster = Cluster::start(configs, data_dir);

    cluster.write_keys("warm:", 0..8).await;

    let newer = table.with_partition(0, table.replicas(0).to_vec(), 2);
    assert_eq!(
        ask(&cluster.clients[0], &format!("SETTABLE {}", newer.encode())).await,
        "+OK"
    );
    assert_eq!(cluster.nodes[0].table().epoch, 2);
    assert_eq!(cluster.nodes[1].table().epoch, 1);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adding_and_removing_servers_moves_partitions_without_losing_data() {
    let (configs, data_dir) = cluster_config("rebalance", &[1, 2, 3], &[4], 4, 3);
    let cluster = Cluster::start(configs, data_dir);

    cluster.write_keys("k", 0..30).await;
    assert!(
        cluster.nodes[3].hosted_partitions().is_empty(),
        "the spare holds nothing yet"
    );

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

    for node in &cluster.nodes {
        assert_eq!(node.table().epoch, table.epoch);
        assert_eq!(node.hosted_partitions(), table.hosted_by(node.id()));
    }

    assert!(
        !cluster.nodes[3].hosted_partitions().is_empty(),
        "server 4 must now hold data"
    );

    cluster.assert_every_replica_holds("k", 0..30).await;
    cluster.assert_membership_matches_table().await;
    cluster.assert_keys("k", 0..30, &[1, 2, 3, 4]).await;
    cluster.write_keys("m", 0..20).await;
    cluster.assert_keys("m", 0..20, &[1, 2, 3, 4]).await;

    let again = ask(&cluster.clients[1], "REBALANCE 1,2,3,4").await;
    assert!(
        again.contains("already balanced"),
        "unexpected response: {again}"
    );

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

    cluster.assert_every_replica_holds("k", 0..30).await;
    cluster.assert_every_replica_holds("m", 0..20).await;
    cluster.assert_membership_matches_table().await;
    cluster.assert_keys("k", 0..30, &[1, 2, 3, 4]).await;
    cluster.assert_keys("m", 0..20, &[2, 3, 4]).await;
    cluster.write_keys("z", 0..10).await;
    cluster.assert_keys("z", 0..10, &[2, 3, 4]).await;
    cluster.assert_every_replica_holds("z", 0..10).await;
}

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

    let again = format!("TXN SET {} uno ; SET {} dos", keys[0].0, keys[1].0);
    assert_eq!(ask_until_ok(&cluster.clients[1], &again).await, "+OK");
    cluster.eventually(2, &keys[0].0, &bulk("uno")).await;
    assert_eq!(
        ask_until_ok(&cluster.clients[2], &format!("SET {} plain", keys[0].0)).await,
        "+OK"
    );

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
    let configs = with_txn_timeout(configs, Duration::from_secs(300));
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);
    let (locked, locked_partition) = (&keys[0].0, keys[0].1);
    let (other, _) = (&keys[1].0, keys[1].1);

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_completes_a_transaction_whose_coordinator_died_after_deciding() {
    let (configs, data_dir) = cluster_config("txn_recover_commit", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    let configs = with_txn_timeout(configs, Duration::from_millis(1500));
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);
    let participants: Vec<PartitionId> = keys.iter().map(|(_, partition)| *partition).collect();
    let coordinator = participants[0];

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

    for (key, _) in &keys {
        cluster
            .eventually(1, key, &bulk("committed-by-recovery"))
            .await;
    }

    let next = format!("TXN SET {} again ; SET {} again", keys[0].0, keys[1].0);
    assert_eq!(ask_until_ok(&cluster.clients[2], &next).await, "+OK");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_aborts_an_undecided_transaction_and_a_late_commit_loses() {
    let (configs, data_dir) = cluster_config("txn_recover_abort", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    let configs = with_txn_timeout(configs, Duration::from_millis(1500));
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);
    let participants: Vec<PartitionId> = keys.iter().map(|(_, partition)| *partition).collect();
    let coordinator = participants[0];

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

    let next = format!("TXN SET {} fresh ; SET {} fresh", keys[0].0, keys[1].0);
    assert_eq!(ask_until_ok(&cluster.clients[0], &next).await, "+OK");
    cluster.eventually(1, &keys[0].0, &bulk("fresh")).await;
    cluster.eventually(2, &keys[1].0, &bulk("fresh")).await;

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

fn with_limits(
    mut configs: Vec<ShardConfig>,
    max_connections: usize,
    max_pending_writes: usize,
) -> Vec<ShardConfig> {
    for config in &mut configs {
        config.max_connections = max_connections;
        config.max_pending_writes = max_pending_writes;
    }

    configs
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_request_is_applied_once_whichever_server_it_reaches() {
    let (configs, data_dir) = cluster_config("v11_once", &[1, 2, 3], &[], 4, 3);
    let cluster = Cluster::start(configs, data_dir);

    for via in 0..3 {
        let reply = ask_until_ok(&cluster.clients[via], "REQ client-a 1 INCR hits").await;
        assert_eq!(reply, bulk("1"), "attempt via server {via}");
    }

    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET hits").await,
        bulk("1")
    );

    assert_eq!(
        ask_until_ok(&cluster.clients[1], "REQ client-a 2 INCR hits").await,
        bulk("2")
    );

    assert_eq!(
        ask_until_ok(&cluster.clients[2], "REQ client-b 1 INCR hits").await,
        bulk("3")
    );

    let stale = ask(&cluster.clients[0], "REQ client-a 1 INCR hits").await;
    assert!(stale.contains("stale") || stale == bulk("1"), "{stale}");
    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET hits").await,
        bulk("3")
    );

    assert!(
        ask(&cluster.clients[0], "INCR hits")
            .await
            .contains("request id")
    );

    assert!(
        ask(&cluster.clients[0], "REQ c SET a b")
            .await
            .starts_with("-ERR")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_set_does_not_undo_someone_elses_newer_write() {
    let (configs, data_dir) = cluster_config("v11_lost_update", &[1, 2, 3], &[], 4, 3);
    let cluster = Cluster::start(configs, data_dir);

    assert_eq!(
        ask_until_ok(&cluster.clients[0], "REQ a 1 SET shared first").await,
        "+OK"
    );
    assert_eq!(
        ask_until_ok(&cluster.clients[1], "REQ b 1 SET shared second").await,
        "+OK"
    );

    assert_eq!(
        ask_until_ok(&cluster.clients[2], "REQ a 1 SET shared first").await,
        "+OK",
        "the retry reports the original success"
    );

    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET shared").await,
        bulk("second"),
        "B's newer write must survive A's retry"
    );

    ask_until_ok(&cluster.clients[0], "SET plain first").await;
    ask_until_ok(&cluster.clients[0], "SET plain second").await;
    ask_until_ok(&cluster.clients[0], "SET plain first").await;
    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET plain").await,
        bulk("first")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_retried_transaction_does_not_run_its_writes_again() {
    let (configs, data_dir) = cluster_config("v11_txn_retry", &[1, 2, 3], &[], 4, 3);
    let table = configs[0].table.clone();
    let cluster = Cluster::start(configs, data_dir);

    let keys = keys_in_distinct_partitions(&table, 2);

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut number = 0;

    let request = loop {
        number += 1;
        let request = format!(
            "REQ t1 {number} TXN SET {} one ; SET {} two",
            keys[0].0, keys[1].0
        );

        let reply = ask(&cluster.clients[0], &request).await;

        if reply == "+OK" {
            break request;
        }

        assert!(
            Instant::now() < deadline,
            "transaction never committed: {reply}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    cluster.eventually(1, &keys[0].0, &bulk("one")).await;
    cluster.eventually(2, &keys[1].0, &bulk("two")).await;

    assert_eq!(
        ask_until_ok(&cluster.clients[0], &format!("SET {} changed", keys[0].0)).await,
        "+OK"
    );

    assert_eq!(ask_until_ok(&cluster.clients[1], &request).await, "+OK");

    cluster.eventually(2, &keys[0].0, &bulk("changed")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_session_table_survives_log_compaction() {
    let (mut configs, data_dir) = cluster_config("v11_compaction", &[1, 2, 3], &[], 1, 3);
    for config in &mut configs {
        config.snapshot_threshold = Some(5);
    }
    let cluster = Cluster::start(configs, data_dir);

    assert_eq!(
        ask_until_ok(&cluster.clients[0], "REQ keeper 1 INCR hits").await,
        bulk("1")
    );

    for i in 0..40 {
        let reply = ask_until_ok(
            &cluster.clients[0],
            &format!("REQ filler {} SET f v{i}", i + 1),
        )
        .await;
        assert_eq!(reply, "+OK");
    }

    assert_eq!(
        ask_until_ok(&cluster.clients[1], "REQ keeper 1 INCR hits").await,
        bulk("1")
    );
    assert_eq!(
        ask_until_ok(&cluster.clients[2], "GET hits").await,
        bulk("1")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_beyond_the_limit_are_refused_with_busy_and_not_applied() {
    let (configs, data_dir) = cluster_config("v11_busy_writes", &[1, 2, 3], &[], 4, 3);
    let configs = with_limits(configs, 1024, 0);
    let cluster = Cluster::start(configs, data_dir);

    let busy_before = crate::metrics::counter_value("writes_rejected_total", &[("reason", "busy")]);

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let reply = ask(&cluster.clients[0], "GET k").await;
        if !reply.starts_with("-ERR") || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let reply = ask(&cluster.clients[0], "SET k v").await;
    assert!(reply.starts_with("-ERR BUSY"), "{reply}");

    let reply = ask(&cluster.clients[1], "REQ c 1 SET k v").await;
    assert!(reply.contains("BUSY"), "{reply}");

    assert_eq!(ask(&cluster.clients[2], "GET k").await, "$-1\r\n");

    assert!(
        crate::metrics::counter_value("writes_rejected_total", &[("reason", "busy")]) > busy_before
    );
}

async fn open_held_connection(address: &str) -> tokio::net::TcpStream {
    use crate::net::framing::{FrameDecoder, encode_frame};
    use crate::raft::server::read_frame;
    use tokio::io::AsyncWriteExt;

    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(&encode_frame(b"STATUS").unwrap())
            .await
            .unwrap();

        let mut decoder = FrameDecoder::new();
        let reply = read_frame(&mut stream, &mut decoder)
            .await
            .unwrap()
            .unwrap();

        if !String::from_utf8_lossy(&reply).starts_with("-ERR BUSY") {
            return stream;
        }

        assert!(Instant::now() < deadline, "could not get a connection slot");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connections_beyond_the_limit_get_busy_and_capacity_returns() {
    let (configs, data_dir) = cluster_config("v11_busy_conns", &[1, 2, 3], &[], 4, 3);
    let configs = with_limits(configs, 2, 1024);
    let cluster = Cluster::start(configs, data_dir);

    ask_until_ok(&cluster.clients[0], "SET warm up").await;

    let first = open_held_connection(&cluster.clients[0]).await;
    let second = open_held_connection(&cluster.clients[0]).await;

    let rejected_before = crate::metrics::counter_value("connections_rejected_total", &[]);

    let reply = ask(&cluster.clients[0], "GET warm").await;
    assert!(reply.starts_with("-ERR BUSY"), "{reply}");
    assert!(crate::metrics::counter_value("connections_rejected_total", &[]) > rejected_before);

    drop(first);
    drop(second);

    assert_eq!(
        ask_until_ok(&cluster.clients[0], "GET warm").await,
        bulk("up")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn requests_and_dedup_hits_show_up_in_the_metrics() {
    let (configs, data_dir) = cluster_config("v11_metrics", &[1, 2, 3], &[], 4, 3);
    let cluster = Cluster::start(configs, data_dir);

    let ok_before = crate::metrics::counter_value("requests_total", &[("outcome", "ok")]);
    let dup_before = crate::metrics::counter_value("session_retries_answered_total", &[]);

    ask_until_ok(&cluster.clients[0], "REQ m 1 INCR n").await;
    ask_until_ok(&cluster.clients[0], "REQ m 1 INCR n").await;
    ask_until_ok(&cluster.clients[0], "GET n").await;

    assert!(crate::metrics::counter_value("requests_total", &[("outcome", "ok")]) >= ok_before + 3);
    assert!(crate::metrics::counter_value("session_retries_answered_total", &[]) > dup_before);
    assert!(crate::metrics::histogram_count("request_seconds", &[("outcome", "ok")]) > 0);

    let text = crate::metrics::render();
    assert!(text.contains("raft_commit_seconds_bucket"));
    assert!(text.contains("raft_log_fsync_seconds_bucket"));
    assert!(text.contains("wal_fsync_seconds_bucket"));
}

fn make_test_certs(name: &str) -> Option<PathBuf> {
    let dir = temp_dir(name);
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/gen-certs.sh");

    let status = std::process::Command::new("sh")
        .arg(script)
        .arg(&dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .ok()?;

    status.success().then_some(dir)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_secured_cluster_requires_the_token_and_encrypts_every_connection() {
    use crate::net::security::{Security, TlsFiles};
    use crate::shard::node::send_request_with;

    let Some(certs) = make_test_certs("secured_certs") else {
        eprintln!("openssl not available: skipping the secured-cluster test");
        return;
    };

    let (mut configs, data_dir) = cluster_config("secured", &[1, 2, 3], &[], 4, 3);

    let security = Security::new(
        Some("correct-horse".to_string()),
        Some(TlsFiles {
            cert: &certs.join("server.pem"),
            key: &certs.join("server.key"),
            ca: &certs.join("ca.pem"),
        }),
    )
    .unwrap();

    for config in &mut configs {
        config.security = std::sync::Arc::clone(&security);
    }

    let cluster = Cluster::start(configs, data_dir);

    let secure = |address: String, request: String| {
        let security = std::sync::Arc::clone(&security);

        async move {
            let deadline = Instant::now() + Duration::from_secs(30);

            loop {
                let reply = send_request_with(&security, &address, &request)
                    .await
                    .unwrap_or_else(|error| format!("-ERR {error}"));

                if !reply.starts_with("-ERR") || Instant::now() >= deadline {
                    return reply;
                }

                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };

    for i in 0..8 {
        let reply = secure(
            cluster.clients[i % 3].clone(),
            format!("REQ me {} SET key{i} value{i}", i + 1),
        )
        .await;
        assert_eq!(reply, "+OK", "SET key{i}");
    }

    for i in 0..8 {
        for via in 0..3 {
            let reply = secure(cluster.clients[via].clone(), format!("GET key{i}")).await;
            assert_eq!(
                reply,
                bulk(&format!("value{i}")),
                "GET key{i} via server {via}"
            );
        }
    }

    assert!(
        send_request(&cluster.clients[0], "GET key0").await.is_err(),
        "a plaintext client must not be served"
    );

    let wrong = Security::new(
        Some("wrong".to_string()),
        Some(TlsFiles {
            cert: &certs.join("server.pem"),
            key: &certs.join("server.key"),
            ca: &certs.join("ca.pem"),
        }),
    )
    .unwrap();

    let error = send_request_with(&wrong, &cluster.clients[0], "GET key0")
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);

    let tls_only = Security::new(
        None,
        Some(TlsFiles {
            cert: &certs.join("server.pem"),
            key: &certs.join("server.key"),
            ca: &certs.join("ca.pem"),
        }),
    )
    .unwrap();

    let reply = send_request_with(&tls_only, &cluster.clients[0], "GET key0")
        .await
        .unwrap();
    assert!(reply.contains("NOAUTH"), "{reply}");

    let _ = std::fs::remove_dir_all(certs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_token_alone_protects_a_cluster_without_tls() {
    use crate::net::security::Security;
    use crate::shard::node::send_request_with;

    let (mut configs, data_dir) = cluster_config("token_only", &[1, 2, 3], &[], 4, 3);
    let security = Security::new(Some("s3cret".to_string()), None).unwrap();

    for config in &mut configs {
        config.security = std::sync::Arc::clone(&security);
    }

    let cluster = Cluster::start(configs, data_dir);
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        let reply = send_request_with(&security, &cluster.clients[0], "SET a 1")
            .await
            .unwrap_or_else(|error| format!("-ERR {error}"));

        if reply == "+OK" {
            break;
        }

        assert!(Instant::now() < deadline, "never became writable: {reply}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let anonymous = send_request(&cluster.clients[1], "GET a").await.unwrap();
    assert!(anonymous.contains("NOAUTH"), "{anonymous}");

    let allowed = send_request_with(&security, &cluster.clients[2], "GET a")
        .await
        .unwrap();
    assert_eq!(allowed, bulk("1"));

    let wrong = Security::new(Some("nope".to_string()), None).unwrap();
    assert!(
        send_request_with(&wrong, &cluster.clients[1], "GET a")
            .await
            .is_err()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_peer_port_ignores_unauthenticated_raft_messages() {
    use crate::net::framing::{FrameDecoder, encode_frame};
    use crate::net::security::Security;
    use crate::raft::rpc::{RaftMessage, encode_envelope};
    use crate::raft::server::read_frame;
    use crate::raft::types::RequestVoteArgs;
    use tokio::io::AsyncWriteExt;

    let (mut configs, data_dir) = cluster_config("peer_auth", &[1, 2, 3], &[], 4, 3);
    let peer_address = configs[0].servers[&1].peer.clone();
    let security = Security::new(Some("s3cret".to_string()), None).unwrap();

    for config in &mut configs {
        config.security = std::sync::Arc::clone(&security);
    }

    let _cluster = Cluster::start(configs, data_dir);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let vote = encode_frame(
        &encode_envelope(
            0,
            &RaftMessage::RequestVote(RequestVoteArgs {
                term: 0,
                candidate_id: 9,
                last_log_index: 0,
                last_log_term: 0,
            }),
        )
        .unwrap(),
    )
    .unwrap();

    let mut stream = tokio::net::TcpStream::connect(&peer_address).await.unwrap();
    stream.write_all(&vote).await.unwrap();
    let mut decoder = FrameDecoder::new();
    let answer = read_frame(&mut stream, &mut decoder).await;
    assert!(
        !matches!(answer, Ok(Some(_))),
        "an unauthenticated peer must get no reply"
    );

    let mut stream = tokio::net::TcpStream::connect(&peer_address).await.unwrap();
    stream
        .write_all(&encode_frame(b"AUTH nope").unwrap())
        .await
        .unwrap();
    stream.write_all(&vote).await.unwrap();
    let mut decoder = FrameDecoder::new();
    let answer = read_frame(&mut stream, &mut decoder).await;
    assert!(!matches!(answer, Ok(Some(_))));

    let mut stream = tokio::net::TcpStream::connect(&peer_address).await.unwrap();
    stream
        .write_all(&encode_frame(b"AUTH s3cret").unwrap())
        .await
        .unwrap();
    stream.write_all(&vote).await.unwrap();
    let mut decoder = FrameDecoder::new();
    let answer = read_frame(&mut stream, &mut decoder).await.unwrap();
    assert!(answer.is_some(), "an authenticated peer must get a reply");
}
