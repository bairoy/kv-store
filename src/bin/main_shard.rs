//! V10: one server of a sharded cluster (8 partitions, 3 replicas per
//! partition by default).
//!
//!   cargo run --bin main_shard -- 1      (and 2, 3, 4 in other terminals)
//!
//! Servers 1-4 share the data at first. Server 5 can be started too: it
//! begins empty, and `REBALANCE 1,2,3,4,5` (sent to any server) moves its
//! share of the partitions onto it while the cluster keeps serving. Later
//! `REBALANCE 1,2,3,5` takes server 4 out again.
//!
//! Server N listens for other servers on 127.0.0.1:920N and for clients on
//! 127.0.0.1:720N. Any server accepts any command and routes it to the
//! partition's leader. Try `client 127.0.0.1:7201`, then `ROUTE <key>` and
//! `STATUS`.
//!
//! Data lives in shard_data/node<N>/p<partition>/ and the routing table in
//! shard_data/node<N>/routing.table.
//!
//! Environment overrides: SHARD_PARTITIONS (8), SHARD_REPLICATION (3),
//! SHARD_REBALANCE_PAUSE_MS (200), SHARD_TXN_TIMEOUT_MS (5000),
//! RAFT_SNAPSHOT_THRESHOLD.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;
use std::time::Duration;

use kv_store::raft::types::NodeId;
use kv_store::shard::node::{ServerAddress, ShardConfig, ShardNode};
use kv_store::shard::ring::ConsistentHashRing;
use kv_store::shard::routing::RoutingTable;

/// Every server that can ever take part (the address book).
const SERVERS: [NodeId; 5] = [1, 2, 3, 4, 5];
/// The servers that hold data when the cluster is first created.
const INITIAL_SERVERS: [NodeId; 4] = [1, 2, 3, 4];
const VNODES_PER_SERVER: u32 = 200;

fn env_number<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let id: NodeId = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .filter(|id| SERVERS.contains(id))
        .unwrap_or_else(|| {
            eprintln!("usage: main_shard <server id: 1-5>");
            std::process::exit(2);
        });

    let partitions: u32 = env_number("SHARD_PARTITIONS").unwrap_or(8);
    let replication: usize = env_number("SHARD_REPLICATION").unwrap_or(3);

    // Every server builds the same table: the hash is stable and the ring is
    // deterministic, so no coordination is needed at startup.
    let mut ring = ConsistentHashRing::new(VNODES_PER_SERVER);
    for server in INITIAL_SERVERS {
        ring.add_node(server);
    }

    let table = RoutingTable::build(&ring, partitions, replication, 1).unwrap_or_else(|error| {
        eprintln!("cannot build the routing table: {error}");
        std::process::exit(2);
    });

    let servers: HashMap<NodeId, ServerAddress> = SERVERS
        .iter()
        .map(|server| {
            (
                *server,
                ServerAddress {
                    peer: format!("127.0.0.1:{}", 9200 + server),
                    client: format!("127.0.0.1:{}", 7200 + server),
                },
            )
        })
        .collect();

    println!("=================================");
    println!("SHARD SERVER {id}");
    println!("Client: {}", servers[&id].client);
    println!("Peers:  {}", servers[&id].peer);
    println!("Routing table (epoch {}):", table.epoch);
    for info in table.partitions() {
        println!("  p{:<3} -> servers {:?}", info.id, info.replicas);
    }
    println!("=================================");

    let node = ShardNode::open(ShardConfig {
        id,
        table,
        servers,
        data_dir: PathBuf::from("shard_data"),
        snapshot_threshold: env_number("RAFT_SNAPSHOT_THRESHOLD"),
        vnodes: VNODES_PER_SERVER,
        txn_timeout: Duration::from_millis(env_number("SHARD_TXN_TIMEOUT_MS").unwrap_or(5000)),
        rebalance_pause: Duration::from_millis(
            env_number("SHARD_REBALANCE_PAUSE_MS").unwrap_or(200),
        ),
    })?;

    node.run().await
}
