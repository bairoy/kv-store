//! One server of a sharded cluster. Configured by environment variables (see README).

use std::io;
use std::path::PathBuf;
use std::time::Duration;

use kv_store::net::security::Security;
use kv_store::raft::types::NodeId;
use kv_store::shard::cluster;
use kv_store::shard::node::{ShardConfig, ShardNode};
use kv_store::shard::ring::ConsistentHashRing;
use kv_store::shard::routing::RoutingTable;

const VNODES_PER_SERVER: u32 = 200;

fn env_number<T: std::str::FromStr>(name: &str) -> Option<T> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("{message}");
    std::process::exit(2);
}

#[tokio::main]
async fn main() -> io::Result<()> {
    kv_store::logging::init();

    let id: NodeId = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("SHARD_ID").ok())
        .and_then(|text| text.parse().ok())
        .unwrap_or_else(|| fail("usage: main_shard <server id>   (or set SHARD_ID)"));

    let spec =
        cluster::from_env(id).unwrap_or_else(|error| fail(format!("configuration: {error}")));
    let security = Security::from_env()
        .unwrap_or_else(|error| fail(format!("security configuration: {error}")));

    let partitions: u32 = env_number("SHARD_PARTITIONS").unwrap_or(8);
    let replication: usize = env_number("SHARD_REPLICATION").unwrap_or(3);

    let mut ring = ConsistentHashRing::new(VNODES_PER_SERVER);
    for server in &spec.initial {
        ring.add_node(*server);
    }

    let table = RoutingTable::build(&ring, partitions, replication, 1)
        .unwrap_or_else(|error| fail(format!("cannot build the routing table: {error}")));

    let exposed = !spec.bind.client.starts_with("127.");

    if exposed && !security.requires_token() {
        tracing::warn!(
            "listening on {} without KV_AUTH_TOKEN: anyone who can reach this port can read and write data",
            spec.bind.client
        );
    }

    if exposed && !security.tls_enabled() {
        tracing::warn!(
            "listening on {} without TLS: traffic is not encrypted",
            spec.bind.client
        );
    }

    println!("=================================");
    println!("SHARD SERVER {id}");
    println!(
        "Client:  {} (listening on {})",
        spec.servers[&id].client, spec.bind.client
    );
    println!(
        "Peers:   {} (listening on {})",
        spec.servers[&id].peer, spec.bind.peer
    );
    println!(
        "Security: token {}, TLS {}",
        if security.requires_token() {
            "required"
        } else {
            "off"
        },
        if security.tls_enabled() { "on" } else { "off" }
    );
    println!("Routing table (epoch {}):", table.epoch);
    for info in table.partitions() {
        println!("  p{:<3} -> servers {:?}", info.id, info.replicas);
    }
    println!("=================================");

    let node = ShardNode::open(ShardConfig {
        id,
        table,
        servers: spec.servers,
        data_dir: PathBuf::from(
            std::env::var("SHARD_DATA_DIR").unwrap_or_else(|_| "shard_data".to_string()),
        ),
        snapshot_threshold: env_number("RAFT_SNAPSHOT_THRESHOLD"),
        vnodes: VNODES_PER_SERVER,
        txn_timeout: Duration::from_millis(env_number("SHARD_TXN_TIMEOUT_MS").unwrap_or(5000)),
        rebalance_pause: Duration::from_millis(
            env_number("SHARD_REBALANCE_PAUSE_MS").unwrap_or(200),
        ),
        max_connections: env_number("SHARD_MAX_CONNECTIONS").unwrap_or(1024),
        max_pending_writes: env_number("SHARD_MAX_PENDING_WRITES").unwrap_or(1024),
        bind: Some(spec.bind),
        security,
    })?;

    let metrics_address = format!(
        "{}:{}",
        std::env::var("SHARD_METRICS_BIND").unwrap_or_else(|_| "127.0.0.1".to_string()),
        env_number::<u16>("SHARD_METRICS_PORT").unwrap_or(9300 + id as u16)
    );
    tokio::spawn(async move {
        if let Err(error) = kv_store::metrics::serve(&metrics_address).await {
            tracing::error!("metrics endpoint on {metrics_address} failed: {error}");
        }
    });

    node.run().await
}
