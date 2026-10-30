//! One node of a three-node Raft cluster.

use std::collections::HashMap;
use std::io;

use kv_store::net::security::Security;
use kv_store::raft::node::RaftNode;
use kv_store::raft::server::{RaftConfig, RaftServer};
use kv_store::raft::types::NodeId;
use kv_store::storage::lsm::LsmStore;

const CLUSTER: [NodeId; 3] = [1, 2, 3];

fn peer_address(id: NodeId) -> String {
    format!("127.0.0.1:{}", 9100 + id)
}

fn client_address(id: NodeId) -> String {
    format!("127.0.0.1:{}", 7100 + id)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    kv_store::logging::init();

    let id: NodeId = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .filter(|id| CLUSTER.contains(id))
        .unwrap_or_else(|| {
            eprintln!("usage: main_raft <node id: 1, 2 or 3>");
            std::process::exit(2);
        });

    let dir = format!("raft_data/node{id}");
    std::fs::create_dir_all(&dir)?;

    let store = LsmStore::open(format!("{dir}/sstables"), format!("{dir}/wal.log"))?;
    let node = RaftNode::open(id, CLUSTER.to_vec(), format!("{dir}/raft.log"))?;

    let peers: HashMap<NodeId, String> = CLUSTER
        .iter()
        .copied()
        .filter(|peer| *peer != id)
        .map(|peer| (peer, peer_address(peer)))
        .collect();

    println!("=================================");
    println!("RAFT NODE {id}");
    println!("Client: {}", client_address(id));
    println!("Peers:  {}", peer_address(id));
    println!("=================================");

    let mut config = RaftConfig::new(peers);
    config.security = Security::from_env().unwrap_or_else(|error| {
        eprintln!("security configuration: {error}");
        std::process::exit(2);
    });

    if let Some(threshold) = std::env::var("RAFT_SNAPSHOT_THRESHOLD")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        config.snapshot_threshold = threshold;
    }

    let server = RaftServer::new(node, store, config);

    let metrics_address = format!("127.0.0.1:{}", 9400 + id);
    tokio::spawn(async move {
        if let Err(error) = kv_store::metrics::serve(&metrics_address).await {
            tracing::error!("metrics endpoint on {metrics_address} failed: {error}");
        }
    });

    let bind_host = std::env::var("KV_BIND_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    let bind =
        |address: String| format!("{bind_host}:{}", address.rsplit(':').next().unwrap_or("0"));

    server
        .run(&bind(peer_address(id)), &bind(client_address(id)))
        .await
}
