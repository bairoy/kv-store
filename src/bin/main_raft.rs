//! V9: one node of a 3-node Raft cluster.
//!
//!   cargo run --bin main_raft -- 1      (and 2, 3 in other terminals)
//!
//! Node N listens for peers on 910N and for clients on 710N.
//! Data lives in raft_data/node<N>/.

use std::collections::HashMap;
use std::io;

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

    // Lower this to watch log compaction and snapshot transfer happen.
    if let Some(threshold) = std::env::var("RAFT_SNAPSHOT_THRESHOLD")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        config.snapshot_threshold = threshold;
    }

    let server = RaftServer::new(node, store, config);

    server.run(&peer_address(id), &client_address(id)).await
}
