use std::io;
use std::path::Path;

use kv_store::replication::leader::{Leader, run_client_server, run_replication_server};
use kv_store::replication::log::ReplicationLog;
use kv_store::storage::lsm::LsmStore;

#[tokio::main]
async fn main() -> io::Result<()> {
    let store = LsmStore::open("v8_leader_sstables", "v8_leader_wal.log")?;

    let log = ReplicationLog::open(Path::new("v8_leader_replication.log"))?;

    let leader = Leader::new(store, log);

    println!("=================================");
    println!("V8 LEADER");
    println!("Client:      127.0.0.1:7878");
    println!("Replication: 127.0.0.1:9000");
    println!("=================================");

    let client_server = run_client_server(leader.clone(), "127.0.0.1:7878");

    let replication_server = run_replication_server(leader, "127.0.0.1:9000");

    tokio::try_join!(client_server, replication_server)?;

    Ok(())
}
