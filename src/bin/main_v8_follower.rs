use kv_store::replication::follower::Follower;
use kv_store::storage::lsm::LsmStore;
use std::io;

#[tokio::main]
async fn main() -> io::Result<()> {
    println!("Follower: opening local LSM store...");

    let store = LsmStore::open("v8_follower_sstables", "v8_follower_wal.log").map_err(|e| {
        eprintln!("FAILED at LsmStore::open: {e:?}");
        e
    })?;

    println!("Follower: local LSM store opened.");

    println!(
        "Snapshot verification: snapshot_test = {:?}",
        store.get("snapshot_test")
    );

    let follower = Follower::new(store, "v8_follower.offset");

    println!("Follower: starting replication loop...");

    follower.run("127.0.0.1:9000").await.map_err(|e| {
        eprintln!("FAILED in Follower::run: {e:?}");
        e
    })
}
