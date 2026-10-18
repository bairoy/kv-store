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

    // Verify persisted data before moving the store into Follower.
    println!(
        "Snapshot verification: snapshot_test = {:?}",
        store.get("snapshot_test")
    );

    // Follower::new takes ownership of the store.
    let follower = Follower::new(store, "v8_follower.offset");

    println!("Follower: starting replication loop...");

    follower.run("127.0.0.1:9000").await.map_err(|e| {
        eprintln!("FAILED in Follower::run: {e:?}");
        e
    })
}
