use std::io;
use std::sync::{Arc, Mutex};

use kv_store::service::async_lsm::handle_connection;
use kv_store::storage::lsm::LsmStore;

use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> io::Result<()> {
    let store = LsmStore::open("v7_sstables", "v7_wal.log")?;

    let store = Arc::new(Mutex::new(store));

    let listener = TcpListener::bind("127.0.0.1:7878").await?;

    println!("V7 async server listening on 127.0.0.1:7878");

    loop {
        let (stream, address) = listener.accept().await?;

        println!("Client connected: {}", address);

        let store = Arc::clone(&store);

        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, store).await {
                eprintln!("Connection error: {}", error);
            }
        });
    }
}
