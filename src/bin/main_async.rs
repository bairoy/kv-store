use std::io;
use std::sync::Arc;

use tokio::net::TcpListener;

use kv_store::legacy::async_server::handle_connection;
use kv_store::legacy::sharded_store::ShardedStore;

#[tokio::main]
async fn main() -> io::Result<()> {
    let store = Arc::new(ShardedStore::open("snapshot.db", "wal.log")?);

    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    println!("V5 server listening on 127.0.0.1:6379");

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
