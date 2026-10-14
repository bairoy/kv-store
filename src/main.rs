use kv_store::server::handle_connection;
use kv_store::store::Store;
use std::io;
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::thread;

fn main() -> io::Result<()> {
    let store = Store::open("snapshot.db", "store.log")?;
    let store = Arc::new(Mutex::new(store));

    let listener = TcpListener::bind("127.0.0.1:7878")?;
    println!("kv server listening on 127.0.0.1:7878");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    if let Err(error) = handle_connection(stream, store) {
                        eprintln!("connection error: {error}");
                    }
                });
            }
            Err(error) => {
                eprintln!("connection failed: {error}");
            }
        }
    }

    Ok(())
}
