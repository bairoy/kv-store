//! Interactive client.

use std::collections::HashMap;
use std::io::{self, BufRead, Write};

use kv_store::client::{Client, ClientError, ClientOptions, Reply};

fn main() -> io::Result<()> {
    let arg = std::env::args()
        .nth(1)
        .or_else(|| std::env::var("KV_SERVERS").ok())
        .unwrap_or_else(|| "127.0.0.1:7878".to_string());

    let mut servers: Vec<String> = arg.split(',').map(str::to_string).collect();
    let mut options = ClientOptions::from_env().unwrap_or_else(|error| {
        eprintln!("security configuration: {error}");
        std::process::exit(2);
    });

    add_known_cluster(&mut servers, &mut options);

    let mut client = Client::with_options(servers.clone(), options);

    println!("connected to KV cluster via {}", servers.join(", "));
    println!("client id {}; type 'stats' or 'quit'", client.id());

    let stdin = io::stdin();

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();

        if stdin.lock().read_line(&mut input)? == 0 {
            break;
        }

        let line = input.trim_end();

        match line {
            "" => continue,
            "quit" => break,
            "stats" => {
                println!("{:?}", client.stats());
                continue;
            }
            _ => {}
        }

        match client.execute_line(line) {
            Ok(Reply::Ok) => println!("+OK"),
            Ok(Reply::Value(value)) => println!("{value}"),
            Ok(Reply::NotFound) => println!("(not found)"),
            Ok(Reply::Error(message)) => println!("-ERR {message}"),
            Err(ClientError::Rejected(message)) => println!("-ERR {message}"),
            Err(error) => println!("-ERR {error}"),
        }
    }

    Ok(())
}

fn add_known_cluster(servers: &mut Vec<String>, options: &mut ClientOptions) {
    let ports: Vec<u16> = servers
        .iter()
        .filter_map(|address| address.rsplit(':').next()?.parse().ok())
        .collect();

    if ports.contains(&7878) {
        options.idempotent_writes = false;
    }

    let raft = ports.iter().any(|port| (7101..=7103).contains(port));
    let shard = ports.iter().any(|port| (7201..=7205).contains(port));

    if raft {
        let mut leaders = HashMap::new();

        for node in 1..=3u64 {
            let address = format!("127.0.0.1:{}", 7100 + node);

            leaders.insert(node, address.clone());

            if !servers.contains(&address) {
                servers.push(address);
            }
        }

        options.leader_addresses = leaders;
    }

    if shard {
        for node in 1..=5u64 {
            let address = format!("127.0.0.1:{}", 7200 + node);

            if !servers.contains(&address) {
                servers.push(address);
            }
        }
    }
}
