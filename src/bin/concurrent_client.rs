use kv_store::net::framing::{FrameDecoder, encode_frame};

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

const SERVER_ADDR: &str = "127.0.0.1:6379";
const CLIENT_COUNT: usize = 6;

fn send_command(stream: &mut TcpStream, command: &str) -> io::Result<String> {
    // Encode the command into our existing length-prefixed frame.
    let frame = encode_frame(command.as_bytes())?;

    stream.write_all(&frame)?;

    // Wait for the framed response.
    let mut decoder = FrameDecoder::new();

    loop {
        if let Some(response) = decoder.next_frame()? {
            return String::from_utf8(response)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 response"));
        }

        let bytes_read = decoder.read_from(stream)?;

        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed connection",
            ));
        }
    }
}

fn main() -> io::Result<()> {
    println!("Starting {} concurrent clients...", CLIENT_COUNT);

    // Every thread waits here until all clients
    // are ready. This makes the requests start
    // at approximately the same time.
    let barrier = Arc::new(Barrier::new(CLIENT_COUNT));

    let mut handles = Vec::new();

    for client_id in 0..CLIENT_COUNT {
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> io::Result<()> {
            // Each client has its own TCP connection.
            let mut stream = TcpStream::connect(SERVER_ADDR)?;

            println!("Client {} connected", client_id);

            // Wait until every client is connected.
            barrier.wait();

            let start = Instant::now();

            // Each client performs a different operation.
            let command = match client_id {
                0 => "SET name Baiju".to_string(),

                1 => "SET age 21".to_string(),

                2 => "GET name".to_string(),

                3 => "GET age".to_string(),

                4 => "SET language Rust".to_string(),

                5 => "DELETE name".to_string(),

                _ => unreachable!(),
            };

            println!("Client {} → {}", client_id, command);

            let response = send_command(&mut stream, &command)?;

            let elapsed = start.elapsed();

            println!("Client {} ← {} ({:?})", client_id, response, elapsed);

            Ok(())
        });

        handles.push(handle);
    }

    // Wait for every client to finish.
    for handle in handles {
        handle.join().expect("client thread panicked")?;
    }

    println!();
    println!("All concurrent clients finished.");

    Ok(())
}
