use crate::framing::{FrameDecoder, encode_frame};
use crate::protocol::{Command, Response, encode_response, parse_request};
use crate::sharded_store::ShardedStore;

use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// ---------------------------------------------------------
// COMMAND EXECUTION
// ---------------------------------------------------------

pub fn execute_command(store: &ShardedStore, command: Command) -> Response {
    match command {
        Command::Set(key, value) => match store.set(key, value) {
            Ok(()) => Response::Ok,
            Err(error) => Response::Error(error.to_string()),
        },

        Command::Get(key) => match store.get(&key) {
            Some(value) => Response::Value(value),

            None => Response::NotFound,
        },

        Command::Delete(key) => match store.delete(&key) {
            Ok(Some(_)) => Response::Ok,

            Ok(None) => Response::NotFound,

            Err(error) => Response::Error(error.to_string()),
        },
    }
}

// ---------------------------------------------------------
// CONNECTION HANDLER
// ---------------------------------------------------------

pub async fn handle_connection(mut stream: TcpStream, store: Arc<ShardedStore>) -> io::Result<()> {
    let mut decoder = FrameDecoder::new();

    let mut buffer = [0u8; 4096];

    loop {
        // Read bytes asynchronously from TCP.
        let bytes_read = stream.read(&mut buffer).await?;

        // Client closed the connection.
        if bytes_read == 0 {
            break;
        }

        // Give the bytes to our existing frame decoder.
        decoder.push_bytes(&buffer[..bytes_read]);

        // Extract every complete frame currently
        // available in the decoder.
        loop {
            let payload = match decoder.next_frame()? {
                Some(payload) => payload,

                None => break,
            };

            // Convert payload into UTF-8 text.
            let request = std::str::from_utf8(&payload)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;

            // Parse the request.
            let response = match parse_request(request) {
                Some(command) => execute_command(&store, command),

                None => Response::Error("invalid command".to_string()),
            };

            // Convert Response into protocol text.
            let response = encode_response(response);

            // Add our length-prefix frame.
            let frame = encode_frame(response.as_bytes())?;

            // Write response asynchronously.
            stream.write_all(&frame).await?;
        }
    }

    Ok(())
}
