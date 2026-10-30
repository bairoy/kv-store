use crate::legacy::sharded_store::ShardedStore;
use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Command, Response, encode_response, parse_request};

use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

pub async fn handle_connection(mut stream: TcpStream, store: Arc<ShardedStore>) -> io::Result<()> {
    let mut decoder = FrameDecoder::new();

    let mut buffer = [0u8; 4096];

    loop {
        let bytes_read = stream.read(&mut buffer).await?;

        if bytes_read == 0 {
            break;
        }

        decoder.push_bytes(&buffer[..bytes_read]);

        loop {
            let payload = match decoder.next_frame()? {
                Some(payload) => payload,

                None => break,
            };

            let request = std::str::from_utf8(&payload)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;

            let response = match parse_request(request) {
                Some(command) => execute_command(&store, command),

                None => Response::Error("invalid command".to_string()),
            };

            let response = encode_response(response);

            let frame = encode_frame(response.as_bytes())?;

            stream.write_all(&frame).await?;
        }
    }

    Ok(())
}
