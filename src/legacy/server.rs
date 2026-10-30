use crate::legacy::store::Store;
use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Command, Response, encode_response, parse_request};
use std::io::{self, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

pub fn execute_command(store: &mut Store, command: Command) -> Response {
    match command {
        Command::Set(key, value) => match store.set(key, value) {
            Ok(()) => Response::Ok,
            Err(error) => Response::Error(error.to_string()),
        },
        Command::Get(key) => match store.get(&key) {
            Some(value) => Response::Value(value.clone()),
            None => Response::NotFound,
        },
        Command::Delete(key) => match store.delete(&key) {
            Ok(Some(_)) => Response::Ok,
            Ok(None) => Response::NotFound,
            Err(error) => Response::Error(error.to_string()),
        },
    }
}
pub fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) -> io::Result<()> {
    let mut decoder = FrameDecoder::new();

    loop {
        let bytes_read = decoder.read_from(&mut stream)?;

        if bytes_read == 0 {
            break;
        }

        loop {
            match decoder.next_frame()? {
                Some(payload) => {
                    let request = std::str::from_utf8(&payload)
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;

                    let response = match parse_request(request) {
                        Some(command) => {
                            let mut store = store.lock().map_err(|_| {
                                io::Error::new(io::ErrorKind::Other, "store lock poisoned")
                            })?;

                            execute_command(&mut store, command)
                        }

                        None => Response::Error("invalid command".to_string()),
                    };

                    let response = encode_response(response);

                    let frame = encode_frame(response.as_bytes())?;

                    stream.write_all(&frame)?;
                }

                None => {
                    break;
                }
            }
        }
    }

    Ok(())
}
