use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Command, Response, encode_response, parse_request};
use crate::legacy::store::Store;
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

    // Keep reading from the TCP connection.
    loop {
        // Read whatever bytes are currently available
        // and append them to decoder.buffer.
        let bytes_read = decoder.read_from(&mut stream)?;

        // Client closed the connection.
        if bytes_read == 0 {
            break;
        }

        // Keep extracting complete frames from the buffer.
        loop {
            match decoder.next_frame()? {
                Some(payload) => {
                    // We have a complete frame.
                    // Now convert the payload bytes into UTF-8 text.
                    let request = std::str::from_utf8(&payload)
                        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;

                    // Parse and execute the command.
                    let response = match parse_request(request) {
                        Some(command) => {
                            let mut store = store.lock().map_err(|_| {
                                io::Error::new(io::ErrorKind::Other, "store lock poisoned")
                            })?;

                            execute_command(&mut store, command)
                        }

                        None => Response::Error("invalid command".to_string()),
                    };

                    // Convert the response into a String.
                    let response = encode_response(response);

                    // Convert the response into a framed message.
                    let frame = encode_frame(response.as_bytes())?;

                    // Send the complete frame back to the client.
                    stream.write_all(&frame)?;
                }

                None => {
                    // We don't have a complete frame yet.
                    //
                    // Leave the remaining bytes in the decoder's buffer.
                    // The outer loop will read more bytes from TCP.
                    break;
                }
            }
        }
    }

    Ok(())
}

// pub fn handle_connection(mut stream: TcpStream, store: Arc<Mutex<Store>>) -> io::Result<()> {
//     let mut buffer = [0u8; 4096];
//     loop {
//         let bytes_read = stream.read(&mut buffer)?;

//         if bytes_read == 0 {
//             break;
//         }
//         let request = std::str::from_utf8(&buffer[..bytes_read])
//             .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8"))?;

//         match parse_request(request) {
//             Some(command) => {
//                 let response = {
//                     let mut store = store
//                         .lock()
//                         .map_err(|_| io::Error::new(io::ErrorKind::Other, "store lock poisoned"))?;
//                     execute_command(&mut store, command)
//                 };
//                 let encoded = encode_response(response);
//                 stream.write_all(encoded.as_bytes())?;
//             }
//             None => {
//                 let response = encode_response(Response::Error("invalid command".to_string()));
//                 stream.write_all(response.as_bytes())?;
//             }
//         }
//     }
//     Ok(())
// }
