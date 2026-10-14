use kv_store::framing::{FrameDecoder, encode_frame};
use std::io::{self, BufRead, Write};
use std::net::TcpStream;

fn main() -> io::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:7878")?;
    let stdin = io::stdin();
    let mut decoder = FrameDecoder::new();
    println!("connected to KV server");
    println!("type 'quit' to exit");

    loop {
        print!("> ");
        io::stdout().flush()?;
        let mut input = String::new();
        stdin.lock().read_line(&mut input)?;
        let command = input.trim_end();
        if command == "quit" {
            break;
        }
        if command.is_empty() {
            continue;
        }

        let frame = encode_frame(command.as_bytes())?;
        stream.write_all(&frame)?;
        loop {
            if let Some(response) = decoder.next_frame()? {
                let response = String::from_utf8(response).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid UTF-8 response")
                })?;
                println!("{response}");
                break;
            }
            let bytes_read = decoder.read_from(&mut stream)?;
            if bytes_read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "server closed connection",
                ));
            }
        }
    }
    Ok(())
}
