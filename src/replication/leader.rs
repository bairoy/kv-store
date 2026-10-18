use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::protocol::{Command, Response, encode_response, parse_request};
use crate::replication::log::{LogEntry, ReplicationEntry, ReplicationLog};
use crate::replication::snapshot::Snapshot;
use crate::replication::{ReplicationMessage, encode_message};
use crate::storage::lsm::LsmStore;
use std::io;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, broadcast};

const MAX_FRAME_SIZE: usize = 1024 * 1024;
const SNAPSHOT_CATCH_UP_THRESHOLD: usize = 3;
pub struct Leader {
    pub store: Arc<Mutex<LsmStore>>,
    log: Arc<Mutex<ReplicationLog>>,
    write_lock: Mutex<()>,
    replication_tx: broadcast::Sender<ReplicationEntry>,
}

impl Leader {
    pub fn new(store: LsmStore, log: ReplicationLog) -> Arc<Self> {
        let (replication_tx, _) = broadcast::channel(1024);

        Arc::new(Self {
            store: Arc::new(Mutex::new(store)),
            log: Arc::new(Mutex::new(log)),
            write_lock: Mutex::new(()),
            replication_tx,
        })
    }

    /// Reapply the durable replication log to the local store.
    ///
    /// Call this once during startup, before accepting client writes.
    /// SET and DELETE are replayed in log order.
    pub async fn recover(&self) -> io::Result<()> {
        let entries = {
            let log = self.log.lock().await;
            log.entries_after(0)
        };

        let mut store = self.store.lock().await;

        for entry in entries {
            match entry.operation {
                LogEntry::Set { key, value } => {
                    store.set(key, value)?;
                }
                LogEntry::Delete { key } => {
                    store.delete(key)?;
                }
            }
        }

        Ok(())
    }

    async fn append_and_apply(&self, operation: LogEntry) -> io::Result<ReplicationEntry> {
        let _write_guard = self.write_lock.lock().await;

        // The replication log is made durable before applying the write.
        let entry = {
            let mut log = self.log.lock().await;
            log.append(operation)?
        };

        let apply_result = {
            let mut store = self.store.lock().await;

            match &entry.operation {
                LogEntry::Set { key, value } => store.set(key.clone(), value.clone()),
                LogEntry::Delete { key } => store.delete(key.clone()).map(|_| ()),
            }
        };

        if let Err(error) = apply_result {
            // The operation is already durable in the replication log.
            // The caller must treat this leader as unhealthy and recover
            // before accepting more writes.
            return Err(io::Error::other(format!(
                "durable replication entry {} could not be applied: {error}",
                entry.offset
            )));
        }

        println!(
            "Leader: applied replication entry at offset {}",
            entry.offset
        );

        // Publish only after the durable log append and local application.
        let _ = self.replication_tx.send(entry.clone());

        Ok(entry)
    }

    async fn execute_command(&self, command: Command) -> Response {
        match command {
            Command::Set(key, value) => {
                match self.append_and_apply(LogEntry::Set { key, value }).await {
                    Ok(_) => Response::Ok,
                    Err(error) => Response::Error(error.to_string()),
                }
            }

            Command::Get(key) => {
                let store = self.store.lock().await;

                match store.get(&key) {
                    Some(value) => Response::Value(value),
                    None => Response::NotFound,
                }
            }

            Command::Delete(key) => {
                let existing = {
                    let store = self.store.lock().await;
                    store.get(&key)
                };

                if existing.is_none() {
                    return Response::NotFound;
                }

                match self.append_and_apply(LogEntry::Delete { key }).await {
                    Ok(_) => Response::Ok,
                    Err(error) => Response::Error(error.to_string()),
                }
            }
        }
    }

    pub async fn handle_client(self: Arc<Self>, mut stream: TcpStream) -> io::Result<()> {
        let mut decoder = FrameDecoder::new();
        let mut buffer = [0u8; 4096];

        loop {
            let count = stream.read(&mut buffer).await?;

            if count == 0 {
                return Ok(());
            }

            decoder.push_bytes(&buffer[..count]);

            while let Some(payload) = decoder.next_frame()? {
                if payload.len() > MAX_FRAME_SIZE {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "client frame too large",
                    ));
                }

                let request = std::str::from_utf8(&payload).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "client request is not UTF-8")
                })?;

                let response = match parse_request(request) {
                    Some(command) => {
                        println!("Leader: received client command: {request}");
                        self.execute_command(command).await
                    }
                    None => Response::Error("invalid command".to_string()),
                };

                let encoded = encode_response(response);
                let frame = encode_frame(encoded.as_bytes())?;

                stream.write_all(&frame).await?;
            }
        }
    }

    async fn send_replication_message(
        stream: &mut TcpStream,
        message: &ReplicationMessage,
    ) -> io::Result<()> {
        let payload = encode_message(message)?;
        let frame = encode_frame(&payload)?;
        stream.write_all(&frame).await
    }

    async fn read_replication_message(stream: &mut TcpStream) -> io::Result<ReplicationMessage> {
        let payload = read_frame(stream).await?;
        crate::replication::decode_message(&payload)
    }

    pub async fn handle_follower(self: Arc<Self>, mut stream: TcpStream) -> io::Result<()> {
        let first = Self::read_replication_message(&mut stream).await?;

        let follower_offset = match first {
            ReplicationMessage::Hello { last_offset } => last_offset,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected Hello from follower",
                ));
            }
        };

        println!(
            "Leader: follower connected with last offset {}",
            follower_offset
        );

        // Subscribe before reading catch-up history so writes that occur
        // during catch-up are also observable through the receiver.
        let mut live_rx = self.replication_tx.subscribe();

        let catch_up = {
            let log = self.log.lock().await;
            log.entries_after(follower_offset)
        };
        println!("Leader: {} historical entries to send", catch_up.len());

        let mut last_sent = follower_offset;
        if catch_up.len() > SNAPSHOT_CATCH_UP_THRESHOLD {
            println!(
                "Leader: follower is behind by {} entries; creating snapshot",
                catch_up.len()
            );

            let snapshot = self.create_snapshot().await?;
            let snapshot_offset = snapshot.last_included_offset;
            let messages = snapshot.to_messages()?;

            for message in &messages {
                Self::send_replication_message(&mut stream, message).await?;
            }

            last_sent = snapshot_offset;

            println!("Leader: sent snapshot through offset {}", last_sent);
        } else {
            for entry in catch_up {
                if entry.offset <= last_sent {
                    continue;
                }

                Self::send_replication_message(
                    &mut stream,
                    &ReplicationMessage::Entry(entry.clone()),
                )
                .await?;

                last_sent = entry.offset;

                println!("Leader: sent catch-up entry at offset {}", last_sent);
            }
        }

        loop {
            match live_rx.recv().await {
                Ok(entry) => {
                    if entry.offset <= last_sent {
                        continue;
                    }

                    // If a broadcast was missed, fetch the missing durable
                    // entries rather than assuming the channel is the log.
                    if entry.offset != last_sent + 1 {
                        let missing = {
                            let log = self.log.lock().await;
                            log.entries_after(last_sent)
                        };

                        for missing_entry in missing {
                            if missing_entry.offset <= last_sent {
                                continue;
                            }

                            Self::send_replication_message(
                                &mut stream,
                                &ReplicationMessage::Entry(missing_entry.clone()),
                            )
                            .await?;

                            last_sent = missing_entry.offset;
                        }

                        continue;
                    }

                    Self::send_replication_message(
                        &mut stream,
                        &ReplicationMessage::Entry(entry.clone()),
                    )
                    .await?;

                    last_sent = entry.offset;
                }

                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let missing = {
                        let log = self.log.lock().await;
                        log.entries_after(last_sent)
                    };

                    for entry in missing {
                        if entry.offset <= last_sent {
                            continue;
                        }

                        Self::send_replication_message(
                            &mut stream,
                            &ReplicationMessage::Entry(entry.clone()),
                        )
                        .await?;

                        last_sent = entry.offset;
                    }
                }

                Err(broadcast::error::RecvError::Closed) => {
                    return Ok(());
                }
            }
        }
    }
    pub async fn create_snapshot(&self) -> io::Result<Snapshot> {
        // Prevent a write from landing between offset capture and state capture.
        let _write_guard = self.write_lock.lock().await;

        let last_included_offset = {
            let log = self.log.lock().await;
            log.last_offset()
        };

        let entries = {
            let store = self.store.lock().await;
            store.snapshot_entries()?
        };

        Ok(Snapshot {
            last_included_offset,
            entries,
        })
    }
}

async fn read_frame(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut header = [0u8; 4];
    stream.read_exact(&mut header).await?;

    let length = u32::from_be_bytes(header) as usize;

    if length == 0 || length > MAX_FRAME_SIZE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid replication frame length",
        ));
    }

    let mut payload = vec![0u8; length];
    stream.read_exact(&mut payload).await?;

    Ok(payload)
}

pub async fn run_client_server(leader: Arc<Leader>, address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let leader = Arc::clone(&leader);

        tokio::spawn(async move {
            if let Err(error) = leader.handle_client(stream).await {
                eprintln!("client connection failed: {error}");
            }
        });
    }
}

pub async fn run_replication_server(leader: Arc<Leader>, address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let leader = Arc::clone(&leader);

        tokio::spawn(async move {
            if let Err(error) = leader.handle_follower(stream).await {
                eprintln!("follower connection failed: {error}");
            }
        });
    }
}
