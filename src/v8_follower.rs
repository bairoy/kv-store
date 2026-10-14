use crate::framing::encode_frame;
use crate::lsm_store::LsmStore;
use crate::replication::{ReplicationMessage, decode_message, encode_message};
use crate::replication_log::{LogEntry, ReplicationEntry};
use crate::v8_snapshot::SnapshotReceiver;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::sleep;

const MAX_FRAME_SIZE: usize = 1024 * 1024;

pub struct Follower {
    pub store: Arc<Mutex<LsmStore>>,
    offset_path: PathBuf,
}

impl Follower {
    pub fn new(store: LsmStore, offset_path: impl Into<PathBuf>) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
            offset_path: offset_path.into(),
        }
    }

    pub async fn load_offset(&self) -> io::Result<u64> {
        let file_offset = if !self.offset_path.exists() {
            0
        } else {
            let mut file = File::open(&self.offset_path)?;
            let mut text = String::new();
            file.read_to_string(&mut text)?;

            text.trim().parse::<u64>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid follower offset file")
            })?
        };

        let snapshot_offset = {
            let store = self.store.lock().await;
            store.installed_snapshot_offset()?
        };

        Ok(file_offset.max(snapshot_offset))
    }
    pub fn persist_offset(&self, offset: u64) -> io::Result<()> {
        let parent = self
            .offset_path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));

        fs::create_dir_all(parent)?;

        let temp_path = self.offset_path.with_extension("tmp");

        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)?;

            writeln!(file, "{offset}")?;
            file.sync_all()?;
        }

        fs::rename(&temp_path, &self.offset_path)?;

        // Make the rename durable on Unix.
        #[cfg(unix)]
        File::open(parent)?.sync_all()?;

        Ok(())
    }

    async fn send_message(stream: &mut TcpStream, message: &ReplicationMessage) -> io::Result<()> {
        let payload = encode_message(message)?;
        let frame = encode_frame(&payload)?;
        stream.write_all(&frame).await
    }

    async fn read_message(stream: &mut TcpStream) -> io::Result<ReplicationMessage> {
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

        decode_message(&payload)
    }

    async fn apply_entry(
        &self,
        entry: ReplicationEntry,
        last_offset: &mut u64,
        stream: &mut TcpStream,
    ) -> io::Result<()> {
        if entry.offset <= *last_offset {
            // Duplicate delivery: acknowledge the offset already applied.
            return Self::send_message(
                stream,
                &ReplicationMessage::Ack {
                    offset: *last_offset,
                },
            )
            .await;
        }

        if entry.offset != *last_offset + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "replication gap: expected {}, received {}",
                    *last_offset + 1,
                    entry.offset
                ),
            ));
        }

        {
            let mut store = self.store.lock().await;

            match entry.operation {
                LogEntry::Set { key, value } => {
                    store.set(key, value).map_err(|error| {
                        io::Error::new(
                            error.kind(),
                            format!(
                                "follower failed to apply SET at offset {}: {error}",
                                entry.offset
                            ),
                        )
                    })?;
                }

                LogEntry::Delete { key } => {
                    store.delete(key).map_err(|error| {
                        io::Error::new(
                            error.kind(),
                            format!(
                                "follower failed to apply DELETE at offset {}: {error}",
                                entry.offset
                            ),
                        )
                    })?;
                }
            }
        }

        // The LSM write is durable before the offset advances.
        self.persist_offset(entry.offset).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "follower failed to persist offset {}: {error}",
                    entry.offset
                ),
            )
        })?;
        *last_offset = entry.offset;
        println!("Follower: applied entry at offset {}", *last_offset);
        Self::send_message(
            stream,
            &ReplicationMessage::Ack {
                offset: *last_offset,
            },
        )
        .await
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "follower failed to send ACK for offset {}: {error}",
                    *last_offset
                ),
            )
        })
    }

    async fn run_connection(&self, address: &str) -> io::Result<()> {
        let mut stream = TcpStream::connect(address).await?;
        println!("Follower: connected to leader at {address}");
        let mut last_offset = self.load_offset().await?;
        let mut snapshot_receiver: Option<SnapshotReceiver> = None;

        Self::send_message(&mut stream, &ReplicationMessage::Hello { last_offset }).await?;
        println!("Follower: sent Hello with offset {last_offset}");
        loop {
            match Self::read_message(&mut stream).await? {
                ReplicationMessage::Entry(entry) => {
                    println!("Follower: received entry {}", entry.offset);
                    if snapshot_receiver.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "received replication entry during snapshot transfer",
                        ));
                    }

                    self.apply_entry(entry, &mut last_offset, &mut stream)
                        .await?;
                }

                ReplicationMessage::SnapshotBegin {
                    last_included_offset,
                    total_bytes,
                } => {
                    if snapshot_receiver.is_some() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "received nested snapshot transfer",
                        ));
                    }

                    if last_included_offset < last_offset {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "received stale snapshot",
                        ));
                    }

                    snapshot_receiver =
                        Some(SnapshotReceiver::begin(last_included_offset, total_bytes)?);
                }

                ReplicationMessage::SnapshotChunk {
                    last_included_offset,
                    chunk_index,
                    data,
                } => {
                    let receiver = snapshot_receiver.as_mut().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "received snapshot chunk before SnapshotBegin",
                        )
                    })?;

                    receiver.push_chunk(last_included_offset, chunk_index, &data)?;
                }

                ReplicationMessage::SnapshotEnd {
                    last_included_offset,
                    total_chunks,
                } => {
                    let receiver = snapshot_receiver.take().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "received SnapshotEnd before SnapshotBegin",
                        )
                    })?;

                    let snapshot = receiver.finish(last_included_offset, total_chunks)?;

                    // Installation replaces the entire logical database state.
                    // Keep the store locked until the installation is complete.

                    {
                        let mut store = self.store.lock().await;
                        println!(
                            "Follower: installing snapshot at offset {}",
                            snapshot.last_included_offset
                        );
                        store
                            .install_snapshot(&snapshot.entries, snapshot.last_included_offset)
                            .map_err(|error| {
                                io::Error::new(
                                    error.kind(),
                                    format!("failed to install received snapshot: {error}"),
                                )
                            })?;
                    }

                    // The store's snapshot offset is already durable at this point.
                    // If persisting this separate offset file fails, load_offset()
                    // can still recover the installed snapshot offset.
                    self.persist_offset(snapshot.last_included_offset)?;

                    last_offset = snapshot.last_included_offset;

                    println!("Snapshot installed at replication offset {}", last_offset);
                }

                ReplicationMessage::Ack { .. } => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "follower received an unexpected Ack",
                    ));
                }

                ReplicationMessage::Hello { .. } => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "follower received an unexpected Hello",
                    ));
                }
            }
        }
    }
    pub async fn run(&self, leader_address: &str) -> io::Result<()> {
        loop {
            match self.run_connection(leader_address).await {
                Ok(()) => {
                    sleep(Duration::from_secs(1)).await;
                }

                Err(error) => {
                    eprintln!("replication connection failed: {error}");
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}
