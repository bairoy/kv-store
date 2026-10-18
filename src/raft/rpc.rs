//! Raft RPC messages and their wire encoding.
//!
//! One message is one frame payload (see `net::framing`): a tag byte followed
//! by the fields of that message.

use std::io;

use crate::raft::codec::{
    invalid_data, put_bytes, put_entry, put_u32, put_u64, read_bytes, read_entry, read_u8,
    read_u32, read_u64,
};
use crate::raft::types::{
    AppendEntriesArgs, AppendEntriesReply, InstallSnapshotArgs, InstallSnapshotReply,
    RequestVoteArgs, RequestVoteReply,
};

const REQUEST_VOTE: u8 = 1;
const REQUEST_VOTE_REPLY: u8 = 2;
const APPEND_ENTRIES: u8 = 3;
const APPEND_ENTRIES_REPLY: u8 = 4;
const INSTALL_SNAPSHOT: u8 = 5;
const INSTALL_SNAPSHOT_REPLY: u8 = 6;

/// Upper bound on entries preallocated while decoding untrusted input.
const MAX_PREALLOCATED_ENTRIES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftMessage {
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshot(InstallSnapshotArgs),
    InstallSnapshotReply(InstallSnapshotReply),
}

pub fn encode_message(message: &RaftMessage) -> io::Result<Vec<u8>> {
    let mut out = Vec::new();

    match message {
        RaftMessage::RequestVote(args) => {
            out.push(REQUEST_VOTE);
            put_u64(&mut out, args.term);
            put_u64(&mut out, args.candidate_id);
            put_u64(&mut out, args.last_log_index);
            put_u64(&mut out, args.last_log_term);
        }
        RaftMessage::RequestVoteReply(reply) => {
            out.push(REQUEST_VOTE_REPLY);
            put_u64(&mut out, reply.term);
            out.push(u8::from(reply.vote_granted));
        }
        RaftMessage::AppendEntries(args) => {
            out.push(APPEND_ENTRIES);
            put_u64(&mut out, args.term);
            put_u64(&mut out, args.leader_id);
            put_u64(&mut out, args.prev_log_index);
            put_u64(&mut out, args.prev_log_term);
            put_u64(&mut out, args.leader_commit);

            let count = u32::try_from(args.entries.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "too many entries"))?;
            put_u32(&mut out, count);

            for entry in &args.entries {
                put_entry(&mut out, entry)?;
            }
        }
        RaftMessage::AppendEntriesReply(reply) => {
            out.push(APPEND_ENTRIES_REPLY);
            put_u64(&mut out, reply.term);
            out.push(u8::from(reply.success));
        }
        RaftMessage::InstallSnapshot(args) => {
            out.push(INSTALL_SNAPSHOT);
            put_u64(&mut out, args.term);
            put_u64(&mut out, args.leader_id);
            put_u64(&mut out, args.last_included_index);
            put_u64(&mut out, args.last_included_term);
            put_u64(&mut out, args.offset);
            out.push(u8::from(args.done));
            put_bytes(&mut out, &args.data)?;
        }
        RaftMessage::InstallSnapshotReply(reply) => {
            out.push(INSTALL_SNAPSHOT_REPLY);
            put_u64(&mut out, reply.term);
        }
    }

    Ok(out)
}

pub fn decode_message(bytes: &[u8]) -> io::Result<RaftMessage> {
    let mut cursor = 0;

    let message = match read_u8(bytes, &mut cursor)? {
        REQUEST_VOTE => RaftMessage::RequestVote(RequestVoteArgs {
            term: read_u64(bytes, &mut cursor)?,
            candidate_id: read_u64(bytes, &mut cursor)?,
            last_log_index: read_u64(bytes, &mut cursor)?,
            last_log_term: read_u64(bytes, &mut cursor)?,
        }),
        REQUEST_VOTE_REPLY => RaftMessage::RequestVoteReply(RequestVoteReply {
            term: read_u64(bytes, &mut cursor)?,
            vote_granted: read_bool(bytes, &mut cursor)?,
        }),
        APPEND_ENTRIES => {
            let term = read_u64(bytes, &mut cursor)?;
            let leader_id = read_u64(bytes, &mut cursor)?;
            let prev_log_index = read_u64(bytes, &mut cursor)?;
            let prev_log_term = read_u64(bytes, &mut cursor)?;
            let leader_commit = read_u64(bytes, &mut cursor)?;
            let count = read_u32(bytes, &mut cursor)? as usize;

            let mut entries = Vec::with_capacity(count.min(MAX_PREALLOCATED_ENTRIES));
            for _ in 0..count {
                entries.push(read_entry(bytes, &mut cursor)?);
            }

            RaftMessage::AppendEntries(AppendEntriesArgs {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            })
        }
        APPEND_ENTRIES_REPLY => RaftMessage::AppendEntriesReply(AppendEntriesReply {
            term: read_u64(bytes, &mut cursor)?,
            success: read_bool(bytes, &mut cursor)?,
        }),
        INSTALL_SNAPSHOT => RaftMessage::InstallSnapshot(InstallSnapshotArgs {
            term: read_u64(bytes, &mut cursor)?,
            leader_id: read_u64(bytes, &mut cursor)?,
            last_included_index: read_u64(bytes, &mut cursor)?,
            last_included_term: read_u64(bytes, &mut cursor)?,
            offset: read_u64(bytes, &mut cursor)?,
            done: read_bool(bytes, &mut cursor)?,
            data: read_bytes(bytes, &mut cursor)?,
        }),
        INSTALL_SNAPSHOT_REPLY => RaftMessage::InstallSnapshotReply(InstallSnapshotReply {
            term: read_u64(bytes, &mut cursor)?,
        }),
        _ => return Err(invalid_data("unknown Raft message type")),
    };

    if cursor != bytes.len() {
        return Err(invalid_data("trailing bytes after Raft message"));
    }

    Ok(message)
}

fn read_bool(bytes: &[u8], cursor: &mut usize) -> io::Result<bool> {
    match read_u8(bytes, cursor)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(invalid_data("invalid boolean")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::types::{Command, LogEntry};

    fn round_trip(message: RaftMessage) {
        let bytes = encode_message(&message).expect("encode");
        assert_eq!(decode_message(&bytes).expect("decode"), message);
    }

    #[test]
    fn every_message_round_trips() {
        round_trip(RaftMessage::RequestVote(RequestVoteArgs {
            term: 3,
            candidate_id: 2,
            last_log_index: 9,
            last_log_term: 2,
        }));
        round_trip(RaftMessage::RequestVoteReply(RequestVoteReply {
            term: 3,
            vote_granted: true,
        }));
        round_trip(RaftMessage::AppendEntries(AppendEntriesArgs {
            term: 4,
            leader_id: 1,
            prev_log_index: 7,
            prev_log_term: 3,
            entries: vec![
                LogEntry {
                    term: 4,
                    command: Command::Set {
                        key: "name".into(),
                        value: "baiju".into(),
                    },
                },
                LogEntry {
                    term: 4,
                    command: Command::Delete { key: "gone".into() },
                },
                LogEntry {
                    term: 4,
                    command: Command::Noop,
                },
            ],
            leader_commit: 6,
        }));
        round_trip(RaftMessage::AppendEntriesReply(AppendEntriesReply {
            term: 4,
            success: false,
        }));
        round_trip(RaftMessage::InstallSnapshot(InstallSnapshotArgs {
            term: 5,
            leader_id: 2,
            last_included_index: 100,
            last_included_term: 4,
            offset: 4096,
            data: vec![1, 2, 3, 0, 255],
            done: true,
        }));
        round_trip(RaftMessage::InstallSnapshotReply(InstallSnapshotReply {
            term: 5,
        }));
    }

    #[test]
    fn malformed_messages_are_rejected() {
        assert!(decode_message(&[]).is_err());
        assert!(decode_message(&[99]).is_err());
        // truncated RequestVote
        assert!(decode_message(&[REQUEST_VOTE, 0, 0, 0]).is_err());
        // invalid boolean
        let mut bytes = encode_message(&RaftMessage::AppendEntriesReply(AppendEntriesReply {
            term: 1,
            success: true,
        }))
        .unwrap();
        *bytes.last_mut().unwrap() = 7;
        assert!(decode_message(&bytes).is_err());
        // trailing garbage
        let mut bytes = encode_message(&RaftMessage::RequestVoteReply(RequestVoteReply {
            term: 1,
            vote_granted: false,
        }))
        .unwrap();
        bytes.push(0);
        assert!(decode_message(&bytes).is_err());
        // snapshot chunk claiming more bytes than the payload holds
        let mut bytes = vec![INSTALL_SNAPSHOT];
        bytes.extend_from_slice(&[0; 40]);
        bytes.push(0);
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_message(&bytes).is_err());
        // entry count larger than the payload
        let mut bytes = vec![APPEND_ENTRIES];
        bytes.extend_from_slice(&[0; 40]);
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode_message(&bytes).is_err());
    }
}
