use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::raft::{
    log::RaftLog,
    node::RaftNode,
    snapshot::RaftSnapshot,
    types::{
        AppendEntriesArgs, ClusterConfig, Command, ConfigChange, LogEntry, NodeId, RequestVoteArgs,
        RequestVoteReply, Role,
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

fn test_log_path(name: &str) -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let dir = std::env::temp_dir()
        .join("kv_store_raft_tests")
        .join(format!("{}_{}_{}", name, std::process::id(), id));

    std::fs::create_dir_all(&dir).expect("create test directory");
    dir.join("raft.log")
}

fn members() -> Vec<NodeId> {
    vec![1, 2, 3]
}

fn set(key: &str, value: &str) -> Command {
    Command::Set {
        key: key.to_owned(),
        value: value.to_owned(),
    }
}

#[test]
fn candidate_wins_with_majority() {
    let mut node =
        RaftNode::open(1, members(), test_log_path("candidate_wins")).expect("open Raft node");

    let request = node.start_election().expect("start election");

    assert_eq!(request.term, 1);
    assert_eq!(node.role, Role::Candidate);
    assert_eq!(node.voted_for, Some(1));

    let became_leader = node
        .record_vote_response(
            2,
            RequestVoteReply {
                term: 1,
                vote_granted: true,
            },
        )
        .expect("record vote response");

    assert!(became_leader);
    assert_eq!(node.role, Role::Leader);
}

#[test]
fn higher_term_makes_candidate_step_down() {
    let mut node =
        RaftNode::open(1, members(), test_log_path("higher_term")).expect("open Raft node");

    let became_leader = node
        .record_vote_response(
            2,
            RequestVoteReply {
                term: 2,
                vote_granted: false,
            },
        )
        .expect("record vote response");

    assert!(!became_leader);
    assert_eq!(node.current_term, 2);
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.voted_for, None);
}

#[test]
fn node_does_not_vote_twice_in_same_term() {
    let mut node =
        RaftNode::open(2, members(), test_log_path("vote_twice")).expect("open Raft node");

    let first = node
        .handle_vote_request(RequestVoteArgs {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        })
        .expect("handle first vote request");

    let second = node
        .handle_vote_request(RequestVoteArgs {
            term: 1,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        })
        .expect("handle second vote request");

    assert!(first.vote_granted);
    assert!(!second.vote_granted);
    assert_eq!(node.voted_for, Some(1));
}

#[test]
fn stale_candidate_log_is_rejected() {
    let mut node =
        RaftNode::open(2, members(), test_log_path("stale_candidate")).expect("open Raft node");

    node.log
        .append(LogEntry {
            term: 2,
            command: set("x", "10"),
        })
        .expect("append existing log entry");

    let reply = node
        .handle_vote_request(RequestVoteArgs {
            term: 3,
            candidate_id: 1,
            last_log_index: 1,
            last_log_term: 1,
        })
        .expect("handle vote request");

    assert!(!reply.vote_granted);
    assert_eq!(node.current_term, 3);
}

#[test]
fn follower_rejects_wrong_previous_log_term() {
    let mut node =
        RaftNode::open(2, members(), test_log_path("wrong_prev_term")).expect("open Raft node");

    node.log
        .append(LogEntry {
            term: 1,
            command: set("x", "10"),
        })
        .expect("append existing log entry");

    let reply = node
        .handle_append_entries(AppendEntriesArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 0,
        })
        .expect("handle AppendEntries");

    assert!(!reply.success);
}

#[test]
fn committed_entry_cannot_be_overwritten() {
    let mut log = RaftLog::open(test_log_path("committed_entry")).expect("open Raft log");

    log.append(LogEntry {
        term: 1,
        command: set("x", "10"),
    })
    .expect("append original entry");

    let result = log.append_from_leader(
        0,
        0,
        &[LogEntry {
            term: 2,
            command: set("x", "20"),
        }],
        1,
    );

    assert!(
        result.is_err(),
        "overwriting a committed entry must be rejected"
    );

    assert_eq!(
        log.term_at(1),
        Some(1),
        "the original committed entry must remain unchanged"
    );
}

#[test]
fn leader_commits_current_term_entry_with_majority() {
    let mut node =
        RaftNode::open(1, members(), test_log_path("leader_commit")).expect("open Raft node");

    node.start_election().expect("start election");

    node.record_vote_response(
        2,
        RequestVoteReply {
            term: 1,
            vote_granted: true,
        },
    )
    .expect("record vote response");

    assert_eq!(node.role, Role::Leader);

    let index = node
        .append_client_command(set("x", "10"))
        .expect("leader should accept command");

    let match_index = HashMap::from([(2, index), (3, 0)]);

    node.advance_commit_index(&match_index);

    assert_eq!(node.commit_index, index);
}

#[test]
fn term_and_vote_survive_restart() {
    let path = test_log_path("persistent_vote");

    {
        let mut node = RaftNode::open(1, members(), &path).expect("open first Raft node");

        node.start_election().expect("start election");

        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));
    }

    {
        let node = RaftNode::open(1, members(), &path).expect("reopen Raft node");

        assert_eq!(node.current_term, 1);
        assert_eq!(node.voted_for, Some(1));
        assert_eq!(node.role, Role::Follower);
    }
}

fn entry(term: u64, key: &str, value: &str) -> LogEntry {
    LogEntry {
        term,
        command: set(key, value),
    }
}

#[test]
fn heartbeat_does_not_commit_unverified_stale_tail() {
    let mut node =
        RaftNode::open(2, members(), test_log_path("stale_tail_commit")).expect("open Raft node");

    node.log.append(entry(1, "x", "1")).expect("append entry 1");
    node.log
        .append(entry(1, "stale", "2"))
        .expect("append stale entry 2");

    // The leader only vouches for index 1, even though leader_commit is 2.
    let reply = node
        .handle_append_entries(AppendEntriesArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 2,
        })
        .expect("handle AppendEntries");

    assert!(reply.success);
    assert_eq!(node.commit_index, 1);
}

#[test]
fn follower_replaces_divergent_tail_with_leader_entries() {
    let mut node =
        RaftNode::open(2, members(), test_log_path("divergent_tail")).expect("open Raft node");

    node.log.append(entry(1, "a", "1")).expect("append 1");
    node.log.append(entry(1, "b", "old")).expect("append 2");
    node.log.append(entry(1, "c", "old")).expect("append 3");

    let reply = node
        .handle_append_entries(AppendEntriesArgs {
            term: 2,
            leader_id: 1,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![entry(2, "b", "new")],
            leader_commit: 0,
        })
        .expect("handle AppendEntries");

    assert!(reply.success);
    assert_eq!(node.log.last_index(), 2);
    assert_eq!(node.log.term_at(2), Some(2));
    assert_eq!(node.log.entry_at(2), Some(&entry(2, "b", "new")));
}

#[test]
fn duplicate_append_entries_leaves_log_unchanged() {
    let mut node =
        RaftNode::open(2, members(), test_log_path("duplicate_append")).expect("open Raft node");

    let args = AppendEntriesArgs {
        term: 1,
        leader_id: 1,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![entry(1, "a", "1"), entry(1, "b", "2")],
        leader_commit: 0,
    };

    assert!(node.handle_append_entries(args.clone()).unwrap().success);

    // A delayed duplicate carrying only the first entry must not truncate.
    let stale = AppendEntriesArgs {
        entries: vec![entry(1, "a", "1")],
        ..args.clone()
    };
    assert!(node.handle_append_entries(stale).unwrap().success);
    assert_eq!(node.log.last_index(), 2);

    assert!(node.handle_append_entries(args).unwrap().success);
    assert_eq!(node.log.last_index(), 2);
}

#[test]
fn leader_steps_down_on_higher_term_append_entries() {
    let mut node =
        RaftNode::open(1, members(), test_log_path("leader_step_down")).expect("open Raft node");

    node.start_election().expect("start election");
    node.record_vote_response(
        2,
        RequestVoteReply {
            term: 1,
            vote_granted: true,
        },
    )
    .expect("record vote response");
    assert_eq!(node.role, Role::Leader);

    let reply = node
        .handle_append_entries(AppendEntriesArgs {
            term: 5,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        })
        .expect("handle AppendEntries");

    assert!(reply.success);
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.current_term, 5);
    assert_eq!(node.voted_for, None);
}

#[test]
fn leader_does_not_commit_old_term_entry_by_counting_replicas() {
    let mut node =
        RaftNode::open(1, members(), test_log_path("old_term_commit")).expect("open Raft node");

    node.log
        .append(entry(1, "old", "1"))
        .expect("append old entry");

    // Move to term 1 via a vote for node 2, then run for term 2.
    node.handle_vote_request(RequestVoteArgs {
        term: 1,
        candidate_id: 2,
        last_log_index: 1,
        last_log_term: 1,
    })
    .expect("handle vote request");

    node.start_election().expect("start election");
    assert_eq!(node.current_term, 2);

    node.record_vote_response(
        2,
        RequestVoteReply {
            term: 2,
            vote_granted: true,
        },
    )
    .expect("record vote response");
    assert_eq!(node.role, Role::Leader);

    // The old-term entry is on a majority, but must not be committed alone.
    node.advance_commit_index(&HashMap::from([(2, 1), (3, 1)]));
    assert_eq!(node.commit_index, 0);

    // A current-term entry on a majority commits it and everything before it.
    let index = node
        .append_client_command(set("new", "2"))
        .expect("leader accepts command");
    node.advance_commit_index(&HashMap::from([(2, index), (3, 0)]));
    assert_eq!(node.commit_index, index);
}

#[test]
fn single_node_cluster_elects_itself() {
    let mut node =
        RaftNode::open(1, vec![1], test_log_path("single_node")).expect("open Raft node");

    node.start_election().expect("start election");

    assert_eq!(node.role, Role::Leader);
}

#[test]
fn invalid_membership_is_rejected() {
    assert!(RaftNode::open(1, vec![], test_log_path("no_members")).is_err());
    assert!(RaftNode::open(9, members(), test_log_path("not_member")).is_err());
    assert!(RaftNode::open(1, vec![1, 2, 2], test_log_path("dup_members")).is_err());
}

#[test]
fn log_survives_restart_and_discards_torn_tail() {
    use std::io::Write;

    let path = test_log_path("log_restart");

    {
        let mut log = RaftLog::open(&path).expect("open Raft log");
        log.append(entry(1, "a", "1")).expect("append 1");
        log.append(entry(2, "b", "2")).expect("append 2");
    }

    // Simulate a crash mid-append: a header promising more bytes than exist.
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("open log for append")
        .write_all(&[0, 0, 0, 50, 1, 2, 3])
        .expect("write torn record");

    let mut log = RaftLog::open(&path).expect("reopen Raft log");

    assert_eq!(log.last_index(), 2);
    assert_eq!(log.last_term(), 2);
    assert_eq!(log.entry_at(2), Some(&entry(2, "b", "2")));

    // The torn bytes are gone, so a new append lands cleanly at index 3.
    assert_eq!(log.append(entry(2, "c", "3")).expect("append 3"), 3);
    drop(log);

    let log = RaftLog::open(&path).expect("reopen Raft log again");
    assert_eq!(log.last_index(), 3);
}

// ---------------------------------------------------------------------------
// In-process cluster helpers: messages are delivered by direct method calls.
// ---------------------------------------------------------------------------

fn cluster(name: &str) -> Vec<RaftNode> {
    members()
        .into_iter()
        .map(|id| {
            RaftNode::open(id, members(), test_log_path(&format!("{name}_{id}")))
                .expect("open Raft node")
        })
        .collect()
}

/// `candidate` starts an election and asks only the nodes in `voters`.
fn elect(nodes: &mut [RaftNode], candidate: usize, voters: &[usize]) {
    let args = nodes[candidate].start_election().expect("start election");

    for &voter in voters {
        let reply = nodes[voter]
            .handle_vote_request(args.clone())
            .expect("handle vote request");
        let voter_id = nodes[voter].id;

        nodes[candidate]
            .record_vote_response(voter_id, reply)
            .expect("record vote response");
    }
}

/// One AppendEntries round trip from leader `from` to follower `to`.
fn deliver(nodes: &mut [RaftNode], from: usize, to: usize) -> bool {
    let to_id = nodes[to].id;
    let args = nodes[from]
        .build_append_entries(to_id, 64)
        .expect("leader builds AppendEntries");

    let reply = nodes[to]
        .handle_append_entries(args.clone())
        .expect("handle AppendEntries");
    let success = reply.success;

    nodes[from]
        .handle_append_entries_reply(to_id, &args, reply)
        .expect("handle AppendEntries reply");

    success
}

#[test]
fn leader_replicates_commits_and_followers_learn_the_commit() {
    let mut nodes = cluster("replicate");

    elect(&mut nodes, 0, &[1]);
    assert_eq!(nodes[0].role, Role::Leader);
    assert_eq!(nodes[0].leader_id, Some(1));

    // The new leader's no-op is entry 1; the client write is entry 2.
    assert_eq!(
        nodes[0].log.entry_at(1).map(|e| &e.command),
        Some(&Command::Noop)
    );
    let index = nodes[0]
        .append_client_command(set("x", "10"))
        .expect("leader accepts command");
    assert_eq!(index, 2);

    assert!(deliver(&mut nodes, 0, 1));
    assert_eq!(
        nodes[0].commit_index, 2,
        "majority (leader + node 2) has it"
    );
    assert_eq!(nodes[0].entries_to_apply().len(), 2);

    // The follower only learns the commit index from the next message.
    assert_eq!(nodes[1].commit_index, 0);
    assert!(deliver(&mut nodes, 0, 1));
    assert_eq!(nodes[1].commit_index, 2);
    assert_eq!(nodes[1].leader_id, Some(1));
    assert_eq!(nodes[1].entries_to_apply().len(), 2);

    nodes[0].mark_applied(2);
    assert!(nodes[0].entries_to_apply().is_empty());
    assert_eq!(nodes[0].last_applied, 2);

    // Applying never runs ahead of the commit index.
    nodes[2].mark_applied(99);
    assert_eq!(nodes[2].last_applied, 0);
}

#[test]
fn leader_backs_up_and_repairs_a_divergent_follower() {
    let mut nodes = cluster("repair");

    // Node 2 holds stale term-1 entries from a leader that never committed them.
    nodes[1]
        .log
        .append(entry(1, "b", "stale"))
        .expect("stale 1");
    nodes[1]
        .log
        .append(entry(1, "c", "stale"))
        .expect("stale 2");

    // Node 1 has a term-2 entry and wins term 2 with node 3's vote.
    nodes[0]
        .log
        .append(entry(2, "a", "1"))
        .expect("leader entry");
    nodes[0].start_election().expect("term 1");
    elect(&mut nodes, 0, &[2]);
    assert_eq!(nodes[0].role, Role::Leader);
    assert_eq!(nodes[0].current_term, 2);

    // First attempt mismatches at prev_log_index 1; the leader backs up.
    assert!(!deliver(&mut nodes, 0, 1));
    // Second attempt overwrites the divergent tail.
    assert!(deliver(&mut nodes, 0, 1));

    assert_eq!(nodes[1].log.last_index(), 2);
    assert_eq!(nodes[1].log.entry_at(1), Some(&entry(2, "a", "1")));
    assert_eq!(nodes[1].log.term_at(2), Some(2));
    assert_eq!(
        nodes[1].log.entry_at(2).map(|e| &e.command),
        Some(&Command::Noop)
    );
    assert_eq!(nodes[0].commit_index, 2);
}

#[test]
fn committed_entry_survives_leader_change() {
    let mut nodes = cluster("failover");

    // Node 1 leads, commits "x" with node 2, then "crashes" before node 3 sees it.
    elect(&mut nodes, 0, &[1]);
    nodes[0]
        .append_client_command(set("x", "10"))
        .expect("leader accepts command");
    assert!(deliver(&mut nodes, 0, 1));
    assert_eq!(nodes[0].commit_index, 2);

    // Node 3 is behind: node 2 must refuse to vote for it.
    elect(&mut nodes, 2, &[1]);
    assert_ne!(nodes[2].role, Role::Leader);

    // Node 2 has the committed entry, so node 3 votes for it.
    elect(&mut nodes, 1, &[2]);
    assert_eq!(nodes[1].role, Role::Leader);
    assert_eq!(
        nodes[1].log.entry_at(2).map(|e| &e.command),
        Some(&set("x", "10"))
    );

    // The new leader brings node 3 up to date.
    while nodes[2].log.last_index() < nodes[1].log.last_index() {
        deliver(&mut nodes, 1, 2);
    }
    assert_eq!(
        nodes[2].log.entry_at(2).map(|e| &e.command),
        Some(&set("x", "10"))
    );
}

#[test]
fn stale_leader_steps_down_when_a_follower_reports_a_higher_term() {
    let mut nodes = cluster("stale_leader");

    elect(&mut nodes, 0, &[1]);
    assert_eq!(nodes[0].role, Role::Leader);

    // Node 2 moved on to a later term while node 1 was partitioned away.
    nodes[1]
        .handle_vote_request(RequestVoteArgs {
            term: 5,
            candidate_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        })
        .expect("handle vote request");

    deliver(&mut nodes, 0, 1);

    assert_eq!(nodes[0].role, Role::Follower);
    assert_eq!(nodes[0].current_term, 5);
    assert!(nodes[0].build_append_entries(2, 64).is_none());
}

#[test]
fn candidate_forgets_the_previous_leader() {
    let mut nodes = cluster("forget_leader");

    elect(&mut nodes, 0, &[1]);
    deliver(&mut nodes, 0, 1);
    assert_eq!(nodes[1].leader_id, Some(1));

    nodes[1].start_election().expect("start election");

    assert_eq!(nodes[1].leader_id, None);
}

// ---------------------------------------------------------------------------
// Log compaction and snapshots
// ---------------------------------------------------------------------------

fn log_with_terms(name: &str, terms: &[u64]) -> (RaftLog, PathBuf) {
    let path = test_log_path(name);
    let mut log = RaftLog::open(&path).expect("open Raft log");

    for (i, term) in terms.iter().enumerate() {
        log.append(entry(*term, &format!("k{}", i + 1), "v"))
            .expect("append entry");
    }

    (log, path)
}

#[test]
fn compaction_keeps_the_suffix_and_survives_restart() {
    let (mut log, path) = log_with_terms("compact_suffix", &[1, 1, 2, 2, 2]);

    log.compact_to(3, 2, ClusterConfig::default())
        .expect("compact");

    assert_eq!(log.base_index(), 3);
    assert_eq!(log.first_index(), 4);
    assert_eq!(log.last_index(), 5);
    assert_eq!(log.term_at(3), Some(2), "boundary answered from the base");
    assert_eq!(log.term_at(2), None, "older entries are gone");
    assert!(log.entry_at(3).is_none());
    assert_eq!(log.entry_at(4), Some(&entry(2, "k4", "v")));
    assert_eq!(log.entries_from(1).len(), 2, "only stored entries returned");

    assert_eq!(log.append(entry(3, "k6", "v")).expect("append"), 6);
    assert!(
        log.truncate_from(3).is_err(),
        "cannot truncate a compacted entry"
    );
    drop(log);

    let log = RaftLog::open(&path).expect("reopen");
    assert_eq!(log.base_index(), 3);
    assert_eq!(log.base_term(), 2);
    assert_eq!(log.last_index(), 6);
    assert_eq!(log.entry_at(6), Some(&entry(3, "k6", "v")));
}

#[test]
fn compacting_past_the_end_or_over_a_conflict_discards_the_whole_log() {
    // Snapshot ahead of the log.
    let (mut log, path) = log_with_terms("compact_ahead", &[1, 1, 1]);
    log.compact_to(10, 4, ClusterConfig::default())
        .expect("compact");

    assert_eq!(log.last_index(), 10);
    assert_eq!(log.last_term(), 4);
    assert_eq!(log.append(entry(4, "next", "v")).expect("append"), 11);
    drop(log);

    let log = RaftLog::open(&path).expect("reopen");
    assert_eq!(log.base_index(), 10);
    assert_eq!(log.last_index(), 11);

    // Snapshot disagrees with the entry at its index.
    let (mut log, _) = log_with_terms("compact_conflict", &[1, 1, 1, 1]);
    log.compact_to(2, 5, ClusterConfig::default())
        .expect("compact");

    assert_eq!(log.last_index(), 2, "conflicting suffix must not survive");
    assert_eq!(log.last_term(), 5);
}

#[test]
fn crash_between_writing_the_base_and_rewriting_the_log_is_recovered() {
    let (mut log, path) = log_with_terms("compact_crash", &[1, 1, 2, 2]);

    let before = std::fs::read(&path).expect("read log before compaction");
    log.compact_to(2, 1, ClusterConfig::default())
        .expect("compact");
    drop(log);

    // Simulate the crash: the new base is durable, the old log file is back.
    std::fs::write(&path, before).expect("restore old log file");

    let log = RaftLog::open(&path).expect("reopen after simulated crash");

    assert_eq!(log.base_index(), 2);
    assert_eq!(log.last_index(), 4);
    assert_eq!(log.entry_at(3), Some(&entry(2, "k3", "v")));
    assert!(log.entry_at(2).is_none());
}

#[test]
fn leader_batches_overlapping_the_snapshot_are_trimmed() {
    let (mut log, _) = log_with_terms("trim_overlap", &[1, 1, 1, 1, 1]);
    log.compact_to(3, 1, ClusterConfig::default())
        .expect("compact");

    let batch = vec![
        entry(1, "k2", "v"),
        entry(1, "k3", "v"),
        entry(1, "k4", "v"),
        entry(1, "k5", "v"),
        entry(1, "k6", "v"),
    ];

    // prev_log_index 1 is inside the snapshot; only entry 6 is new.
    assert!(log.append_from_leader(1, 1, &batch, 3).expect("append"));
    assert_eq!(log.last_index(), 6);
    assert_eq!(log.entry_at(4), Some(&entry(1, "k4", "v")));

    // A batch lying entirely inside the snapshot is a harmless no-op.
    assert!(
        log.append_from_leader(0, 0, &batch[..2], 3)
            .expect("append")
    );
    assert_eq!(log.last_index(), 6);
}

#[test]
fn compacted_node_resumes_from_its_snapshot_and_needs_the_snapshot_file() {
    let path = test_log_path("node_restart_compacted");
    let snapshot_path = path.with_extension("snapshot");

    {
        let mut node = RaftNode::open(2, members(), &path).expect("open node");

        for i in 1..=4 {
            node.log
                .append(entry(1, &format!("k{i}"), "v"))
                .expect("append");
        }

        node.commit_index = 3;
        node.mark_applied(3);

        RaftSnapshot {
            last_included_index: 3,
            last_included_term: 1,
            config: ClusterConfig::new(members()),
            entries: Default::default(),
        }
        .write_atomic(&snapshot_path)
        .expect("write snapshot");

        assert_eq!(node.compaction_point(2), Some((3, 1)));
        assert_eq!(node.compaction_point(0), None, "zero disables compaction");
        assert_eq!(node.compaction_point(4), None, "not enough entries yet");

        assert!(
            node.compact_log_to(4, 1).is_err(),
            "cannot pass last_applied"
        );
        node.compact_log_to(3, 1).expect("compact");
    }

    let node = RaftNode::open(2, members(), &path).expect("reopen node");
    assert_eq!(node.commit_index, 3);
    assert_eq!(node.last_applied, 3);
    assert_eq!(node.log.last_index(), 4);
    drop(node);

    std::fs::remove_file(&snapshot_path).expect("remove snapshot");
    assert!(
        RaftNode::open(2, members(), &path).is_err(),
        "a compacted log without its snapshot must not open"
    );
}

#[test]
fn lagging_follower_is_caught_up_with_a_snapshot() {
    let mut nodes = cluster("snapshot_catch_up");

    // Node 1 leads and commits five client writes together with node 2.
    elect(&mut nodes, 0, &[1]);
    for i in 0..5 {
        nodes[0]
            .append_client_command(set(&format!("k{i}"), "v"))
            .expect("leader accepts command");
    }
    assert!(deliver(&mut nodes, 0, 1));
    assert_eq!(nodes[0].commit_index, 6);

    // The leader snapshots and drops its whole log.
    nodes[0].mark_applied(6);
    nodes[0].compact_log_to(6, 1).expect("compact leader log");
    assert_eq!(nodes[0].log.first_index(), 7);

    // Node 3 never saw anything, so it needs entries that no longer exist.
    assert!(nodes[0].needs_snapshot(3));
    assert!(!nodes[0].needs_snapshot(2));

    // Follower side of InstallSnapshot.
    assert!(nodes[2].observe_leader(1, 1).expect("current term"));
    assert!(!nodes[2].observe_leader(0, 1).expect("stale term"));
    assert!(nodes[2].should_install_snapshot(6));
    nodes[2]
        .finish_snapshot_install(6, 1, ClusterConfig::new(members()))
        .expect("install snapshot");

    assert_eq!(nodes[2].commit_index, 6);
    assert_eq!(nodes[2].last_applied, 6);
    assert_eq!(nodes[2].log.last_index(), 6);
    assert_eq!(nodes[2].log.term_at(6), Some(1));
    assert!(!nodes[2].should_install_snapshot(6), "already have it");

    // Leader side: the acknowledgement moves node 3 onto normal replication.
    nodes[0]
        .handle_install_snapshot_reply(3, 6, 1, 1)
        .expect("handle snapshot reply");
    assert!(!nodes[0].needs_snapshot(3));

    nodes[0]
        .append_client_command(set("after", "snapshot"))
        .expect("leader accepts command");
    assert!(deliver(&mut nodes, 0, 2));
    assert_eq!(nodes[2].log.last_index(), 7);
    assert_eq!(
        nodes[2].log.entry_at(7).map(|e| &e.command),
        Some(&set("after", "snapshot"))
    );
}

// ---------------------------------------------------------------------------
// Membership changes
// ---------------------------------------------------------------------------

/// Three voters (ids 1-3) plus a joining server (id 4) that knows nothing.
fn cluster_with_joiner(name: &str) -> Vec<RaftNode> {
    let mut nodes = cluster(name);

    nodes.push(
        RaftNode::open_joining(4, test_log_path(&format!("{name}_4"))).expect("open joining node"),
    );

    nodes
}

fn voters_of(node: &RaftNode) -> Vec<NodeId> {
    node.config().voters.clone()
}

#[test]
fn config_changes_apply_one_server_at_a_time() {
    let base = ClusterConfig::new(vec![1, 2, 3]);

    let with_learner = ConfigChange::AddLearner(4).apply(&base).unwrap();
    assert_eq!(with_learner.voters, vec![1, 2, 3]);
    assert_eq!(with_learner.learners, vec![4]);
    assert_eq!(
        with_learner.quorum(),
        2,
        "a learner never changes the quorum"
    );

    let promoted = ConfigChange::Promote(4).apply(&with_learner).unwrap();
    assert_eq!(promoted.voters, vec![1, 2, 3, 4]);
    assert!(promoted.learners.is_empty());
    assert_eq!(promoted.quorum(), 3);

    let removed = ConfigChange::Remove(1).apply(&promoted).unwrap();
    assert_eq!(removed.voters, vec![2, 3, 4]);

    assert!(
        ConfigChange::AddLearner(2).apply(&base).is_err(),
        "already a member"
    );
    assert!(
        ConfigChange::Promote(4).apply(&base).is_err(),
        "not a learner"
    );
    assert!(
        ConfigChange::Remove(9).apply(&base).is_err(),
        "not a member"
    );
    assert!(
        ConfigChange::Remove(1)
            .apply(&ClusterConfig::new(vec![1]))
            .is_err(),
        "cannot remove the last voter"
    );
}

#[test]
fn log_reports_the_latest_config_and_keeps_it_across_compaction() {
    let path = test_log_path("log_config");
    let bootstrap = ClusterConfig::new(vec![1, 2, 3]);
    let changed = ClusterConfig {
        voters: vec![1, 2, 3],
        learners: vec![4],
    };

    let mut log = RaftLog::open_with_config(&path, bootstrap.clone()).expect("open");
    assert_eq!(log.latest_config(), (0, bootstrap.clone()));

    log.append(entry(1, "a", "1")).expect("append 1");
    log.append(LogEntry {
        term: 1,
        command: Command::Config(changed.clone()),
    })
    .expect("append config at 2");
    log.append(entry(1, "b", "2")).expect("append 3");

    assert_eq!(log.latest_config(), (2, changed.clone()));
    assert_eq!(
        log.config_as_of(1),
        (0, bootstrap.clone()),
        "before the change"
    );
    assert_eq!(log.config_as_of(3), (2, changed.clone()));

    // Dropping the config entry by compaction must not forget it.
    log.compact_to(3, 1, changed.clone()).expect("compact");
    assert_eq!(log.latest_config(), (0, changed.clone()));
    drop(log);

    // ...even though a restart is given a different bootstrap config.
    let log = RaftLog::open_with_config(&path, bootstrap).expect("reopen");
    assert_eq!(log.base_config(), &changed);
    assert_eq!(log.latest_config().1, changed);
}

#[test]
fn learner_receives_the_log_but_never_counts_toward_commit() {
    let mut nodes = cluster_with_joiner("learner_commit");

    elect(&mut nodes, 0, &[1]);
    assert!(deliver(&mut nodes, 0, 1)); // commits the leader's no-op

    let index = nodes[0]
        .propose_config_change(ConfigChange::AddLearner(4))
        .expect("add learner");
    assert_eq!(
        nodes[0].config().learners,
        vec![4],
        "effective once appended"
    );
    assert_eq!(nodes[0].quorum_size(), 2);

    // The two voters commit it; the learner has acknowledged nothing.
    assert!(deliver(&mut nodes, 0, 1));
    assert_eq!(nodes[0].commit_index, index);
    assert_eq!(nodes[0].match_index_of(4), Some(0));

    // The learner catches up from the beginning and learns the config.
    assert!(!nodes[3].is_voter());
    while nodes[3].log.last_index() < nodes[0].log.last_index() {
        assert!(deliver(&mut nodes, 0, 3));
    }
    assert_eq!(nodes[3].config(), nodes[0].config());
    assert!(!nodes[3].is_voter(), "a learner is not a voter");

    // A new entry that exists only on the leader and the learner is on 2
    // servers, but only 1 of them is a voter: it must NOT commit.
    let index = nodes[0]
        .append_client_command(set("only", "learner"))
        .expect("leader accepts command");
    assert!(deliver(&mut nodes, 0, 3));
    assert_eq!(nodes[3].log.last_index(), index);
    assert!(
        nodes[0].commit_index < index,
        "a learner's copy must not count toward the majority"
    );

    // One more voter makes it a real majority.
    assert!(deliver(&mut nodes, 0, 1));
    assert_eq!(nodes[0].commit_index, index);
}

#[test]
fn one_change_at_a_time_and_only_after_committing_in_the_leaders_term() {
    let mut nodes = cluster_with_joiner("one_at_a_time");

    elect(&mut nodes, 0, &[1]);

    // The new leader's no-op is not committed yet.
    assert!(
        nodes[0]
            .propose_config_change(ConfigChange::AddLearner(4))
            .is_err(),
        "must first commit an entry from the current term"
    );

    assert!(deliver(&mut nodes, 0, 1));

    nodes[0]
        .propose_config_change(ConfigChange::AddLearner(4))
        .expect("first change");

    assert!(
        nodes[0]
            .propose_config_change(ConfigChange::Remove(3))
            .is_err(),
        "the first change has not committed yet"
    );

    assert!(deliver(&mut nodes, 0, 1)); // commits it
    nodes[0]
        .propose_config_change(ConfigChange::Remove(3))
        .expect("allowed once the previous change committed");
}

#[test]
fn promoting_a_learner_raises_the_quorum_to_three_of_four() {
    let mut nodes = cluster_with_joiner("promote");

    elect(&mut nodes, 0, &[1]);
    assert!(deliver(&mut nodes, 0, 1));

    nodes[0]
        .propose_config_change(ConfigChange::AddLearner(4))
        .expect("add learner");
    assert!(deliver(&mut nodes, 0, 1));
    while nodes[3].log.last_index() < nodes[0].log.last_index() {
        assert!(deliver(&mut nodes, 0, 3));
    }

    nodes[0]
        .propose_config_change(ConfigChange::Promote(4))
        .expect("promote");
    assert_eq!(voters_of(&nodes[0]), vec![1, 2, 3, 4]);
    assert_eq!(nodes[0].quorum_size(), 3);

    // Leader + one follower is only 2 of 4: not committed.
    let promote_index = nodes[0].log.last_index();
    assert!(deliver(&mut nodes, 0, 1));
    assert!(nodes[0].commit_index < promote_index);

    // A third copy commits it.
    assert!(deliver(&mut nodes, 0, 3));
    assert_eq!(nodes[0].commit_index, promote_index);
}

#[test]
fn a_server_that_knows_no_config_grants_votes_instead_of_vetoing() {
    let mut nodes = cluster_with_joiner("empty_grants");

    // Server 4 has never heard from a leader, so it knows no voters at all.
    assert!(nodes[3].config().voters.is_empty());

    let reply = nodes[3]
        .handle_vote_request(RequestVoteArgs {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        })
        .expect("handle vote request");

    assert!(
        reply.vote_granted,
        "it cannot know the candidate is not a voter"
    );
    assert_eq!(nodes[3].voted_for, Some(2));
}

#[test]
fn learners_and_removed_servers_cannot_campaign_or_disturb_voters() {
    let mut nodes = cluster_with_joiner("no_campaign");

    // A joining server has no config: it must not start elections.
    assert!(nodes[3].start_election().is_err());

    // A voter ignores vote requests from a server outside its config, and
    // does not even adopt its (higher) term.
    let before = nodes[0].current_term;
    let reply = nodes[0]
        .handle_vote_request(RequestVoteArgs {
            term: before + 5,
            candidate_id: 4,
            last_log_index: 0,
            last_log_term: 0,
        })
        .expect("handle vote request");

    assert!(!reply.vote_granted);
    assert_eq!(nodes[0].current_term, before);
}

#[test]
fn a_leader_that_removes_itself_steps_down_only_after_the_change_commits() {
    let mut nodes = cluster("remove_leader");

    elect(&mut nodes, 0, &[1]);
    assert!(deliver(&mut nodes, 0, 1));

    nodes[0]
        .propose_config_change(ConfigChange::Remove(1))
        .expect("leader removes itself");
    assert_eq!(voters_of(&nodes[0]), vec![2, 3]);
    assert_eq!(
        nodes[0].role,
        Role::Leader,
        "still leading until it commits"
    );

    // The leader no longer counts itself: one follower is 1 of 2, not enough.
    let index = nodes[0].log.last_index();
    assert!(deliver(&mut nodes, 0, 1));
    assert!(nodes[0].commit_index < index);
    assert_eq!(nodes[0].role, Role::Leader);

    // Both remaining voters have it: committed, so the old leader steps down.
    assert!(deliver(&mut nodes, 0, 2));
    assert_eq!(nodes[0].commit_index, index);
    assert_eq!(nodes[0].role, Role::Follower);
    assert!(!nodes[0].is_voter());
}

#[test]
fn a_follower_drops_an_uncommitted_config_when_its_entry_is_overwritten() {
    let mut node = RaftNode::open(2, members(), test_log_path("config_rollback")).expect("open");

    let changed = ClusterConfig {
        voters: vec![1, 2, 3],
        learners: vec![4],
    };

    // Leader 1 (term 1) sends a no-op and a config change.
    let reply = node
        .handle_append_entries(AppendEntriesArgs {
            term: 1,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![
                LogEntry {
                    term: 1,
                    command: Command::Noop,
                },
                LogEntry {
                    term: 1,
                    command: Command::Config(changed.clone()),
                },
            ],
            leader_commit: 0,
        })
        .expect("append");
    assert!(reply.success);
    assert_eq!(node.config(), &changed, "adopted as soon as it is appended");
    assert!(node.has_pending_config());

    // Leader 3 (term 2) never saw that entry and overwrites index 2.
    let reply = node
        .handle_append_entries(AppendEntriesArgs {
            term: 2,
            leader_id: 3,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![entry(2, "x", "1")],
            leader_commit: 0,
        })
        .expect("overwrite");
    assert!(reply.success);

    assert_eq!(
        node.config(),
        &ClusterConfig::new(members()),
        "the rolled-back config must be forgotten"
    );
    assert!(!node.has_pending_config());
}

#[test]
fn a_snapshot_carries_the_config_to_a_joining_server_and_restarts_keep_it() {
    let mut nodes = cluster_with_joiner("snapshot_config");

    elect(&mut nodes, 0, &[1]);
    assert!(deliver(&mut nodes, 0, 1));
    nodes[0]
        .propose_config_change(ConfigChange::AddLearner(4))
        .expect("add learner");
    assert!(deliver(&mut nodes, 0, 1));

    // The leader snapshots everything, dropping the config entry with it.
    let applied = nodes[0].commit_index;
    nodes[0].mark_applied(applied);
    let term = nodes[0].log.term_at(applied).expect("term");
    let config = nodes[0].config_as_of(applied);
    assert_eq!(config.learners, vec![4]);

    nodes[0].compact_log_to(applied, term).expect("compact");
    assert!(nodes[0].needs_snapshot(4));

    // The joiner installs that snapshot and learns the membership from it.
    assert!(nodes[3].observe_leader(1, 1).expect("observe leader"));
    nodes[3]
        .finish_snapshot_install(applied, term, config.clone())
        .expect("install snapshot");

    assert_eq!(nodes[3].config(), &config);
    assert_eq!(nodes[3].commit_index, applied);
    assert!(!nodes[3].is_voter());
}
