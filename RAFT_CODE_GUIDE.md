# Raft Code Guide — read `src/raft/` and have it click

This guide walks through **every file in `src/raft/`**, in the order you should read them.
For each file it answers, in this order:

1. **Which file** is it?
2. **What does it do** (one paragraph)?
3. **What does each function do** (a table)?
4. **How does the code work, line by line**, in simple language?

After the files, **Part 5** follows real requests (an election, a write, a read, a crash) through the
code step by step, with diagrams. **Part 7** tells you which line protects which safety rule.

> **Updated after V9.** The parts below were written when V9 was finished. Since then the code
> gained membership changes, a config in snapshots, sharding, rebalancing and transactions, so
> **line numbers in `node.rs`, `log.rs`, `server.rs`, `snapshot.rs` and `rpc.rs` have drifted**
> (function names are still right). Read **[Appendix A](#appendix-a--what-changed-after-v9)** at the
> end for what is new.
>
> Line numbers like `node.rs:169` refer to the code as it is today. If you edit the files, the numbers
> will drift, but the function names stay the same.

---

## Table of contents

- [Part 0 — How to read this guide](#part-0--how-to-read-this-guide)
- [Part 1 — Raft concepts you need first](#part-1--raft-concepts-you-need-first)
- [Part 2 — The big picture](#part-2--the-big-picture)
- [Part 3 — Rust cheat-sheet for this code](#part-3--rust-cheat-sheet-for-this-code)
- [Part 4 — The files, in reading order](#part-4--the-files-in-reading-order)
  - [4.1 `types.rs`](#41-typesrs--the-vocabulary)
  - [4.2 `fsutil.rs`](#42-fsutilrs--crash-safe-file-writes)
  - [4.3 `persistence.rs`](#43-persistencers--term-and-vote-on-disk)
  - [4.4 `codec.rs`](#44-codecrs--bytes-in-bytes-out)
  - [4.5 `rpc.rs`](#45-rpcrs--the-messages-on-the-wire)
  - [4.6 `snapshot.rs`](#46-snapshotrs--the-state-machine-on-disk)
  - [4.7 `log.rs`](#47-logrs--the-durable-log)
  - [4.8 `node.rs`](#48-noders--the-raft-brain)
  - [4.9 `server.rs`](#49-serverrs--timers-network-and-the-store)
  - [4.10 `mod.rs`, `main_raft.rs`](#410-modrs-and-main_raftrs)
  - [4.11 `sim.rs` and `tests.rs`](#411-simrs-and-testsrs--the-proof)
- [Part 5 — Follow real requests through the code](#part-5--follow-real-requests-through-the-code)
- [Part 6 — Locks and concurrency map](#part-6--locks-and-concurrency-map)
- [Part 7 — Safety rule → code → "what if I delete it?"](#part-7--safety-rule--code--what-if-i-delete-it)
- [Part 8 — Files on disk and crash-safety ordering](#part-8--files-on-disk-and-crash-safety-ordering)
- [Part 9 — Exercises](#part-9--exercises)
- [Part 10 — Known simplifications and quirks](#part-10--known-simplifications-and-quirks)

---

# Part 0 — How to read this guide

**Reading order matters.** Each file only uses things from files you already read:

```
types.rs ──► fsutil.rs ──► persistence.rs ──► codec.rs ──► rpc.rs ──► snapshot.rs
                                                                          │
                          log.rs  ◄───────────────────────────────────────┘
                            │
                            ▼
                         node.rs      (the protocol: pure logic, no network)
                            │
                            ▼
                        server.rs     (timers + TCP + applying to the store)
                            │
                            ▼
                    sim.rs / tests.rs (proof that it is correct)
```

**How to study each function** (do this, don't just read):

1. Read only the function *name and signature*. Guess in one sentence what it does.
2. Read the explanation here, then the real code.
3. Ask of every `if`: *"what bad thing happens if I delete this?"* Part 7 gives you the answers.
4. Run the related test: `cargo test --lib raft::tests::<name>`.

**Legend used in diagrams**

```
 N1, N2, N3   = nodes (servers) with ids 1, 2, 3
 t1, t2 ...   = a term number
 [3:t2 SET x] = log entry at INDEX 3, written in TERM 2, command "SET x"
 ───►         = a message or a function call
```

---

# Part 1 — Raft concepts you need first

Skip nothing here. Every line of code in `src/raft/` exists to protect one of these ideas.

## 1.1 The problem Raft solves

You have a key-value store (`LsmStore`). You want **three copies** on three machines so that if one
dies, data is not lost and the service keeps working. The hard part: all three copies must apply the
**same operations in the same order**, even when machines crash, restart, and the network drops
messages.

Raft's idea: keep a **log** of operations. Make all nodes agree on the log. Then every node just
applies the log to its own store, in order, and all stores end up identical.

```
   client: SET name baiju
        │
        ▼
   ┌─────────┐      same log, same order       ┌─────────┐
   │ Node 1  │◄───────────────────────────────►│ Node 2  │
   │ log     │                                 │ log     │
   │ store   │          ┌─────────┐            │ store   │
   └─────────┘          │ Node 3  │            └─────────┘
                        │ log     │
                        │ store   │
                        └─────────┘
```

## 1.2 Three roles

Every node is always in exactly one role (`Role` in `types.rs`):

| Role | Meaning | What it does |
|---|---|---|
| **Follower** | Default. Passive. | Answers requests from the leader and candidates. |
| **Candidate** | "I want to be leader." | Asks everyone for votes. |
| **Leader** | Won the election. | Accepts client writes, sends log entries to followers. |

```
                         election timer fires
        ┌───────────────────────────────────────────────┐
        │                                               ▼
   ┌──────────┐                                   ┌───────────┐
   │ FOLLOWER │◄──────────────────────────────────│ CANDIDATE │──┐ timer fires again
   └──────────┘   sees a higher term, or          └───────────┘◄─┘ (split vote) → new election
        ▲         sees a current leader                │
        │                                              │ gets votes from a MAJORITY
        │   sees a higher term                         ▼
        │                                         ┌──────────┐
        └─────────────────────────────────────────│  LEADER  │
                                                  └──────────┘
```

## 1.3 Terms = a logical clock

Wall clocks lie, so Raft uses a counter called the **term**. Time is cut into terms: 1, 2, 3 …

- Each election starts a **new term** (`current_term + 1`).
- **At most one leader per term.** (Safety rule #1.)
- Every message carries the sender's term. **If a node sees a higher term, it immediately becomes a
  follower and adopts that term.** A leader that was cut off from the network and comes back will
  learn "there's a newer term" and step down by itself.

```
 term 1            term 2                 term 3
 ├── N1 leader ───┤├── (election fails) ──┤├── N2 leader ─────────────►
                   N1 crashes
```

## 1.4 The log

The log is a list of **entries**. An entry has an **index** (position, starting at 1), a **term** (the
term of the leader that created it), and a **command**.

```
 index:    1       2         3         4        5       6
        ┌───────┬─────────┬─────────┬────────┬───────┬─────────┐
 term:  │   1   │    1    │    2    │   2    │   3   │    3    │
 cmd:   │ Noop  │ SET a 1 │ SET b 2 │ DEL a  │ Noop  │ SET c 3 │
        └───────┴─────────┴─────────┴────────┴───────┴─────────┘
                                      ▲
                              commit_index = 4  → entries 1..4 are safe forever
```

Two facts about logs that Raft guarantees (the **Log Matching Property**):

1. If two logs have an entry with the **same index and the same term**, they hold the **same command**.
2. And then **all entries before it are identical too**.

This is why the code checks `(prev_log_index, prev_log_term)` before appending (Section 1.7).

## 1.5 Majority (quorum) — the idea behind everything

With **N** nodes, a majority is `N/2 + 1` (3 nodes → 2, 5 nodes → 3; see `quorum_size()`).

**Any two majorities overlap in at least one node.** That single fact is the whole safety argument:

```
 5 nodes:   A  B  C  D  E
 majority 1: A  B  C
 majority 2:       C  D  E          ← C is in both
```

- A candidate needs a majority of votes → two leaders can't both win the same term.
- A write is *committed* once a majority has it → any future leader's voting majority overlaps with
  it → the future leader knows about every committed write.

## 1.6 Elections

A follower that hears nothing from a leader for a **random** time (300–600 ms here) becomes a
candidate:

1. Increase `current_term`, vote for itself, **save both to disk**.
2. Send `RequestVote(term, my id, my last log index, my last log term)` to all peers.
3. A peer grants its vote only if **all** are true:
   - the candidate's term is not older than the peer's term,
   - the peer hasn't already voted for someone else **in this term**,
   - the candidate's log is **at least as up to date** as the peer's (compare last term first, then
     last index).
4. A candidate with votes from a majority becomes leader.

Why the "up to date" rule? So a node that missed committed entries can **never** win and overwrite
them.

Why random timeouts? If all nodes timed out together they'd keep splitting the vote. Random timeouts
let one node usually go first.

## 1.7 Replication: `AppendEntries`

The leader sends each follower: *"Here are entries after index P. Entry P has term T. My commit index
is C."*

```
 AppendEntries { term, leader_id, prev_log_index=P, prev_log_term=T, entries=[...], leader_commit=C }
```

The follower **only accepts** if its own entry at index `P` has term `T` (the consistency check). If
not it replies `success = false`, and the leader tries again starting one entry earlier. Repeating
this finds the last point where the logs agree; everything after it in the follower gets **replaced**
by the leader's entries.

```
 Leader:   [1:t1][2:t1][3:t2][4:t2]                 ← the truth
 Follower: [1:t1][2:t1][3:t1][4:t1][5:t1]           ← old, wrong tail from a dead leader

 try prev=4  → follower has 4:t1 but leader says 4:t2 → success=false
 try prev=3  → follower has 3:t1 but leader says 3:t2 → success=false
 try prev=2  → 2:t1 matches → follower deletes 3,4,5 and appends leader's [3:t2][4:t2] ✓
```

A heartbeat is just an `AppendEntries` with `entries = []`. It also resets the follower's election
timer, which is how followers know the leader is alive.

## 1.8 Commit and apply

- An entry is **committed** when the leader knows it is stored on a **majority**. Committed entries
  are never lost or changed.
- The leader tells followers its `commit_index` inside later `AppendEntries` (`leader_commit`).
- **Applying** means running the command on the real store (`LsmStore::set/delete`). A node applies
  entries from `last_applied + 1` up to `commit_index`, in order.

```
   log:           [1][2][3][4][5][6][7]
                              ▲        ▲
                    commit_index=4     last_index=7  (5,6,7 are not yet safe)
              ▲
        last_applied=3   (entry 4 is committed but not applied yet)
```

### The tricky rule: only count replicas for entries of the *current* term

A leader may only decide "this entry is committed" by counting replicas if the entry is from its **own
term**. Older entries become committed *indirectly* when a current-term entry after them commits.
(The Raft paper's "Figure 8" shows data loss if you break this.) This is why a new leader appends a
**no-op entry** immediately: it gives it a current-term entry to commit.

## 1.9 What must survive a crash

| State | Persistent? | Where in code |
|---|---|---|
| `current_term` | **Yes** | `persistence.rs` |
| `voted_for` | **Yes** | `persistence.rs` |
| the log | **Yes** | `log.rs` |
| `commit_index`, `last_applied` | No (rebuilt) | `node.rs` |
| `role`, `leader_id`, `next_index`, `match_index` | No | `node.rs` |

*Why persist the vote?* If a node voted for A in term 5, crashed, restarted, forgot, and voted for B
in term 5, two leaders could win term 5.

## 1.10 Snapshots (log compaction)

The log would grow forever. Once entries 1..N are applied, a node can **save the whole key-value
state** (a *snapshot* "as of index N") and delete log entries 1..N.

```
 before:  [1][2][3][4][5][6][7][8]                  after snapshot at 5:
          ▲___applied through 5___▲                 snapshot(index=5, term=t) + log: [6][7][8]
```

If a follower is so far behind that the leader no longer *has* the entries it needs, the leader sends
the snapshot instead (`InstallSnapshot`).

## 1.11 Linearizable reads

"Linearizable" means a read never returns stale data. A leader can't just read its local store: it
might have been deposed without knowing it (e.g. it was paused). So before answering a `GET` it
confirms with a **majority** that it is still the leader (this is called *ReadIndex*, implemented by
`read_barrier()` in `server.rs`).

## 1.12 Concept → where it lives

| Concept | Function(s) |
|---|---|
| Roles, terms | `Role` in `types.rs`; `RaftNode.role`, `current_term` in `node.rs` |
| Start an election | `RaftNode::start_election` |
| Decide whether to grant a vote | `RaftNode::handle_vote_request` |
| Count votes, become leader | `RaftNode::record_vote_response`, `become_leader` |
| Follower accepts entries / consistency check | `RaftNode::handle_append_entries` → `RaftLog::append_from_leader` |
| Leader tracks followers | `next_index`, `match_index`; `build_append_entries`, `handle_append_entries_reply` |
| Commit rule | `RaftNode::advance_commit_index` |
| Apply to the store | `RaftNode::entries_to_apply`, `mark_applied`; `RaftServer::apply_committed` |
| Timers | `RaftServer::run_ticker`, `reset_election_timer` |
| Snapshots | `snapshot.rs`; `RaftLog::compact_to`; `RaftServer::compact_if_needed`, `send_snapshot`, `handle_install_snapshot` |
| Linearizable read | `RaftServer::read_barrier` |

---

# Part 2 — The big picture

## 2.1 Two halves: a pure core and an async shell

The most important design decision in this folder:

```
┌─────────────────────────────── server.rs  (the SHELL) ─────────────────────────────────┐
│  timers · TCP sockets · client commands · applying to the store · tokio locks          │
│                                                                                        │
│      "something happened" ──calls──►  ┌────────────── node.rs (the CORE) ─────────┐   │
│                                       │  RaftNode: pure protocol decisions         │   │
│      ◄──returns what to do──          │  no sockets, no timers, no async           │   │
│                                       │                 │                           │   │
│                                       │                 ▼                           │   │
│                                       │        log.rs  (RaftLog)                    │   │
│                                       │        persistence.rs (term + vote)         │   │
│                                       └─────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

Why? Because the **core is easy to test**: you can call `handle_vote_request(...)` directly in a unit
test, or run 5 nodes in `sim.rs` with a fake network. The shell is thin glue.

## 2.2 Which file uses which

```
                     types.rs  (vocabulary — used by everyone)
                         ▲
        ┌────────────────┼────────────────────────┐
        │                │                        │
     codec.rs ◄──────── rpc.rs                persistence.rs
        ▲                                        ▲
        │          fsutil.rs ◄── snapshot.rs     │
        │              ▲             ▲           │
        │              │             │           │
        └──────────── log.rs         │           │
                         ▲           │           │
                         └─────── node.rs ───────┘
                                     ▲
                                  server.rs ──uses──► net/framing.rs, net/protocol.rs,
                                                      storage/lsm (LsmStore)
```

## 2.3 What `server.rs` borrows from outside `raft/`

| From | What | Used for |
|---|---|---|
| `net/framing.rs` | `encode_frame`, `FrameDecoder` | Every TCP message is `[4-byte length][payload]` so the receiver knows where one message ends. |
| `net/protocol.rs` | `parse_request`, `encode_response`, `Response`, `Command` | The text protocol for **clients** (`SET k v`, `GET k`, `DELETE k`). |
| `storage/lsm` | `LsmStore::set/delete/get/snapshot_entries/install_snapshot` | The real key-value store the Raft log is applied to. |

## 2.4 The tasks that run inside one node

When you run `cargo run --bin main_raft -- 1`, `RaftServer::run` starts three long-running loops, and
those loops spawn short-lived tasks:

```
RaftServer::run  (tokio::try_join!)
 │
 ├── run_peer_server ──► for each incoming TCP connection from another node:
 │                          handle_peer ─► handle_peer_message ─► node.handle_vote_request
 │                                                             ├► node.handle_append_entries
 │                                                             └► handle_install_snapshot
 │
 ├── run_client_server ─► for each incoming TCP connection from a client:
 │                          handle_client ─► execute_command ─► read_barrier / replicate_write
 │
 └── run_ticker  (wakes every 20 ms)
        ├─ if I'm leader and 100 ms passed → broadcast_append → spawns replicate_to(peer) per peer
        └─ if I'm not leader and my timer expired → start_election → spawns request_vote(peer) per peer
```

---

# Part 3 — Rust cheat-sheet for this code

Read this once; the rest of the guide assumes it.

| You see | It means |
|---|---|
| `io::Result<T>` | `Result<T, io::Error>`. Almost every function that touches disk returns this. |
| `foo()?` | If `foo()` returned `Err`, **return that error from the current function now**; otherwise unwrap the `Ok` value. |
| `Option<T>` | `Some(value)` or `None` (e.g. "no vote cast yet" is `voted_for: None`). |
| `let Some(x) = opt else { return ... };` | "Unwrap `opt`; if it's `None`, run the `else` block (which must leave the function)." |
| `if let Some(x) = opt { ... }` | Run the block only when `opt` is `Some`. |
| `&self` / `&mut self` | Method that only reads / may modify the struct. |
| `pub` | Visible outside the module. Fields without `pub` (like `members`) are private. |
| `u64::checked_add(a, b)` | Add, but give `None` instead of wrapping around on overflow. Used so a corrupt value can't panic or silently wrap. |
| `x.max(y)`, `x.min(y)` | Larger / smaller of two numbers. |
| `impl AsRef<Path>` | "Accepts a `&str`, `String`, `PathBuf`, `&Path` ..." — anything path-like. |
| `#[derive(Debug, Clone, PartialEq, Eq)]` | Auto-generate printing, copying, and `==`. |
| `Vec<u8>` / `&[u8]` | A growable byte buffer / a borrowed slice of bytes. |
| `Arc<T>` | A shared, reference-counted pointer: many tasks can hold the same `RaftServer`. |
| `tokio::sync::Mutex<T>` | An async lock. `.lock().await` waits without blocking the thread. Safe to hold across `.await`. |
| `std::sync::Mutex<T>` | A normal lock. Only used for the tiny election deadline (no `.await` while held). |
| `async fn` / `.await` | A function that can pause while waiting (for a socket, a timer, a lock). |
| `tokio::spawn(async move { ... })` | Run a task in the background, concurrently. |

**Pattern you'll see everywhere — "undo on failure":**

```rust
let old_term = self.current_term;
self.current_term = next_term;
if let Err(error) = self.persist_state() {   // try to save to disk
    self.current_term = old_term;            // saving failed → put memory back as it was
    return Err(error);
}
```

Memory and disk must never disagree: the node must not *act* on a term it couldn't save.

---

# Part 4 — The files, in reading order

---

## 4.1 `types.rs` — the vocabulary

**File:** `src/raft/types.rs` (84 lines)

**What it does:** Defines the *nouns* of Raft: roles, commands, log entries, and the six message
structs. It contains **no logic**. Every other file imports from here. If you understand these 84
lines, every later function signature will make sense.

### What is in it

| Type | Lines | Meaning |
|---|---|---|
| `NodeId` | 3 | Just `u64`. A node's number (1, 2, 3). |
| `Role` | 5–10 | `Follower`, `Candidate`, `Leader`. |
| `Command` | 12–23 | The operation stored in a log entry: `Set`, `Delete`, or `Noop`. |
| `LogEntry` | 25–29 | One log slot: `term` + `command`. (The *index* is not stored in the struct — it's the position in the log.) |
| `RequestVoteArgs` / `RequestVoteReply` | 31–43 | "Vote for me" and the answer. |
| `AppendEntriesArgs` / `AppendEntriesReply` | 45–59 | "Here are entries / heartbeat" and the answer. |
| `InstallSnapshotArgs` / `InstallSnapshotReply` | 61–78 | One chunk of a snapshot, and the answer. |
| `impl Display for Role` | 80–84 | So you can print a role with `{}`. |

### Line by line

```rust
pub type NodeId = u64;                                   // line 3
```
A *type alias*: `NodeId` and `u64` are the same type. It exists only so the code reads
`candidate_id: NodeId` instead of `candidate_id: u64`.

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]              // line 5
pub enum Role { Follower, Candidate, Leader }
```
- `Copy` means a `Role` can be copied freely like a number (so `self.role == Role::Leader` doesn't move
  anything).
- `PartialEq` lets you compare roles with `==`.

```rust
pub enum Command {                                        // lines 12-23
    Set { key: String, value: String },
    Delete { key: String },
    Noop,
}
```
- `Set` / `Delete` are real client operations.
- `Noop` ("no operation") does nothing when applied. A **new leader appends one** so it has an entry
  from its own term to commit (Part 1.8). Without it, old-term entries could stay uncommitted until a
  client happens to write.
- No `Copy` here (it holds `String`s), only `Clone`.

```rust
pub struct LogEntry { pub term: u64, pub command: Command }      // lines 25-29
```
The thing in the log. **Index is implicit** — entry #3 is the third slot. `RaftLog` computes indexes.

```rust
pub struct RequestVoteArgs {                              // lines 31-37
    pub term: u64,            // the candidate's term
    pub candidate_id: NodeId, // who is asking
    pub last_log_index: u64,  // index of the candidate's last entry
    pub last_log_term: u64,   // term of the candidate's last entry
}
pub struct RequestVoteReply { pub term: u64, pub vote_granted: bool }   // lines 39-43
```
The last two fields of the args are how a voter judges "is your log at least as up to date as mine?".
The reply carries the voter's term so a candidate with an old term learns it is behind.

```rust
pub struct AppendEntriesArgs {                            // lines 45-53
    pub term: u64,            // leader's term
    pub leader_id: NodeId,    // so followers know who the leader is
    pub prev_log_index: u64,  // entry just BEFORE the new ones...
    pub prev_log_term: u64,   // ...and its term (the consistency check)
    pub entries: Vec<LogEntry>, // new entries; EMPTY = heartbeat
    pub leader_commit: u64,   // leader's commit_index
}
pub struct AppendEntriesReply { pub term: u64, pub success: bool }       // lines 55-59
```

```rust
pub struct InstallSnapshotArgs {                          // lines 61-73
    pub term, pub leader_id,
    pub last_included_index: u64,  // snapshot covers entries 1..=this
    pub last_included_term: u64,   // term of that last entry
    pub offset: u64,               // where this chunk starts inside the snapshot bytes
    pub data: Vec<u8>,             // the chunk
    pub done: bool,                // true on the last chunk
}
pub struct InstallSnapshotReply { pub term: u64 }
```
A snapshot can be large, so it's sent in chunks. `offset` + `done` let the receiver reassemble it.

> **Notice:** every reply has a `term` field. That is how "I see a higher term" travels back to the
> sender.

---

## 4.2 `fsutil.rs` — crash-safe file writes

**File:** `src/raft/fsutil.rs` (50 lines)

**What it does:** One job: *replace a file's contents so that after a crash you see either the old
contents or the new contents — never half of each.* It's used for the small "base" file in the log
and for snapshot files.

### Functions

| Function | Lines | What it does |
|---|---|---|
| `write_file_atomic(path, bytes)` | 9–25 | Write `bytes` into a temp file, fsync it, rename it over `path`, fsync the folder. |
| `temp_path_for(path)` | 27–31 | `foo.snapshot` → `foo.snapshot.tmp`. |
| `sync_parent_directory(path)` | 33–40 | fsync the folder that contains `path`. |
| `sync_directory` (two versions) | 42–50 | The real fsync on Unix; a no-op on other platforms (`#[cfg(unix)]`). |

### The idea, as a picture

```
 step 1: write bytes into  data.tmp        (data.tmp may be half-written if we crash here;
 step 2: fsync(data.tmp)                    the real file `data` is untouched and still valid)
 step 3: rename data.tmp ──► data           (rename is ATOMIC: instant, all-or-nothing)
 step 4: fsync(folder)                      (make the rename itself survive a power cut)
```

### Line by line

```rust
pub fn write_file_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let temp = temp_path_for(path);                  // 10: pick "<path>.tmp"
    {
        let mut file = OpenOptions::new()
            .create(true).truncate(true).write(true) // 13-16: create the temp file, empty it if it exists
            .open(&temp)?;                           // 17: open it (error → return)
        file.write_all(bytes)?;                      // 19: write everything
        file.sync_all()?;                            // 20: force it onto the disk (fsync)
    }                                                // 21: the braces close `file` (drops the handle)
    fs::rename(&temp, path)?;                        // 23: atomically swap it into place
    sync_parent_directory(path)                      // 24: persist the rename; its result is returned
}
```

`sync_parent_directory` finds the folder: `path.parent()` gives `Some(folder)`; the `.filter(...)`
rejects an empty path (when `path` is just `"raft.base"`, the parent is `""`), and
`.unwrap_or_else(|| Path::new("."))` falls back to the current folder.

---

## 4.3 `persistence.rs` — term and vote on disk

**File:** `src/raft/persistence.rs` (≈190 lines, of which the first ~125 are real code; the rest are
tests)

**What it does:** Saves and loads the two values that must survive a restart: **`current_term`** and
**`voted_for`** (Part 1.9).

### Functions

| Function | Lines | What it does |
|---|---|---|
| `PersistentState::load(path)` | 17–58 | Read the state file. If it doesn't exist → defaults (term 0, no vote). If it's malformed → error. |
| `PersistentState::save(&self, path)` | 60–102 | Write the 21-byte file atomically. |
| `temp_path_for` | 105–109 | `x.state` → `x.state.tmp`. |
| `invalid_data` | 111–113 | Shortcut to build an `InvalidData` error. |
| `sync_directory` | 115–… | fsync the folder (Unix only). |

### The file format (21 bytes)

```
 byte:   0 1 2 3 │ 4 ........ 11 │   12   │ 13 ........ 20
        ┌────────┼───────────────┼────────┼────────────────┐
        │ "RAFT" │ current_term  │ marker │ voted_for id   │
        │ magic  │ u64 big-endian│ 0 or 1 │ u64 big-endian │
        └────────┴───────────────┴────────┴────────────────┘
 marker = 0 → no vote cast (the 8 id bytes must all be 0)
 marker = 1 → voted for the node id in the last 8 bytes
```

### `load` line by line

```rust
let mut file = match File::open(path) {                  // 20
    Ok(file) => file,
    Err(error) if error.kind() == io::ErrorKind::NotFound => {
        return Ok(Self::default());                      // 23: first run → term 0, no vote
    }
    Err(error) => return Err(error),                     // 25: any other error is real
};
```
`#[derive(Default)]` (line 10) is what makes `Self::default()` give `{ current_term: 0, voted_for: None }`.

```rust
let mut bytes = Vec::new();
file.read_to_end(&mut bytes)?;                            // 28-29: read the whole file into memory
if bytes.len() != STATE_SIZE { return Err(...) }          // 31: must be exactly 21 bytes
if &bytes[0..4] != MAGIC { return Err(...) }              // 35: must start with "RAFT"
let current_term = u64::from_be_bytes(bytes[4..12].try_into().unwrap());  // 39
```
`bytes[4..12]` is 8 bytes; `.try_into().unwrap()` turns that slice into `[u8; 8]` (cannot fail
because of the length check above); `from_be_bytes` turns the 8 big-endian bytes into a number.

```rust
let voted_for = match bytes[12] {                         // 41-52
    0 => { if bytes[13..21].iter().any(|b| *b != 0) { return Err(...) } None }
    1 => Some(u64::from_be_bytes(bytes[13..21].try_into().unwrap())),
    _ => return Err(invalid_data("invalid vote marker")),
};
```
`0` → nobody voted for (and the id bytes must be zero, otherwise the file is corrupt). `1` → read the id.
Anything else → corrupt.

### `save` line by line

```rust
let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));  // 63-66
fs::create_dir_all(parent)?;                              // 68: make sure the folder exists
let temp_path = temp_path_for(path);                      // 70
let mut bytes = Vec::with_capacity(STATE_SIZE);           // 72: build the 21 bytes in memory
bytes.extend_from_slice(MAGIC);                           // 73: "RAFT"
bytes.extend_from_slice(&self.current_term.to_be_bytes());// 74: term
match self.voted_for {                                    // 76-85
    Some(node_id) => { bytes.push(1); bytes.extend_from_slice(&node_id.to_be_bytes()); }
    None          => { bytes.push(0); bytes.extend_from_slice(&0_u64.to_be_bytes()); }
}
// 87-96: write temp file + fsync  (same recipe as fsutil::write_file_atomic)
fs::rename(&temp_path, path)?;                            // 98: atomic swap
sync_directory(parent)?;                                  // 99: persist the rename
```

> `persistence.rs` has its own copy of the "temp + rename" code instead of calling `fsutil`. It
> behaves the same — it's just older code. (See Part 10.)

---

## 4.4 `codec.rs` — bytes in, bytes out

**File:** `src/raft/codec.rs` (166 lines)

**What it does:** A tiny toolbox to turn numbers, strings and commands into bytes (`put_*`) and back
(`read_*`). Both the **log file** and the **network messages** use it, so they agree on one format.

**Format rules:** integers are **big-endian** (most significant byte first); a string/byte-string is a
`u32` length followed by the bytes.

### Functions

| Function | Lines | Does |
|---|---|---|
| `invalid_data(msg)` | 13–15 | Build an `InvalidData` error. |
| `put_u32`, `put_u64` | 17–23 | Append a number's 4 / 8 big-endian bytes. |
| `put_string` | 25–35 | Append `[u32 len][utf-8 bytes]`. |
| `put_bytes` | 37–45 | Same for raw bytes. |
| `put_command` | 47–62 | Append a tag byte (1=Set, 2=Delete, 3=Noop) then its strings. |
| `put_entry` | 64–68 | Append `[u64 term]` + the command. |
| `read_u8`, `read_u32`, `read_u64` | 70–109 | Read a number at the cursor and move the cursor forward. |
| `read_string` | 111–129 | Read `[u32 len][bytes]` as a `String`. |
| `read_bytes` | 131–145 | Same, as `Vec<u8>`. |
| `read_command`, `read_entry` | 147–166 | Inverse of `put_command`, `put_entry`. |

### The "cursor" pattern

Every `read_*` function takes `bytes: &[u8]` and `cursor: &mut usize`. The cursor is "how far I've
read". Each read advances it:

```
 bytes:   [ 00 00 00 00 00 00 00 05 | 01 | 00 00 00 01 61 | ... ]
            └──── read_u64 ────────┘  └ read_u8 ┘ └─ read_string ─┘
 cursor:  0 ───────────────────────► 8 ──────► 9 ───────────────► 14
```

### `read_u64` line by line (the other readers are the same shape)

```rust
pub fn read_u64(bytes: &[u8], cursor: &mut usize) -> io::Result<u64> {
    let end = cursor.checked_add(8)                       // 96: where this number would end
        .ok_or_else(|| invalid_data("record offset overflow"))?;   // overflow → error, not a panic
    let slice = bytes.get(*cursor..end)                   // 100: take bytes[cursor..end] — or None if it runs off the end
        .ok_or_else(|| invalid_data("truncated integer"))?;        // not enough bytes → error
    let mut array = [0u8; 8];
    array.copy_from_slice(slice);                         // 104-105: copy the 8 bytes into an array
    *cursor = end;                                        // 106: advance the cursor
    Ok(u64::from_be_bytes(array))                         // 108: convert
}
```
**Why so careful?** These bytes come from disk or from the network. They could be truncated or
malicious. Using `.get(..)` and `checked_add` means bad input becomes an `Err`, never a crash.

### `put_command` / `read_command`

```rust
Command::Set { key, value } => { out.push(COMMAND_SET); put_string(out, key)?; put_string(out, value)?; }
Command::Delete { key }     => { out.push(COMMAND_DELETE); put_string(out, key)?; }
Command::Noop               => out.push(COMMAND_NOOP),
```
Bytes on the wire: `01 [key] [value]` / `02 [key]` / `03`. `read_command` reads that tag byte and
does the reverse; an unknown tag → `Err("unknown Raft command type")`.

---

## 4.5 `rpc.rs` — the messages on the wire

**File:** `src/raft/rpc.rs` (≈260 lines; real code is lines 1–161, the rest are tests)

**What it does:** Defines `RaftMessage` (one enum holding all six message kinds) and converts it to and
from bytes. Each message is one **frame** on TCP (see `net/framing.rs`: `[4-byte length][payload]`).

### Functions

| Function | Lines | Does |
|---|---|---|
| `RaftMessage` (enum) | 27–35 | The six messages: request/reply for Vote, AppendEntries, InstallSnapshot. |
| `encode_message` | 37–91 | `RaftMessage` → bytes. |
| `decode_message` | 93–153 | bytes → `RaftMessage` (or error). |
| `read_bool` | 155–161 | A byte that must be exactly 0 or 1. |

### Wire layout of each message (first byte = type tag)

```
 RequestVote (1):          [1][term][candidate_id][last_log_index][last_log_term]
 RequestVoteReply (2):     [2][term][vote_granted: 0/1]
 AppendEntries (3):        [3][term][leader_id][prev_log_index][prev_log_term][leader_commit]
                               [entry_count: u32] [entry][entry]...      (each entry = term + command)
 AppendEntriesReply (4):   [4][term][success: 0/1]
 InstallSnapshot (5):      [5][term][leader_id][last_included_index][last_included_term]
                               [offset][done: 0/1][data: u32 len + bytes]
 InstallSnapshotReply (6): [6][term]
        (every [term]/[...] numeric field is 8 bytes, big-endian)
```

### `encode_message`, line by line

```rust
let mut out = Vec::new();                       // 38: the output buffer
match message {                                 // 40: which kind of message?
    RaftMessage::RequestVote(args) => {
        out.push(REQUEST_VOTE);                 // 42: tag byte = 1
        put_u64(&mut out, args.term);           // 43-46: the four numbers, 8 bytes each
        ...
    }
    RaftMessage::RequestVoteReply(reply) => {
        out.push(REQUEST_VOTE_REPLY);
        put_u64(&mut out, reply.term);
        out.push(u8::from(reply.vote_granted)); // 51: bool → 0 or 1
    }
    RaftMessage::AppendEntries(args) => {
        ... five numbers ...
        let count = u32::try_from(args.entries.len())?;   // 61-62: entry count must fit in a u32
        put_u32(&mut out, count);                         // 63
        for entry in &args.entries { put_entry(&mut out, entry)?; }   // 65-67: each entry's bytes
    }
    ...
}
Ok(out)
```

### `decode_message`, line by line

```rust
let mut cursor = 0;                                           // 94
let message = match read_u8(bytes, &mut cursor)? {            // 96: read the tag, then branch
    REQUEST_VOTE => RaftMessage::RequestVote(RequestVoteArgs {
        term: read_u64(bytes, &mut cursor)?,                  // 98-101: fields are read in the
        candidate_id: read_u64(bytes, &mut cursor)?,          // SAME ORDER encode wrote them
        ...
    }),
    APPEND_ENTRIES => {                                       // 107
        ... read the five numbers and the count ...
        let mut entries = Vec::with_capacity(count.min(MAX_PREALLOCATED_ENTRIES)); // 115
        for _ in 0..count { entries.push(read_entry(bytes, &mut cursor)?); }       // 116-118
        ...
    }
    ...
    _ => return Err(invalid_data("unknown Raft message type")),   // 145
};
if cursor != bytes.len() { return Err(invalid_data("trailing bytes after Raft message")); } // 148
Ok(message)
```

**Two defensive details worth understanding:**

1. `Vec::with_capacity(count.min(1024))` (line 115): the message *claims* it has `count` entries.
   A malicious sender could claim 4 billion and make you allocate gigabytes. So we pre-allocate at
   most 1024 slots; the vector can still grow if the data is really there. If the bytes run out, the
   `read_entry(...)?` fails first.
2. `cursor != bytes.len()` (line 148): leftover bytes mean the message was malformed → reject.

---

## 4.6 `snapshot.rs` — the state machine on disk

**File:** `src/raft/snapshot.rs` (≈180 lines; real code lines 1–124)

**What it does:** Defines `RaftSnapshot`: *the entire key-value contents as of log index N*, plus `N`
and its term. The **same bytes** are saved to disk and streamed over the network in `InstallSnapshot`.

### Functions

| Function | Lines | Does |
|---|---|---|
| `RaftSnapshot::encode` | 27–48 | Snapshot → bytes. |
| `RaftSnapshot::decode` | 50–79 | bytes → snapshot (validating everything). |
| `RaftSnapshot::write_atomic` | 81–83 | Encode and save with `write_file_atomic`. |
| `RaftSnapshot::read` | 85–96 | Load and decode a snapshot file. |
| `RaftSnapshot::read_meta` | 98–106 | Read **only the 32-byte header** (index, term) — cheap check at startup. |
| `decode_header` | 109–124 | Validate magic + index/term; return `(index, term, count)`. |

### File layout

```
  0..8        8..16              16..24            24..32        32..
 ┌──────────┬──────────────────┬─────────────────┬─────────────┬──────────────────────────────┐
 │"RAFTSNP1"│ last_included_   │ last_included_  │ entry count │ [key][value][key][value] ... │
 │  magic   │ index  (u64)     │ term  (u64)     │  (u64)      │ (each = u32 len + utf-8)     │
 └──────────┴──────────────────┴─────────────────┴─────────────┴──────────────────────────────┘
   HEADER_SIZE = 8+8+8+8 = 32
```

### `encode`, line by line

```rust
out.extend_from_slice(MAGIC);                         // 30: "RAFTSNP1"
put_u64(&mut out, self.last_included_index);          // 31
put_u64(&mut out, self.last_included_term);           // 32
put_u64(&mut out, self.entries.len() as u64);         // 33: how many key/value pairs
for (key, value) in &self.entries {                   // 35: BTreeMap iterates in sorted key order
    put_string(&mut out, key)?;
    put_string(&mut out, value)?;
}
if out.len() as u64 > MAX_SNAPSHOT_BYTES { return Err(...) }   // 40: refuse > 1 GiB
```
`entries` is a `BTreeMap`, so encoding the same data always gives the **same bytes** (sorted keys).

### `decode`, line by line

```rust
let (last_included_index, last_included_term, count) = decode_header(bytes)?;   // 51: validates magic etc.
let mut cursor = HEADER_SIZE;                                                    // 52: entries start at byte 32
if count > bytes.len() as u64 { return Err(...) }       // 55: a count bigger than the file is corrupt
let mut entries = BTreeMap::new();
for _ in 0..count {                                     // 61
    let key = read_string(bytes, &mut cursor)?;
    let value = read_string(bytes, &mut cursor)?;
    if entries.insert(key, value).is_some() {           // 65: insert returns Some(old) if the key existed
        return Err(invalid_data("duplicate snapshot key"));
    }
}
if cursor != bytes.len() { return Err(...) }            // 70: no trailing junk
```

### `decode_header`

```rust
if bytes.len() < HEADER_SIZE || &bytes[..8] != MAGIC { return Err(...) }   // 110: too short or wrong magic
... read index, term, count ...
if index == 0 || term == 0 { return Err(...) }                              // 119: a snapshot of "nothing" is invalid
```
Index 0 / term 0 is reserved for "no snapshot", so a real snapshot must have both ≥ 1.

### `read_meta` — why it exists

At startup, `RaftNode::open` only needs to know *"does a usable snapshot exist, and what index does it
cover?"* — not the (possibly huge) data. `read_meta` reads just the first 32 bytes
(`File::open(path)?.read_exact(&mut header)?`).

---

## 4.7 `log.rs` — the durable log

**File:** `src/raft/log.rs` (454 lines)

**What it does:** `RaftLog` is the Raft log: an ordered list of `LogEntry` that is **stored in a file**
so it survives crashes, and **mirrored in memory** (a `Vec`) so reads are fast. It also knows how to
**forget a prefix** after a snapshot (compaction). Everything the protocol needs from the log — "what's
my last index/term?", "append", "throw away a wrong tail", "does entry P have term T?" — is here.

### The struct (lines 21–30)

```rust
pub struct RaftLog {
    path: PathBuf,              // the log file, e.g. raft_data/node1/raft.log
    file: File,                 // an open handle to it (read + write)
    base_path: PathBuf,         // the side file "raft.base"
    base_index: u64,            // entries 1..=base_index were compacted away (0 = none)
    base_term: u64,             // the term of the entry at base_index (0 = none)
    entries: Vec<LogEntry>,     // the entries still stored: base_index+1, base_index+2, ...
    record_positions: Vec<u64>, // record_positions[i] = byte offset in the file where entries[i] starts
}
```

### Memory picture (this is the key to all the index math)

```
  base_index = 3, base_term = 2        (entries 1,2,3 were folded into a snapshot)

   index:        (1)(2)(3) │  4     5     6
                 gone gone gone │ ┌────┬────┬────┐
                          ▲     │ │ e4 │ e5 │ e6 │   ← `entries` vector
                          │     │ └────┴────┴────┘
                    "base": we only             vec position:  0    1    2
                    remember its term (2)

   first_index() = base_index + 1            = 4
   last_index()  = base_index + entries.len() = 3 + 3 = 6
   entry_at(i)   = entries[ i - base_index - 1 ]        e.g. entry_at(5) = entries[1] = e5
```

If **no compaction ever happened**, `base_index = 0` and the formulas collapse to the obvious ones:
`last_index = entries.len()`, `entry_at(i) = entries[i-1]`.

### The file picture

```
 raft.log  (a sequence of records, back to back)
 ┌───────────┬──────────────────────────────────────┬───────────┬─────────────────────┬─ ...
 │ len (u32) │ payload = [index u64][term u64][cmd] │ len (u32) │ payload ...         │
 └───────────┴──────────────────────────────────────┴───────────┴─────────────────────┘
  ▲ position 0                                       ▲ position = 4 + len of record 0
  record_positions = [0, that number, ...]

 raft.base  (20 bytes, only exists after the first compaction)
 ┌────────┬───────────────┬───────────────┐
 │ "RBAS" │ base_index u64│ base_term u64 │
 └────────┴───────────────┴───────────────┘
```

Each record stores its own **index**. That's what lets `open` detect corruption ("indices must be
consecutive") and ignore stale records after an interrupted compaction.

### Functions

| Function | Lines | Does |
|---|---|---|
| `RaftLog::open(path)` | 33–117 | Load `raft.base`, read every record from `raft.log`, cut off a half-written last record. |
| `base_index`, `base_term` | 119–127 | Getters. |
| `first_index()` | 129–132 | `base_index + 1`. |
| `last_index()` | 134–136 | Index of the last entry. |
| `last_term()` | 138–142 | Term of the last entry (or `base_term` if no entries). Used in voting. |
| `term_at(i)` | 144–152 | Term at index `i` (the base boundary is answered from `base_term`). |
| `entry_at(i)` | 154–161 | The entry at `i`, or `None` if compacted / beyond the end. |
| `entries_from(start)` | 163–173 | Copy of entries from `start` on. |
| `append(entry)` | 175–214 | Add one entry to the end, **durably** (fsync). Returns its index. |
| `truncate_from(i)` | 216–239 | Delete entry `i` and everything after it (file + memory). |
| `compact_to(i, term)` | 241–313 | Fold entries ≤ `i` into a snapshot: write base file, rewrite log file, update memory. |
| `append_from_leader(...)` | 315–395 | The follower's rule for accepting a leader's batch. The most important function in the file. |
| `encode_base`, `load_base` | 398–428 | Encode/decode the 20-byte base file. |
| `encode_entry`, `decode_entry` | 430–450 | One record's payload (index + term + command). |

### `open` — line by line (33–117)

```rust
let path = path.as_ref().to_path_buf();                       // 34
let base_path = path.with_extension("base");                  // 35: raft.log → raft.base
let (base_index, base_term) = load_base(&base_path)?;         // 36: (0,0) if there's no base file
```

```rust
let mut file = OpenOptions::new()
    .create(true).read(true).write(true)                      // 38-41: make it if missing; read AND write
    .truncate(false)                                          // 42: NEVER wipe an existing log!
    .open(&path)?;
let file_len = file.metadata()?.len();                        // 45: how many bytes are in the file
let mut position = 0u64;                                      // 46: where the next record starts
let mut entries = Vec::new();                                 // 47
let mut record_positions = Vec::new();                        // 48
```

The loop reads one record per iteration, `while position < file_len` (line 50):

```rust
file.seek(SeekFrom::Start(position))?;                        // 51: jump to this record
let mut header = [0u8; 4];
if let Err(error) = file.read_exact(&mut header) {            // 55: try to read the 4-byte length
    if error.kind() == io::ErrorKind::UnexpectedEof { break; }// 56: fewer than 4 bytes left → torn tail → stop
    return Err(error);
}
let length = u32::from_be_bytes(header) as usize;             // 62: payload size
if length == 0 || length > MAX_RECORD_SIZE {                  // 64: absurd length → the file is corrupt
    return Err(invalid_data("invalid Raft record length"));
}
let record_end = position + 4 + length  (with checked_add);   // 68-71: where this record ends
if record_end > file_len { break; }                           // 73: record is cut off → torn tail → stop
let mut payload = vec![0; length];
file.read_exact(&mut payload)?;                               // 77-78: read the payload
let (index, entry) = decode_entry(&payload)?;                 // 80: bytes → (index, entry)
```

Then two checks:

```rust
if index <= base_index {                                      // 84
    position = record_end;                                    // 85
    continue;                                                 // 86
}
```
**Stale-record skip.** If a crash happened *after* `raft.base` was written but *before* the log file was
rewritten (see `compact_to`), the old file still has records the snapshot already covers. They are
ignored here.

```rust
let expected_index = base_index + entries.len() as u64 + 1;   // 89
if index != expected_index {                                  // 91
    return Err(invalid_data("Raft log indices are not consecutive"));
}
record_positions.push(position);                              // 95
entries.push(entry);                                          // 96
position = record_end;                                        // 97
```
Entries must be exactly `base+1, base+2, base+3, ...` with no gaps.

After the loop:

```rust
if position < file_len {                                      // 101: bytes remain that we couldn't parse as a whole record
    file.set_len(position)?;                                  // 102: CUT them off the file
    file.sync_all()?;                                         // 103
}
file.seek(SeekFrom::End(0))?;                                 // 106: be ready to append at the end
```

> **Why "torn tail" matters:** if the machine loses power in the middle of `append`, the last record may
> be half-written. Raft never acknowledged that entry (it fsyncs *before* acknowledging), so dropping it
> is safe. Without this cleanup, a half-record would make the next restart fail.

### The accessors (119–173) — with examples

Say `base_index = 3`, `base_term = 2`, entries `e4(t2) e5(t3) e6(t3)`.

| Call | Result | Why |
|---|---|---|
| `first_index()` | 4 | `3 + 1` |
| `last_index()` | 6 | `3 + 3` |
| `last_term()` | 3 | term of `e6` |
| `term_at(3)` | `Some(2)` | index equals `base_index` → answered from `base_term` (line 147) |
| `term_at(2)` | `None` | compacted away; no longer known |
| `entry_at(3)` | `None` | `index <= base_index` (line 155) |
| `entry_at(5)` | `Some(e5)` | `entries[5-3-1] = entries[1]` |
| `term_at(9)` | `None` | beyond the end |

```rust
pub fn entries_from(&self, start_index: u64) -> Vec<LogEntry> {
    let start_index = start_index.max(self.first_index());   // 166: never go below what we still have
    let Ok(start) = usize::try_from(start_index - self.first_index()) else { return Vec::new(); };
    self.entries.get(start..).unwrap_or(&[]).to_vec()         // 172: slice from `start` to the end, copied
}
```
`.get(start..)` returns `None` if `start` is past the end; `.unwrap_or(&[])` then gives an empty slice.
The comment on line 163 says it: if the caller needs entries older than `first_index` it must send a
snapshot instead — that decision lives in `RaftNode::needs_snapshot`.

### `append` (175–214) — add one entry, durably

```rust
let index = self.last_index().checked_add(1).ok_or_else(...)?;   // 176-179: new index = last + 1
let payload = encode_entry(index, &entry)?;                      // 181: index+term+command → bytes
if payload.len() > MAX_RECORD_SIZE { return Err(...) }           // 183: refuse > 1 MiB entries
let length = u32::try_from(payload.len())...?;                   // 190: the 4-byte length header
let position = self.file.seek(SeekFrom::End(0))?;                // 193: remember where this record starts
```

```rust
let result = (|| {                                               // 195
    self.file.write_all(&length.to_be_bytes())?;                 // 196: write [length]
    self.file.write_all(&payload)?;                              // 197: write [payload]
    self.file.sync_all()?;                                       // 198: fsync — now it's on disk
    Ok::<(), io::Error>(())
})();                                                            // 200: the trailing () calls the closure immediately
```
The closure groups three fallible steps so one failure handler can undo all of them:

```rust
if let Err(error) = result {                                     // 202
    self.file.set_len(position)?;                                // 204: cut the file back to before this record
    self.file.sync_all()?;
    self.file.seek(SeekFrom::End(0))?;
    return Err(error);                                           // 207
}
self.record_positions.push(position);                            // 210: only NOW update memory
self.entries.push(entry);                                        // 211
Ok(index)
```
**Order matters:** disk first, memory second. If the write fails, memory is untouched. If we crash
after the fsync, `open` will find the record.

### `truncate_from(index)` (216–239) — delete a wrong tail

```rust
if index <= self.base_index { return Err("cannot truncate a compacted Raft entry") }  // 217: can't delete what's already gone
let Ok(position) = usize::try_from(index - self.base_index - 1) else { return Err(...) }; // 221: vec position
if position >= self.entries.len() { return Ok(()); }              // 225: nothing to delete
let file_position = self.record_positions[position];              // 229: byte offset where that record begins
self.file.set_len(file_position)?;                                // 231: shrink the FILE to that offset
self.file.sync_all()?;                                            // 232
self.file.seek(SeekFrom::End(0))?;                                // 233
self.entries.truncate(position);                                  // 235: shrink the vector
self.record_positions.truncate(position);                         // 236
```
That's why `record_positions` exists: to know *which byte* to cut the file at.

### `compact_to(index, term)` (247–313) — forget a prefix

Goal: after a snapshot covering entries `1..=index` is **already safely saved**, delete those entries
from the log.

```rust
if index <= self.base_index { return Ok(()); }                    // 248: already compacted past this
if term == 0 { return Err(...) }                                  // 252
let keep_suffix = index < self.last_index() && self.term_at(index) == Some(term);   // 256
```
`keep_suffix` answers: *"does my log actually contain the snapshot's last entry, with the same term, and
have more entries after it?"*

- **Yes** → we only drop entries ≤ `index` and keep the rest.
- **No** → either the snapshot is *ahead* of my log (index > last_index), or my entry at `index` has a
  *different term* (my log conflicts with the snapshot, so it's wrong). In both cases **throw the whole
  log away**: it can't be trusted after that point.

```rust
let dropped = if keep_suffix { index - self.base_index } else { self.entries.len() };   // 258-263: how many to drop from the front
let kept: Vec<LogEntry> = self.entries[dropped..].to_vec();                              // 265: the survivors
```

Then **two durable steps, in this exact order:**

```rust
write_file_atomic(&self.base_path, &encode_base(index, term))?;   // 269: STEP 1 — record the new base
```
```rust
// 272-298: STEP 2 — write the survivors into raft.log.tmp, giving each its correct new index
let temp_path = temp_path_for(&self.path);
... for (offset, entry) in kept.iter().enumerate() {
        let record_index = index + 1 + offset as u64;             // 285: survivors are index+1, index+2, ...
        let payload = encode_entry(record_index, entry)?;
        temp.write_all(&length.to_be_bytes())?; temp.write_all(&payload)?;
        positions.push(written);                                   // 293: new byte offsets
        written += HEADER_SIZE + payload.len() as u64;
    }
    temp.sync_all()?;                                              // 297
fs::rename(&temp_path, &self.path)?;                               // 300: swap the new log in
sync_parent_directory(&self.path)?;                                // 301
```
Finally reopen the handle and update memory (303–310): `self.file`, `base_index`, `base_term`, `entries`,
`record_positions`.

**Crash safety — what if the power dies at each point?**

```
 time ───────────────────────────────────────────────────────────────────────────────►
  A: snapshot file saved (done by the CALLER before compact_to is ever called)
  B: write raft.base (atomic)
  C: write raft.log.tmp
  D: rename raft.log.tmp → raft.log

  crash before B → nothing changed. Old log + old (or no) base. Fine.
  crash B..D     → new base says "entries ≤ index are gone", but raft.log still holds the old
                   records. On open(), line 84 SKIPS every record with index <= base_index. Fine.
  crash after D  → complete.
```

### `append_from_leader` (315–395) — the follower's rule

This implements Raft's AppendEntries receiver steps 2–4. Inputs: the leader's `prev_log_index`,
`prev_log_term`, the `incoming` entries, and this node's own `commit_index`. Output: `Ok(true)` =
accepted, `Ok(false)` = consistency check failed (leader should back up), `Err` = serious violation.

**Step 0 — skip entries the snapshot already covers (322–342):**

```rust
let mut prev_log_index = prev_log_index; ... let mut incoming = incoming;     // 322-324: make mutable copies
if prev_log_index < self.base_index {                                          // 328: leader is talking about entries older than my snapshot
    let skip = (self.base_index - prev_log_index).min(incoming.len());         // 329-331
    incoming = &incoming[skip..];                                              // 333: drop the already-covered entries
    prev_log_index = prev_log_index.saturating_add(skip as u64);               // 334
    if prev_log_index < self.base_index { return Ok(true); }                   // 336: whole batch is inside my snapshot — nothing to do
    prev_log_term = self.base_term;                                            // 341: now we stand exactly on the boundary
}
```
Those entries are committed and part of my snapshot, so they're guaranteed identical. Skipping them is
safe.

**Step 1 — the consistency check (344–347):**

```rust
if self.term_at(prev_log_index) != Some(prev_log_term) { return Ok(false); }
```
"Do I have an entry at `prev_log_index` with the term you said?" If not → refuse; the leader will try
an earlier `prev`. (When `prev_log_index == 0` and `prev_log_term == 0`, `term_at(0)` is `Some(0)`
because `base_index == 0` — so "append from the very beginning" always passes.)

**Step 2 — walk the incoming entries (349–392):**

```rust
for (offset, incoming_entry) in incoming.iter().enumerate() {
    let index = prev_log_index + offset + 1;                  // 353-356: this entry's index
    match self.entry_at(index) {
```

There are three cases for what's at that index in *my* log:

```
 CASE A  Some(existing) with the SAME term            (359-366)
         → already have it. Nothing to do; continue with the next incoming entry.
           (Extra safety: if the commands differ, that "can't happen" → return Err.)

 CASE B  Some(existing) with a DIFFERENT term         (368-382)
         → CONFLICT: my entry is from a dead leader; the leader's version wins.
           If index <= commit_index → Err  (a COMMITTED entry must never be overwritten!)
           Otherwise: truncate_from(index), then append ALL remaining incoming entries, return true.

 CASE C  None  (I have nothing at this index)         (384-390)
         → just append all remaining incoming entries from here, return true.
```
If the loop finishes without returning (every entry was Case A), return `Ok(true)` (line 394) — the log
was already up to date.

### Worked examples

```
 Leader log: [1:t1][2:t1][3:t2][4:t2]

 ① normal append      follower [1:t1][2:t1]            leader sends prev=2(t1), entries [3:t2][4:t2]
     check: term_at(2)=t1 ✓ → idx3 None → append both → follower [1][2][3:t2][4:t2]            true

 ② duplicate/delayed  follower [1:t1][2:t1][3:t2][4:t2] leader re-sends prev=2, entries [3:t2]
     idx3 exists, same term → Case A → loop ends → nothing changed                              true
     (A delayed message must NEVER truncate the longer log — that is what Case A guarantees.)

 ③ conflict           follower [1:t1][2:t1][3:t1]      leader sends prev=2(t1), entries [3:t2][4:t2]
     idx3 exists with term t1 ≠ t2 → Case B → truncate from 3, append [3:t2][4:t2]              true

 ④ gap                follower [1:t1]                  leader sends prev=3(t2), entries [4:t2]
     term_at(3) = None ≠ Some(t2) → return false  → leader will retry with an earlier prev

 ⑤ committed overwrite attempt   (commit_index = 3)    leader sends prev=2, entries [3:t9]
     idx3 exists with a different term, 3 <= 3 → Err("attempted to overwrite a committed entry")
```

### Helper functions (398–454)

- `encode_base(index, term)` — `"RBAS" + index + term` (20 bytes).
- `load_base(path)` — reads the base file; **missing file → `(0, 0)`**; wrong size/magic or a zero
  index/term → error.
- `encode_entry(index, entry)` — `put_u64(index)` then `put_entry` (term + command).
- `decode_entry(bytes)` — reads index, then the entry; rejects trailing bytes, index 0, or term 0.

---

## 4.8 `node.rs` — the Raft brain

**File:** `src/raft/node.rs` (548 lines)

**What it does:** `RaftNode` *is* the Raft protocol. It owns the term, the vote, the log, the commit
index, and (when leader) the progress of each follower. Every Raft rule from Part 1 is a method here.
**It has no sockets, no timers and no async** — it just takes a message in and returns what to reply (or
changes its own state). That's why it can be tested directly and simulated.

### The struct (lines 19–39)

| Field | Persistent? | Meaning |
|---|---|---|
| `id` | – | This node's id. |
| `role` | no | Follower / Candidate / Leader. |
| `current_term` | **yes** (state file) | Latest term this node has seen. |
| `voted_for` | **yes** (state file) | Who it voted for in `current_term` (or `None`). |
| `log` | **yes** | The `RaftLog`. |
| `commit_index` | no | Highest index known to be committed. |
| `last_applied` | no | Highest index applied to the store. Always `≤ commit_index`. |
| `leader_id` | no | Who this node thinks is the current leader (for redirecting clients). |
| `members` (private) | – | All node ids in the cluster, including this one. |
| `votes_received` (private) | no | Set of nodes that voted for me this election. |
| `state_path`, `snapshot_path` (private) | – | Where the state file and snapshot live. |
| `next_index` (private) | no, leader only | For each peer: the next log index I'll try to send it. |
| `match_index` (private) | no, leader only | For each peer: highest index I *know* it has. |

`next_index` vs `match_index` — the pair that confuses everyone:

```
 leader log:  [1][2][3][4][5][6][7]
 peer 2:      next_index = 6   ← OPTIMISTIC guess: "send it entry 6 next"  (can be wrong, we back off)
              match_index = 4  ← PROVEN: "I know it holds everything up to 4" (only moves forward)
```
`match_index` is what the commit rule counts. `next_index` is only a hint for what to send.

### Functions

| Function | Lines | Raft rule |
|---|---|---|
| `open` | 42–98 | Load state + log, validate members, resume from snapshot. |
| `persist_state` | 100–106 | Save term + vote to disk. |
| `step_down_for_higher_term` | 108–127 | "See a higher term → adopt it, become follower." |
| `quorum_size` | 129–131 | Majority count. |
| `start_election` | 133–167 | Become candidate. |
| `handle_vote_request` | 169–205 | **Voter** side: grant or refuse. |
| `record_vote_response` | 207–233 | **Candidate** side: count votes, maybe win. |
| `append_client_command` | 235–244 | Leader appends a client command to its log. |
| `handle_append_entries` | 246–290 | **Follower** side of replication / heartbeat. |
| `snapshot_path` | 292–295 | Getter. |
| `compaction_point` | 297–307 | "Is it time to snapshot? At which index?" |
| `compact_log_to` | 309–317 | Drop log prefix (after snapshot is saved). |
| `needs_snapshot` | 319–326 | Leader: "does this peer need entries I've deleted?" |
| `observe_leader` | 328–345 | Follower: accept a leader's term (used by snapshot chunks). |
| `should_install_snapshot` | 347–350 | Does a snapshot at `index` move me forward? |
| `finish_snapshot_install` | 352–360 | After installing a snapshot: update log, commit, applied. |
| `handle_install_snapshot_reply` | 362–393 | Leader: peer finished a snapshot → update progress. |
| `peers` | 395–402 | All members except me. |
| `become_leader` | 404–426 | Take leadership: reset progress, append no-op. |
| `advance_commit_from_progress` | 428–431 | Run the commit rule using `match_index`. |
| `build_append_entries` | 433–458 | Leader: make the next message for a peer. |
| `handle_append_entries_reply` | 460–502 | Leader: process a follower's answer. |
| `entries_to_apply` | 504–509 | Committed-but-unapplied entries. |
| `mark_applied` | 511–514 | Record that entries were applied. |
| `advance_commit_index` | 516–547 | **The commit rule.** |

### `open` (42–98)

```rust
if members.is_empty()        { return Err(invalid_input("cluster must have members")); }   // 43
if !members.contains(&id)    { return Err(invalid_input("members must include this node")); } // 47
let mut unique = HashSet::new();
if !members.iter().all(|m| unique.insert(*m)) { return Err(...unique...); }                 // 51-54
```
Basic validation. `HashSet::insert` returns `false` if the value was already there, so `.all(...)` fails
on a duplicate id.

```rust
let log_path = log_path.as_ref().to_path_buf();            // 56
let state_path    = log_path.with_extension("state");      // 57: raft.log → raft.state
let snapshot_path = log_path.with_extension("snapshot");   // 58: raft.log → raft.snapshot
let persistent = PersistentState::load(&state_path)?;      // 59: term + vote from disk (0/None on first run)
let log = RaftLog::open(&log_path)?;                       // 60: load the log
```
So **one `log_path` determines all three files**, all in the same folder.

```rust
if log.base_index() > 0 {                                  // 63: the log was compacted before...
    let (index, _) = RaftSnapshot::read_meta(&snapshot_path).map_err(...)?;   // 64: ...so a snapshot file MUST exist
    if index < log.base_index() { return Err("snapshot is older than the compacted log") }  // 71
}
```
A compacted log without its snapshot would mean the deleted entries are lost forever — refuse to start
rather than silently run with missing data.

```rust
let snapshot_index = log.base_index();                      // 80
... commit_index: snapshot_index, last_applied: snapshot_index,   // 88-89
```
Everything inside a snapshot was committed and applied. So on restart, `commit_index` and
`last_applied` start at the snapshot's index (or 0 if none). Role starts as **Follower** (line 84) and
`voted_for`/`current_term` come from disk.

### `persist_state` (100–106) and `step_down_for_higher_term` (108–127)

```rust
fn persist_state(&self) -> io::Result<()> {
    PersistentState { current_term: self.current_term, voted_for: self.voted_for }.save(&self.state_path)
}
```
Builds the 2-field struct from the current values and saves it (fsync inside).

```rust
fn step_down_for_higher_term(&mut self, term: u64) -> io::Result<()> {
    if term > self.current_term {                           // 109: only if it's really newer
        let old_term = self.current_term; let old_vote = self.voted_for;   // 110-111: remember for undo
        self.current_term = term;                           // 113: adopt the newer term
        self.voted_for = None;                              // 114: a new term → I haven't voted yet
        if let Err(error) = self.persist_state() {          // 116: save it
            self.current_term = old_term; self.voted_for = old_vote;       // 117-118: undo if it failed
            return Err(error);
        }
    }
    self.role = Role::Follower;                             // 123
    self.votes_received.clear();                            // 124: forget votes from any election I was running
    self.leader_id = None;                                  // 125: I don't know who leads this new term yet
    Ok(())
}
```
Called from 6 places (`handle_vote_request`, `record_vote_response`, `handle_append_entries`, `observe_leader`, `handle_append_entries_reply`, `handle_install_snapshot_reply`) whenever a message with a higher term arrives. **Rule: a node that sees a higher
term always becomes a follower.** This is how a stale leader is dethroned.

`quorum_size` (129): `members.len() / 2 + 1` using integer division: 3→2, 4→3, 5→3.

### `start_election` (133–167)

```rust
let next_term = self.current_term.checked_add(1).ok_or_else(|| ... "Raft term overflow")?;   // 134-137
let old_term = self.current_term; let old_vote = self.voted_for;       // 139-140
self.current_term = next_term;                                          // 142: new term
self.voted_for = Some(self.id);                                         // 143: vote for myself
if let Err(error) = self.persist_state() { /* undo */ return Err(error); }   // 145-149: save BEFORE asking for votes
self.role = Role::Candidate;                                            // 151
self.leader_id = None;                                                  // 152
self.votes_received.clear();                                            // 153
self.votes_received.insert(self.id);                                    // 154: I have 1 vote (mine)
if self.votes_received.len() >= self.quorum_size() { self.become_leader()?; }   // 157-159: single-node cluster wins instantly
Ok(RequestVoteArgs { term: self.current_term, candidate_id: self.id,
                     last_log_index: self.log.last_index(), last_log_term: self.log.last_term() })   // 161-166
```
**Persist before sending.** If the node crashed after sending vote requests but before saving, it might
restart in the old term and vote again.
The returned `RequestVoteArgs` is the message the shell (`server.rs`) sends to every peer.

### `handle_vote_request` (169–205) — the voter's decision

```rust
if args.term < self.current_term {                                      // 170
    return Ok(RequestVoteReply { term: self.current_term, vote_granted: false });   // 171-174
}
```
**Rule 1:** candidate's term is older than mine → refuse, and tell it my (newer) term.

```rust
if args.term > self.current_term { self.step_down_for_higher_term(args.term)?; }   // 177-179
```
Candidate is in a **newer** term → I adopt it (clearing my old vote and becoming follower) *before*
deciding.

```rust
let my_last_term  = self.log.last_term();                               // 181
let my_last_index = self.log.last_index();                              // 182
let candidate_is_up_to_date = args.last_log_term > my_last_term         // 184: candidate's last entry has a newer term, OR
    || (args.last_log_term == my_last_term && args.last_log_index >= my_last_index);   // 185: same term and at least as long
let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);  // 187
let vote_granted = can_vote && candidate_is_up_to_date;                 // 189
```
**Rule 2 — "up to date":** compare **last term first**, then last index. A log whose last entry is from
a later term is "more up to date" even if it is shorter.
**Rule 3 — one vote per term:** I can vote if I haven't voted, or I already voted for *this same*
candidate (a retransmitted request shouldn't flip the answer).

```rust
if vote_granted && self.voted_for != Some(args.candidate_id) {          // 191: new vote (not a repeat)
    let old_vote = self.voted_for;
    self.voted_for = Some(args.candidate_id);                           // 193
    if let Err(error) = self.persist_state() { self.voted_for = old_vote; return Err(error); }  // 195-198
}
Ok(RequestVoteReply { term: self.current_term, vote_granted })          // 201-204
```
The vote is **saved before replying**. Never grant a vote you could forget.

Decision table:

| Candidate term vs mine | Already voted for someone else in this term? | Candidate log up to date? | Result |
|---|---|---|---|
| lower | – | – | **No** (and reply my term) |
| equal / higher (after adopting) | no (or voted for it) | yes | **Yes** (saved first) |
| equal / higher | yes, for another | – | No |
| equal / higher | – | no | No |

### `record_vote_response` (207–233) — the candidate counts votes

```rust
if reply.term > self.current_term {                                      // 212
    self.step_down_for_higher_term(reply.term)?; return Ok(false);       // 213-214: someone is ahead of me → give up
}
if self.role != Role::Candidate || reply.term != self.current_term {     // 217
    return Ok(false);                                                    // 218: a late/stale reply — ignore
}
if !reply.vote_granted || !self.members.contains(&voter_id) { return Ok(false); }   // 221-223
self.votes_received.insert(voter_id);                                    // 225: it's a HashSet, so duplicates don't double count
if self.votes_received.len() >= self.quorum_size() {                     // 227
    self.become_leader()?; return Ok(true);                              // 228-229
}
Ok(false)
```
Returns `true` **exactly once**: at the moment this node wins. The shell uses that `true` to start
sending heartbeats immediately.

### `become_leader` (404–426)

```rust
let next = self.log.last_index() + 1;                                    // 409: BEFORE appending the no-op
self.log.append(LogEntry { term: self.current_term, command: Command::Noop })?;   // 411-414
self.next_index  = self.peers().into_iter().map(|peer| (peer, next)).collect();   // 416
self.match_index = self.peers().into_iter().map(|peer| (peer, 0)).collect();      // 417
self.role = Role::Leader;                                                // 419
self.leader_id = Some(self.id);                                          // 420
self.advance_commit_from_progress();                                     // 423
```
- **Why a no-op?** Section 1.8: a leader may only commit entries of *its own term* by counting. The
  no-op is such an entry; once it's replicated to a majority, every older entry before it commits too.
- **`next` is computed before the no-op** so the very first message to each follower starts right
  after the leader's *old* log end and already includes the no-op.
- `next_index` starts optimistic (everyone is caught up); `match_index` starts pessimistic (we know
  nothing is confirmed).
- `advance_commit_from_progress` at the end matters only for a **single-node cluster**, where the leader
  alone is a majority and can commit its own no-op.

### `append_client_command` (235–244)

```rust
if self.role != Role::Leader { return Err(io::Error::other("not leader")); }      // 236
self.log.append(LogEntry { term: self.current_term, command })                    // 240: stamp with MY term, append durably
```
Returns the new entry's index. Note it **doesn't send anything** — replication is the shell's job.

### `handle_append_entries` (246–290) — follower side of replication

```rust
if args.term < self.current_term {                                       // 250
    return Ok(AppendEntriesReply { term: self.current_term, success: false });   // 251-254: stale leader → reject
}
if args.term > self.current_term { self.step_down_for_higher_term(args.term)?; }   // 257-259: newer term → adopt it
self.role = Role::Follower;                                              // 261: a valid leader exists → I'm a follower
self.votes_received.clear();                                             // 262
self.leader_id = Some(args.leader_id);                                   // 263: remember who it is
```
Any `AppendEntries` with `term >= mine` proves a legitimate leader exists for this term: a candidate
gives up, a follower remembers the leader.

```rust
let success = self.log.append_from_leader(
    args.prev_log_index, args.prev_log_term, &args.entries, self.commit_index)?;   // 265-270
```
The consistency check + append/replace logic (Section 4.7). `self.commit_index` is passed so it can
refuse to overwrite committed entries.

```rust
if success {
    let last_new_index = args.prev_log_index + args.entries.len();       // 276-279 (with overflow checks)
    self.commit_index = self.commit_index.max(args.leader_commit.min(last_new_index));   // 281-283
}
Ok(AppendEntriesReply { term: self.current_term, success })              // 286-289
```
**The commit update — and a bug we fixed.** The follower may commit up to
`min(leader_commit, last_new_index)` where `last_new_index = prev_log_index + entries.len()` is *the last
entry this very message verified*. An earlier version used `log.last_index()` instead, which could
commit a stale tail the leader never confirmed:

```
 follower: [1:t1][2:t1(stale!)]      leader heartbeat: prev=1, entries=[], leader_commit=2
 ✗ using last_index() = 2 → commit_index = min(2, 2) = 2  → committed an entry the leader never checked!
 ✓ using last_new_index = 1+0 = 1 → commit_index = min(2, 1) = 1                        correct
```
(`.max(self.commit_index)` ensures the commit index never goes down.)

### Leader-side bookkeeping: `build_append_entries` (433–458)

```rust
if self.role != Role::Leader { return None; }                            // 439
let next = *self.next_index.get(&peer)?;                                 // 443: the peer's next_index (None if unknown → return None)
let prev_log_index = next.checked_sub(1)?;                               // 444: entry just before `next`
let prev_log_term  = self.log.term_at(prev_log_index)?;                  // 445: its term (None if compacted → return None)
let mut entries = self.log.entries_from(next);                           // 447: everything from `next` on
entries.truncate(max_entries);                                           // 448: cap the batch (0 = heartbeat)
Some(AppendEntriesArgs { term: self.current_term, leader_id: self.id,
    prev_log_index, prev_log_term, entries, leader_commit: self.commit_index })   // 450-457
```
The trailing `?` on an `Option` means "if `None`, return `None` from this function".
`max_entries` = 64 in normal replication, 3 in the simulator, **0 for a read-barrier heartbeat**.

### `handle_append_entries_reply` (460–502) — leader processes the answer

```rust
if reply.term > self.current_term { return self.step_down_for_higher_term(reply.term); }   // 468: follower is ahead → I'm deposed
if self.role != Role::Leader || sent.term != self.current_term
   || reply.term != self.current_term || !self.next_index.contains_key(&peer) { return Ok(()); }   // 473-479: stale reply → ignore
```
Replies arrive late. If I've since stepped down or my term changed, this reply says nothing about the
present — ignore it.

```rust
if reply.success {
    let replicated = sent.prev_log_index + sent.entries.len() as u64;     // 482-485: highest index the follower now has
    let matched = self.match_index.entry(peer).or_insert(0);
    *matched = (*matched).max(replicated);                                // 488-489: only ever moves FORWARD (replies can be reordered)
    let matched = *matched;
    let next = self.next_index.entry(peer).or_insert(1);
    *next = (*next).max(matched + 1);                                     // 492-493: next send starts after what it has
    self.advance_commit_from_progress();                                  // 495: maybe a majority now has more
} else if let Some(next) = self.next_index.get_mut(&peer) {
    *next = (*next).min(sent.prev_log_index).max(1);                      // 498: back up by one and retry
}
```
Worked example:

```
 leader log has 7 entries. peer 2: next_index=8.
 ── sent prev=7, entries=[]  (heartbeat)     reply success ──► replicated = 7+0 = 7 → match=7, next=8

 leader appends entry 8, 9. build_append_entries(2): prev=7, entries=[8,9]
 ── reply success ──► replicated = 7+2 = 9 → match=9, next=10 → commit rule re-evaluated

 peer 3: next_index=8 but its log only has 5 entries.
 ── sent prev=7 → peer has no entry 7 → reply FAILURE ──► next = min(8, 7).max(1) = 7
 ── sent prev=6 → failure ──► next = 6
 ── sent prev=5, entries=[6,7] → success ──► match=7, next=8
```

### `advance_commit_index` (516–547) — THE commit rule

```rust
if self.role != Role::Leader { return; }                                 // 517: only leaders commit by counting
let last_index = self.log.last_index();                                  // 521
for index in (self.commit_index + 1..=last_index).rev() {                // 523: try the HIGHEST uncommitted index first, go down
    if self.log.term_at(index) != Some(self.current_term) { continue; }  // 524: ONLY entries from MY term may be counted
    let replicated_count = self.members.iter().filter(|member| {         // 528-540: how many nodes have this entry?
        if **member == self.id { last_index >= index }                   //   me: yes (it's in my log)
        else { match_index.get(member).is_some_and(|r| *r >= index) }    //   others: match_index says so
    }).count();
    if replicated_count >= self.quorum_size() {                          // 542
        self.commit_index = index; break;                                // 543-544: found the highest committable index
    }
}
```
Example — 5 nodes (quorum 3), I'm node 1 in term 4, `commit_index = 3`, log has 7 entries (entries
4–7 are term 4), `match_index = {2:7, 3:5, 4:3, 5:0}`:

```
 index 7: me ✓  node2(7≥7) ✓  node3(5≥7) ✗  node4 ✗  node5 ✗  → 2 < 3   no
 index 6: me ✓  node2 ✓       node3(5≥6) ✗                              → 2 < 3   no
 index 5: me ✓  node2 ✓       node3(5≥5) ✓                              → 3 ≥ 3   YES → commit_index = 5
```
Because we go from high to low and `break` at the first success, we commit the **highest** possible
index. Committing 5 also commits 4 (everything ≤ 5).

If entry 5 had been from **term 2** (an old leader's), line 524 would `continue` past it and it would only
become committed once a current-term entry above it (6 or 7) reaches a majority. That's the Figure-8
protection.

### Snapshot-related functions in `node.rs` (292–393)

```rust
pub fn compaction_point(&self, threshold: u64) -> Option<(u64, u64)> {
    if threshold == 0 || self.last_applied < self.log.base_index() + threshold { return None; }   // 300
    let term = self.log.term_at(self.last_applied)?;                                                // 304
    Some((self.last_applied, term))                                                                 // 306
}
```
"Snapshot once at least `threshold` applied entries have piled up since the last snapshot, and snapshot
*at `last_applied`*." (Never beyond what's applied: the snapshot must equal the store's contents.)

```rust
pub fn compact_log_to(&mut self, index: u64, term: u64) -> io::Result<()> {
    if index > self.last_applied { return Err(io::Error::other("cannot compact past the applied index")); }  // 312
    self.log.compact_to(index, term)                                                                         // 316
}
```
A safety net: the log must never drop entries the store hasn't applied yet.

```rust
pub fn needs_snapshot(&self, peer: NodeId) -> bool {
    self.role == Role::Leader && self.next_index.get(&peer).is_some_and(|next| *next < self.log.first_index())   // 320-325
}
```
"I'm leader, and the next entry this peer needs is older than my oldest stored entry → I can't send it
as entries; send the snapshot."

```rust
pub fn observe_leader(&mut self, term: u64, leader_id: NodeId) -> io::Result<bool> {
    if term < self.current_term { return Ok(false); }              // 332: stale sender
    if term > self.current_term { self.step_down_for_higher_term(term)?; }   // 336
    self.role = Role::Follower; self.votes_received.clear(); self.leader_id = Some(leader_id);   // 340-342
    Ok(true)
}
```
Exactly the "term handling" prologue of `handle_append_entries`, reused for snapshot chunks.

```rust
pub fn should_install_snapshot(&self, index: u64) -> bool { index > self.commit_index }       // 348-350
pub fn finish_snapshot_install(&mut self, index: u64, term: u64) -> io::Result<()> {          // 353
    self.log.compact_to(index, term)?;                                                         // 354: drop/replace the log prefix
    self.commit_index = self.commit_index.max(index);                                          // 356
    self.last_applied = index;                                                                 // 357: the store now equals the snapshot
    Ok(())
}
```
Only install a snapshot that is *ahead* of what I've committed. After installing, my store *is* the
snapshot, so `last_applied = index`.

`handle_install_snapshot_reply` (362–393) mirrors the success branch of `handle_append_entries_reply`:
after the follower accepted the whole snapshot, `match_index[peer] = max(…, last_included_index)` and
`next_index[peer] = match + 1`, so normal replication resumes right after the snapshot.

### `entries_to_apply` / `mark_applied` (504–514)

```rust
(self.last_applied + 1..=self.commit_index)                          // the range of committed-but-unapplied indexes
    .filter_map(|index| self.log.entry_at(index).map(|entry| (index, entry.clone())))   // look each up; skip missing
    .collect()                                                       // → Vec<(index, entry)>
```
```rust
self.last_applied = self.last_applied.max(index.min(self.commit_index));   // never exceeds commit_index, never decreases
```
The shell calls `entries_to_apply`, applies them to the store, then calls `mark_applied(last_index)`.

---

## 4.9 `server.rs` — timers, network and the store

**File:** `src/raft/server.rs` (909 lines)

**What it does:** Drives a `RaftNode` in the real world. It owns the things `node.rs` deliberately
doesn't have:

- **timers** (election timeout, heartbeat tick),
- **TCP**: listens for other nodes and for clients, and connects out to peers,
- **the store**: applies committed entries to `LsmStore`, takes snapshots, installs snapshots,
- **concurrency**: many tasks at once, protected by locks.

Rule of thumb: **`server.rs` decides *when* and *to whom*; `node.rs` decides *what is correct*.**

### Constants and config (lines 28–68)

| Name | Value | Meaning |
|---|---|---|
| `MAX_ENTRIES_PER_MESSAGE` | 64 | Max log entries in one `AppendEntries`. |
| `MAX_REPLICATION_ROUNDS` | 256 | Max send/retry rounds to one peer per call (stops infinite loops). |
| `TICK_INTERVAL` | 20 ms | How often the ticker wakes up. |
| `APPLY_POLL_INTERVAL` | 10 ms | How often a waiting client request re-checks progress. |
| `MAX_INCOMING_SNAPSHOT_BYTES` | 256 MiB | Refuse a snapshot bigger than this. |

`RaftConfig` (34–68): `peers` (id → "host:port" of **every other node**), `election_timeout_min/max`
(300–600 ms), `heartbeat_interval` (100 ms), `rpc_timeout` (200 ms), `commit_timeout` (3 s),
`snapshot_threshold` (1000 entries), `snapshot_chunk_size` (256 KiB), `linearizable_reads` (true).

> The timing relationship matters: **heartbeat (100 ms) ≪ election timeout (300–600 ms)**. A follower
> receives several heartbeats per timeout window, so it only starts an election when the leader is
> really gone.

### The struct (lines 77–86) and what each lock protects

```rust
pub struct RaftServer {
    id: NodeId,
    node: Mutex<RaftNode>,                          // the protocol state (term, log, roles...)
    store: Mutex<LsmStore>,                         // the actual key-value data
    apply_lock: Mutex<()>,                          // serialises "apply committed entries" (holds no data)
    incoming_snapshot: Mutex<Option<IncomingSnapshot>>, // snapshot chunks being received
    config: RaftConfig,
    election_deadline: StdMutex<Instant>,           // "start an election if no leader contact by this time"
}
```
`Mutex<()>` holds nothing — it's purely a **"only one at a time"** token. `RaftServer` is always used
through `Arc<RaftServer>` so many tasks share it.

### Functions

| Function | Lines | Does |
|---|---|---|
| `RaftServer::new` | 89–102 | Build the server, set the first election deadline. |
| `run` | 105–113 | Run the three main loops together. |
| `random_election_timeout` | 117–134 | A random duration in `[min, max)`. |
| `reset_election_timer` | 136–142 | "I just heard from a leader / granted a vote: push the deadline out." |
| `election_timer_expired` | 144–149 | Has the deadline passed? |
| `run_ticker` | 151–178 | The heartbeat/election loop. |
| `start_election` | 182–213 | Become candidate and spawn vote requests. |
| `request_vote` | 215–243 | Ask one peer for its vote; count the answer. |
| `broadcast_append` | 247–255 | Spawn a `replicate_to` for every peer. |
| `replicate_to` | 259–312 | Send entries/snapshot to one peer until it is caught up. |
| `apply_committed` | 315–341 | Apply committed entries to the store. |
| `compact_if_needed` | 346–377 | Snapshot + drop log prefix when enough has accumulated. |
| `send_snapshot`, `try_send_snapshot` | 383–452 | Leader: stream the snapshot to a lagging peer. |
| `handle_install_snapshot` | 455–509 | Follower: receive snapshot chunks. |
| `install_received_snapshot` | 513–552 | Follower: install the complete snapshot. |
| `rpc` | 556–582 | Send one message to a peer and wait for one reply (with timeout). |
| `run_peer_server` | 584–599 | Accept connections from other nodes. |
| `handle_peer` | 601–612 | Loop: read a message, answer it. |
| `handle_peer_message` | 614–646 | Route a message to the right `node` method. |
| `run_client_server` | 650–663 | Accept client connections. |
| `handle_client` | 665–683 | Loop: read a request, answer it. |
| `execute_command` | 685–715 | `GET` / `SET` / `DELETE`. |
| `read_barrier` | 724–798 | Make a read linearizable. |
| `heartbeat_acknowledged` | 801–835 | One peer's answer to a read-barrier heartbeat. |
| `replicate_write` | 839–879 | Append → replicate → wait until applied. |
| `read_frame` (free fn) | 883–902 | Read one complete frame from a socket. |
| `not_leader_response` (free fn) | 904–909 | The error text for non-leaders. |

### `new` and `run` (89–113)

```rust
let server = Arc::new(Self { id: node.id, node: Mutex::new(node), store: Mutex::new(store),
    apply_lock: Mutex::new(()), incoming_snapshot: Mutex::new(None), config,
    election_deadline: StdMutex::new(Instant::now()) });                  // 90-98
server.reset_election_timer();                                            // 100: replace "now" with now + random timeout
server
```
`run` (105) uses `tokio::try_join!` to run the peer listener, the client listener and the ticker **at
the same time**; if any of them fails, `run` returns that error. `self.clone()` clones the `Arc` (cheap
pointer copy), not the server.

### Timers (117–149)

```rust
let min = ...election_timeout_min.as_millis() as u64;   // 118: 300
let max = ...election_timeout_max.as_millis() as u64;   // 119: 600
let span = max.saturating_sub(min).max(1);               // 120: 300 (never 0, so `% span` is safe)
let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);   // 122-125
let mut x = now ^ self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;       // 128: mix clock with node id; |1 keeps it non-zero
x ^= x << 13; x ^= x >> 7; x ^= x << 17;                                 // 129-131: "xorshift" scrambling
Duration::from_millis(min + x % span)                                    // 133: somewhere in [300, 600)
```
There's no `rand` crate here, so this hand-rolls a pseudo-random number. The node id is mixed in so two
nodes starting at the same instant still get different timeouts (that's what prevents repeated split
votes).

```rust
fn reset_election_timer(&self) {
    let deadline = Instant::now() + self.random_election_timeout();      // 137
    if let Ok(mut guard) = self.election_deadline.lock() { *guard = deadline; }   // 139-141
}
fn election_timer_expired(&self) -> bool {
    self.election_deadline.lock().map(|d| Instant::now() >= *d).unwrap_or(false)   // 144-148
}
```
A `std` mutex is fine here: it's locked for a few nanoseconds and never across an `.await`.

**When is `reset_election_timer` called?** (a) `new`; (b) `start_election`; (c) a vote was **granted**
(`handle_peer_message`, 619); (d) a valid `AppendEntries` arrived from a leader (631); (e) a valid
snapshot chunk arrived (472); (f) the ticker noticed this node just lost leadership (168). Everything
that proves "a leader or candidate is alive and legitimate" resets the timer.

### `run_ticker` (151–178) — the heartbeat

```rust
let mut last_heartbeat = Instant::now();
let mut was_leader = false;
loop {
    sleep(TICK_INTERVAL).await;                                          // 156: wake every 20 ms
    let is_leader = self.node.lock().await.role == Role::Leader;         // 158: lock, read role, unlock immediately
    if is_leader {
        if last_heartbeat.elapsed() >= self.config.heartbeat_interval {  // 161: 100 ms passed?
            last_heartbeat = Instant::now();
            self.broadcast_append().await;                               // 163: send entries/heartbeats to everyone
        }
    } else {
        if was_leader { self.reset_election_timer(); }                   // 166-169: just lost leadership → don't instantly campaign
        if self.election_timer_expired() { self.start_election().await; }// 171-173: no leader heard in time → run for office
    }
    was_leader = is_leader;                                              // 176
}
```

### `start_election` (182–213)

```rust
self.reset_election_timer();                                  // 183: give this election a full timeout to finish
let (args, became_leader) = {
    let mut node = self.node.lock().await;                    // 186: take the lock...
    match node.start_election() {                             // 188: ...let node.rs do the protocol work (term+1, persist, vote for self)
        Ok(args) => (args, node.role == Role::Leader),        // 189: `became_leader` is true only for a 1-node cluster
        Err(error) => { eprintln!(...); return; }             // 190-193
    }
};                                                            // 195: lock released here
if became_leader { ...; self.broadcast_append().await; return; }       // 199-203
for peer in peers {                                           // 207
    let server = Arc::clone(self); let args = args.clone();
    tokio::spawn(async move { server.request_vote(peer, args).await });  // 211: ask EACH peer concurrently, don't wait
}
```
Spawning means all peers are asked **in parallel**; a dead peer won't delay the others.

### `request_vote` (215–243)

```rust
let term = args.term;
let reply = match self.rpc(peer, &RaftMessage::RequestVote(args)).await {      // 222: send, wait for a reply
    Ok(RaftMessage::RequestVoteReply(reply)) => reply,
    Ok(_) | Err(_) => return,                                                  // 224: wrong message type or network failure → give up quietly
};
let became_leader = { let mut node = self.node.lock().await;
                      match node.record_vote_response(peer, reply) { Ok(b) => b, Err(e) => { eprintln!(...); return; } } };   // 227-237
if became_leader { println!(...); self.broadcast_append().await; }             // 239-242: win → immediately tell everyone (stops their timers)
```
A lost reply or dead peer is simply *no vote from that peer*. The election succeeds if a majority of
the answers arrive.

### `broadcast_append` and `replicate_to` (247–312)

`broadcast_append` just spawns `replicate_to(peer)` for each peer. `replicate_to` is the leader's
**per-follower sending loop**:

```rust
for _ in 0..MAX_REPLICATION_ROUNDS {                                     // 260: bounded loop
    let args = { let node = self.node.lock().await;
                 if node.needs_snapshot(peer) { None }                   // 264: entries compacted → use snapshot path
                 else { node.build_append_entries(peer, MAX_ENTRIES_PER_MESSAGE) } };   // 267: else build the next batch
    let Some(args) = args else {                                         // 271: nothing to send as entries
        let needs_snapshot = self.node.lock().await.needs_snapshot(peer);               // 274
        if needs_snapshot && self.send_snapshot(peer).await { continue; }               // 276: snapshot sent OK → loop again for entries after it
        return;                                                          // 280: not leader any more, or snapshot failed
    };
    let reply = match self.rpc(peer, &RaftMessage::AppendEntries(args.clone())).await {  // 283: send
        Ok(RaftMessage::AppendEntriesReply(reply)) => reply, Ok(_) | Err(_) => return };  // 287-288
    let keep_going = reply.term == args.term && (!reply.success || !args.entries.is_empty());   // 291-292
    { let mut node = self.node.lock().await;
      if let Err(error) = node.handle_append_entries_reply(peer, &args, reply) { ...; return; } }   // 294-301
    if let Err(error) = self.apply_committed().await { ...; return; }    // 303: the commit index may have advanced → apply
    if !keep_going { return; }                                           // 308
}
```
**`keep_going` — when does it loop again?**

| Situation | `keep_going` | Why |
|---|---|---|
| Reply is from a different term (I'm deposed) | false | Stop. |
| `success = false` (log mismatch) | **true** | The leader already stepped `next_index` back; retry immediately. |
| `success = true` and we sent entries | **true** | More may remain, and the follower should learn the new commit index via the next (maybe empty) message. |
| `success = true` and it was an empty heartbeat | false | Caught up; wait for the next tick. |

So a lagging follower is brought up to date in a **burst** of messages, not one per heartbeat.

### `apply_committed` (315–341) — committed entries → the real store

```rust
let _apply_guard = self.apply_lock.lock().await;                         // 316: only ONE task applies at a time
let pending = self.node.lock().await.entries_to_apply();                 // 318: [(index, entry)] after last_applied, up to commit_index
let Some(last_index) = pending.last().map(|(index, _)| *index) else { return Ok(()); };   // 320: nothing to do
{
    let mut store = self.store.lock().await;                             // 325
    for (_, entry) in pending {                                          // 327: IN ORDER
        match entry.command {
            Command::Set { key, value } => store.set(key, value)?,       // 329
            Command::Delete { key } => { store.delete(key)?; }           // 330-332
            Command::Noop => {}                                          // 333: nothing to do
        }
    }
}
self.node.lock().await.mark_applied(last_index);                         // 338: record progress AFTER the store changed
self.compact_if_needed().await                                           // 340: maybe snapshot now
```
Why `apply_lock`? Two tasks could both see "entries 5..8 pending" and apply them twice or interleave.
(`set`/`delete` are idempotent, but order and `last_applied` bookkeeping would get confusing, and
`compact_if_needed` needs the store to match `last_applied` *exactly*.)
If applying fails half-way, `mark_applied` is skipped, so the same entries are retried next time —
safe because applying the same `SET`/`DELETE` twice gives the same result.

### `compact_if_needed` (346–377)

```rust
let point = { let node = self.node.lock().await; node.compaction_point(self.config.snapshot_threshold) };   // 347-350
let Some((index, term)) = point else { return Ok(()); };                // 352: not enough entries yet
let entries = self.store.lock().await.snapshot_entries()?;              // 356: dump the WHOLE store as a BTreeMap
let snapshot = RaftSnapshot { last_included_index: index, last_included_term: term, entries };   // 358-362
let mut node = self.node.lock().await;                                  // 364
snapshot.write_atomic(node.snapshot_path())?;                           // 367: STEP 1 — snapshot file durable
node.compact_log_to(index, term)?;                                      // 368: STEP 2 — only then delete log entries
```
Called while `apply_committed` still holds `apply_lock`, so **no new entries can be applied during the
dump** → the store matches `last_applied` exactly → the snapshot is a true "state as of `index`".
Order is critical: if we deleted log entries first and crashed, those entries would exist nowhere.

### Snapshots over the network

**Sender (leader)** — `try_send_snapshot` (393–452):

```rust
let (term, path) = { ... if node.role != Role::Leader { return Ok(false); } ... };   // 394-402
let snapshot = RaftSnapshot::read(&path)?;                              // 404: load OUR snapshot file
let bytes = snapshot.encode()?;                                         // 405
let chunk_size = self.config.snapshot_chunk_size.max(1);                // 406
let mut offset = 0usize; let mut reply_term = term;
while offset < bytes.len() {                                            // 411
    let end = (offset + chunk_size).min(bytes.len());                   // 412: this chunk is bytes[offset..end]
    let message = RaftMessage::InstallSnapshot(InstallSnapshotArgs {
        term, leader_id: self.id, last_included_index, last_included_term,
        offset: offset as u64, data: bytes[offset..end].to_vec(), done: end == bytes.len() });   // 414-422
    reply_term = match self.rpc(peer, &message).await? { RaftMessage::InstallSnapshotReply(r) => r.term, _ => return Err(...) };   // 424-427
    if reply_term != term { break; }                                    // 429: follower is in a different term → stop
    if offset == 0 { println!("sending snapshot ..."); }                // 435: log once, only after the peer answered
    offset = end;                                                       // 444
}
node.handle_install_snapshot_reply(peer, snapshot.last_included_index, term, reply_term)?;   // 449
Ok(reply_term == term)                                                  // 451
```

**Receiver (follower)** — `handle_install_snapshot` (455–509):

```rust
let term = { let mut node = self.node.lock().await;
             let current = node.observe_leader(args.term, args.leader_id)?;     // 461: term check, adopt leader
             if !current { return Ok(InstallSnapshotReply { term: node.current_term }); }   // 463-467: stale leader → reject
             node.current_term };                                               // 469
self.reset_election_timer();                                                    // 472: a live leader is talking to me

let finished = { let mut incoming = self.incoming_snapshot.lock().await;
    if args.offset == 0 {                                                       // 477: first chunk → start a fresh buffer
        *incoming = Some(IncomingSnapshot { last_included_index, last_included_term, data: Vec::new() });
    }
    let Some(current) = incoming.as_mut() else { return Err(invalid_data("snapshot chunk without a start")); };   // 485
    let in_order = current.last_included_index == args.last_included_index      // 489: same snapshot
        && current.last_included_term == args.last_included_term
        && current.data.len() as u64 == args.offset                              // 491: chunk starts exactly where the last ended
        && current.data.len() + args.data.len() <= MAX_INCOMING_SNAPSHOT_BYTES;  // 492: size cap
    if !in_order { *incoming = None; return Err(invalid_data("snapshot chunk out of order")); }   // 494-497: discard the whole attempt
    current.data.extend_from_slice(&args.data);                                  // 499: append the chunk
    if args.done { incoming.take() } else { None }                               // 501: on the last chunk, hand over the finished buffer
};
if let Some(snapshot) = finished { self.install_received_snapshot(snapshot).await?; }   // 504-506
Ok(InstallSnapshotReply { term })                                               // 508
```

`install_received_snapshot` (513–552):

```rust
let snapshot = RaftSnapshot::decode(&received.data)?;                   // 514: parse + validate the bytes
if header index/term don't match what the chunks claimed { return Err(...) }   // 516-520
let _apply_guard = self.apply_lock.lock().await;                        // 522: nobody applies entries meanwhile
let path = { let node = self.node.lock().await;
             if !node.should_install_snapshot(snapshot.last_included_index) { return Ok(()); }   // 527-529: already ahead? skip
             node.snapshot_path().to_path_buf() };
snapshot.write_atomic(&path)?;                                          // 534: STEP 1 — keep it on disk (so we can serve/restart from it)
self.store.lock().await.install_snapshot(&snapshot.entries, snapshot.last_included_index)?;   // 536-539: STEP 2 — replace the store's contents
self.node.lock().await.finish_snapshot_install(snapshot.last_included_index, snapshot.last_included_term)?;   // 541-544: STEP 3 — fix log/commit/applied
```
If we crash between steps, we restart with an old log; the leader notices we're still behind and sends
the snapshot again. Order again goes **durable first, bookkeeping last**.

### `rpc` (556–582) — one request, one reply

```rust
let address = self.config.peers.get(&peer).ok_or_else(|| ... "unknown peer")?;       // 557-561
let payload = encode_message(message)?;                                             // 563: message → bytes
let frame = encode_frame(&payload)?;                                                // 564: add the 4-byte length prefix
let exchange = async {                                                              // 566: the whole conversation as one future
    let mut stream = TcpStream::connect(address).await?;                            // 567: NEW connection for each RPC
    stream.write_all(&frame).await?;                                                // 568
    let mut decoder = FrameDecoder::new();
    read_frame(&mut stream, &mut decoder).await?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "peer closed"))  // 572-574
};
let reply = timeout(self.config.rpc_timeout, exchange).await                       // 577: give up after 200 ms
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Raft RPC timed out"))??;  // 579: two `?`: timeout error, then inner io error
decode_message(&reply)                                                              // 581: bytes → RaftMessage
```
The timeout is essential: a dead peer would otherwise hang a task forever. (Opening a connection per
RPC is simple but slow; a connection pool is a possible later improvement.)

### The peer server (584–646)

```rust
pub async fn run_peer_server(self: Arc<Self>, address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;                       // 585: listen on 127.0.0.1:910N
    loop {
        let (stream, _) = listener.accept().await?;                         // 588: wait for a connection
        let server = Arc::clone(&self);
        tokio::spawn(async move {                                           // 591: one task per connection
            if let Err(error) = server.handle_peer(stream).await
                && error.kind() != io::ErrorKind::UnexpectedEof {           // 592-593: EOF just means "peer hung up" — normal
                eprintln!(...);
            }
        });
    }
}
async fn handle_peer(&self, mut stream: TcpStream) -> io::Result<()> {
    let mut decoder = FrameDecoder::new();
    while let Some(payload) = read_frame(&mut stream, &mut decoder).await? {   // 604: next frame, or None when the peer closed
        let reply = self.handle_peer_message(decode_message(&payload)?).await?;   // 605
        let frame = encode_frame(&encode_message(&reply)?)?;                     // 606
        stream.write_all(&frame).await?;                                         // 608
    }
    Ok(())
}
```

`handle_peer_message` (614–646) — the **router**:

```rust
match message {
    RaftMessage::RequestVote(args) => {
        let reply = self.node.lock().await.handle_vote_request(args)?;     // 617: node.rs decides
        if reply.vote_granted { self.reset_election_timer(); }             // 619: I just voted → give the candidate time to win
        Ok(RaftMessage::RequestVoteReply(reply))
    }
    RaftMessage::AppendEntries(args) => {
        let term = args.term;
        let reply = self.node.lock().await.handle_append_entries(args)?;   // 628
        if reply.term == term { self.reset_election_timer(); }             // 631: valid leader → reset. If reply.term > term the sender was stale: don't reset
        self.apply_committed().await?;                                     // 635: the commit index may have moved → apply
        Ok(RaftMessage::AppendEntriesReply(reply))
    }
    RaftMessage::InstallSnapshot(args) => Ok(RaftMessage::InstallSnapshotReply(self.handle_install_snapshot(args).await?)),   // 640-642
    _ => Err(invalid_data("unexpected Raft message from peer")),           // 644: a *reply* arriving as a request
}
```
**Notice there is no protocol logic in the router.** Each arm is "call `node`, maybe reset the timer,
maybe apply".

### The client side (650–715)

`run_client_server` / `handle_client` mirror the peer server, but the payload is a **text command**:

```rust
let request = std::str::from_utf8(&payload).map_err(...)?;                // 669: bytes → &str
let response = match parse_request(request) {                              // 672: "SET a 1" → Command::Set("a","1")
    Some(command) => self.execute_command(command).await,
    None => Response::Error("invalid command".to_string()),
};
let frame = encode_frame(encode_response(response).as_bytes())?;           // 677: "+OK", "$5\r\nbaiju\r\n", "-ERR ..."
stream.write_all(&frame).await?;
```

```rust
async fn execute_command(self: &Arc<Self>, command: ClientCommand) -> Response {
    match command {
        ClientCommand::Get(key) => {
            if let Err(response) = self.read_barrier().await { return response; }   // 688: must be leader AND still leader
            match self.store.lock().await.get(&key) { Some(v) => Response::Value(v), None => Response::NotFound }   // 692
        }
        ClientCommand::Set(key, value) => self.replicate_write(Command::Set { key, value }).await,   // 698: go through Raft
        ClientCommand::Delete(key) => {
            if let Err(response) = self.read_barrier().await { return response; }   // 704: the "exists?" check is a read
            if self.store.lock().await.get(&key).is_none() { return Response::NotFound; }   // 708
            self.replicate_write(Command::Delete { key }).await                    // 712
        }
    }
}
```
Writes go **through the log**; reads go **to the store**, but only after `read_barrier` proves we're
still the leader.

### `replicate_write` (839–879) — a client write

```rust
let (index, term) = {
    let mut node = self.node.lock().await;
    if node.role != Role::Leader { return not_leader_response(&node); }      // 843-845: followers redirect
    match node.append_client_command(command) { Ok(index) => (index, node.current_term), Err(e) => return Response::Error(e.to_string()) }   // 847-850: durable append in MY term
};
self.broadcast_append().await;                                               // 853: start replicating now (don't wait for the next tick)
let deadline = Instant::now() + self.config.commit_timeout;                  // 855: 3 s
loop {
    { let node = self.node.lock().await;
      if node.role != Role::Leader || node.current_term != term {            // 861
          return Response::Error("leadership changed before the write committed; outcome unknown".to_string()); }
      if node.last_applied >= index { return Response::Ok; } }               // 868: applied here ⇒ committed on a majority ⇒ SAFE to say OK
    if Instant::now() >= deadline { return Response::Error("timed out waiting for a majority".to_string()); }   // 873
    sleep(APPLY_POLL_INTERVAL).await;                                        // 877: check again in 10 ms
}
```
**`+OK` is sent only when `last_applied >= index`**, and `last_applied ≤ commit_index`, and an entry is
committed only when a majority has it. So `+OK` means "durably stored on a majority". The two error
cases say honestly *"outcome unknown"* — the write may or may not have taken effect.

### `read_barrier` (724–798) — linearizable reads (ReadIndex)

```rust
let (term, read_index, quorum, peers) = {
    let node = self.node.lock().await;
    if node.role != Role::Leader { return Err(not_leader_response(&node)); }       // 728
    if !self.config.linearizable_reads { return Ok(()); }                          // 732: feature switch
    if node.log.term_at(node.commit_index) != Some(node.current_term) {            // 736
        return Err(Response::Error("leader has not committed an entry in its term yet; retry".to_string())); }
    (node.current_term, node.commit_index, node.quorum_size(), node.peers())       // 742-747
};
```
**Check ① (736):** the entry at `commit_index` must be from *my* term — i.e. my no-op has been
committed. Right after an election my `commit_index` may still be behind the real one.

```rust
let mut acknowledgements = 1;                                              // 750: myself
if acknowledgements < quorum {
    let tasks: Vec<_> = peers.into_iter().map(|peer| { let server = Arc::clone(self);
        tokio::spawn(async move { server.heartbeat_acknowledged(peer, term).await }) }).collect();   // 753-760: heartbeat EVERY peer in parallel
    for task in tasks { if let Ok(true) = task.await { acknowledgements += 1; } }                     // 762-766: count answers
}
if acknowledgements < quorum { return Err(Response::Error("could not confirm leadership with a majority".to_string())); }   // 769-773
```
**Check ② (750–773):** a majority must acknowledge me *right now*. If a newer leader existed, a majority
couldn't still be following me.

```rust
let deadline = Instant::now() + self.config.commit_timeout;
loop {
    { let node = self.node.lock().await;
      if node.role != Role::Leader || node.current_term != term { return Err(not_leader_response(&node)); }   // 781-783
      if node.last_applied >= read_index { return Ok(()); } }                                                // 785-787
    ... timeout / sleep 10 ms ...
}
```
**Check ③ (775–797):** the store must have applied everything that was committed when the read started
(`read_index`), so the value we read isn't older than a write the client already saw succeed.

`heartbeat_acknowledged` (801–835):

```rust
if node.needs_snapshot(peer) { return false; }                             // 805: that peer can't be heartbeated with entries
node.build_append_entries(peer, 0)                                         // 809: max_entries = 0 → an EMPTY AppendEntries (heartbeat)
...rpc...
let acknowledged = reply.term == term;                                      // 826
```
Even `success = false` counts as acknowledgement as long as the peer's term equals mine: it recognised my
leadership; it only disagreed about log contents. The reply is then also fed to
`handle_append_entries_reply` (830) so progress/step-down logic still runs.

### Free functions (883–909)

```rust
async fn read_frame(stream: &mut TcpStream, decoder: &mut FrameDecoder) -> io::Result<Option<Vec<u8>>> {
    let mut buffer = [0u8; 4096];
    loop {
        if let Some(frame) = decoder.next_frame()? { return Ok(Some(frame)); }   // 890: a full frame is already buffered
        let count = stream.read(&mut buffer).await?;                              // 894: otherwise read more bytes
        if count == 0 { return Ok(None); }                                        // 896: 0 bytes = the peer closed the connection
        decoder.push_bytes(&buffer[..count]);                                     // 900: feed them to the decoder
    }
}
```
TCP is a byte stream, not messages. One `read` may return half a frame or two frames; `FrameDecoder`
buffers bytes and hands back whole frames. `not_leader_response` (904) turns `leader_id` into
`"not leader; leader is node N"` or `"not leader; no leader elected yet"`.

---

## 4.10 `mod.rs` and `main_raft.rs`

**`src/raft/mod.rs`** (13 lines) just declares the submodules: `codec`, `fsutil`, `log`, `node`,
`persistence`, `rpc`, `server`, `snapshot`, `types` (public) and `sim`, `tests` (compiled only for
tests, via `#[cfg(test)]`).

**`src/bin/main_raft.rs`** starts one node:

```
cargo run --bin main_raft -- 1        # also 2 and 3, in other terminals
```
- Cluster is hard-wired to ids `[1, 2, 3]`.
- Node *N* listens for **peers** on `127.0.0.1:910N` and for **clients** on `127.0.0.1:710N`.
- Data lives in `raft_data/node<N>/` (`sstables/`, `wal.log`, `raft.log`, `raft.state`, plus
  `raft.base` / `raft.snapshot` after compaction).
- `RAFT_SNAPSHOT_THRESHOLD=10` (env var) makes snapshots happen after 10 entries so you can watch them.
- It builds `RaftNode::open(...)`, `LsmStore::open(...)`, `RaftConfig::new(peers)`, then
  `RaftServer::new(...).run(peer_addr, client_addr)`.

---

## 4.11 `sim.rs` and `tests.rs` — the proof

### `tests.rs` (874 lines) — unit tests of the pure core

Each test sets up one situation and checks one rule. Use this table to connect a test to a rule:

| Test | Rule it protects |
|---|---|
| `candidate_wins_with_majority` | Votes from a majority → leader. |
| `higher_term_makes_candidate_step_down` | A higher term always demotes. |
| `node_does_not_vote_twice_in_same_term` | One vote per term. |
| `stale_candidate_log_is_rejected` | The "up to date" vote rule. |
| `follower_rejects_wrong_previous_log_term` | The `prev_log_term` consistency check. |
| `committed_entry_cannot_be_overwritten` | Never overwrite committed entries. |
| `leader_commits_current_term_entry_with_majority` | Commit rule (basic). |
| `term_and_vote_survive_restart` | Persistence of term + vote. |
| `heartbeat_does_not_commit_unverified_stale_tail` | The `last_new_index` bug fix. |
| `follower_replaces_divergent_tail_with_leader_entries` | Log repair (Case B). |
| `duplicate_append_entries_leaves_log_unchanged` | A delayed duplicate doesn't truncate (Case A). |
| `leader_steps_down_on_higher_term_append_entries` | Leader deposed by newer leader. |
| `leader_does_not_commit_old_term_entry_by_counting_replicas` | Figure-8 protection. |
| `single_node_cluster_elects_itself` | `start_election` with quorum 1. |
| `invalid_membership_is_rejected` | `open` validation. |
| `log_survives_restart_and_discards_torn_tail` | Torn-tail recovery in `RaftLog::open`. |
| `leader_replicates_commits_and_followers_learn_the_commit` | The full replicate → commit → learn-commit loop. |
| `leader_backs_up_and_repairs_a_divergent_follower` | `next_index` backoff. |
| `committed_entry_survives_leader_change` | Leader completeness across an election. |
| `stale_leader_steps_down_when_a_follower_reports_a_higher_term` | Reply-driven step-down. |
| `candidate_forgets_the_previous_leader` | `leader_id` cleared on election. |
| `compaction_keeps_the_suffix_and_survives_restart` | `compact_to` keep-suffix path. |
| `compacting_past_the_end_or_over_a_conflict_discards_the_whole_log` | `compact_to` discard path. |
| `crash_between_writing_the_base_and_rewriting_the_log_is_recovered` | The stale-record skip (line 84 of `log.rs`). |
| `leader_batches_overlapping_the_snapshot_are_trimmed` | Step 0 of `append_from_leader`. |
| `compacted_node_resumes_from_its_snapshot_and_needs_the_snapshot_file` | `RaftNode::open` snapshot checks. |
| `lagging_follower_is_caught_up_with_a_snapshot` | The whole InstallSnapshot flow at node level. |

**Helpers worth knowing:** `cluster(name)` makes 3 nodes; `elect(nodes, candidate, voters)` runs an
election by direct calls; `deliver(nodes, from, to)` performs one full AppendEntries round trip
(build → handle → reply). The network is just **function calls**.

### `sim.rs` (747 lines) — the fault-injection simulator

`SimCluster` runs real `RaftNode`s with a **fake network and fake clock** driven by a seeded random
number generator. Same seed → same run, so a failure is reproducible.

What it can do to the cluster:

```
 election_timeout(i)        node i's timer fires → runs an election
 concurrent_elections(a,b)  two nodes time out together (same-term rivalry)
 replicate(leader)          one round of AppendEntries to everyone reachable
 client_write(i)            a client write arrives at node i
 crash(i) / restart(i)      lose RAM, keep disk (restart re-opens the files)
 partition(groups)          split the network;  heal()
 set_message_loss(‰)        randomly drop requests/replies
 random_step()              pick one of the above at random
 settle()                   heal everything, restart everyone, require a leader + a committed write
```

After **every** operation `check_invariants()` (sim.rs:501) verifies:

| Check | Meaning |
|---|---|
| terms/commit never decrease | Monotonicity. |
| `commit_index <= last_index` | Sanity. |
| **at most one leader per term** | Election safety (`leader_of_term` map). |
| **no two different entries committed at one index** | State machine safety (`committed` map, the "oracle"). |
| **a leader holds every entry committed in earlier terms** | Leader completeness. |
| **equal term at an index ⇒ equal prefix** | Log matching. |

If a check fails, `fail(...)` prints the seed and the last 40 operations — a ready-made bug report.

The named scenario tests in `sim.rs`: `minority_partitioned_leader_cannot_commit_and_is_overwritten`,
`entry_on_a_majority_survives_the_leader_crashing_before_the_ack`,
`leader_with_lost_messages_still_converges_once_the_network_recovers`,
`random_faults_never_break_safety` (12 random runs), and the long `random_faults_soak` (run with
`cargo test --lib raft::sim -- --ignored`).

---

# Part 5 — Follow real requests through the code

Now that you know each piece, watch them work together. For each flow: the picture first, then the
exact function calls in order.

## 5.1 Startup and the first election (3 nodes)

All three nodes start as **Followers** in term 0 with empty logs. Nobody sends heartbeats, so the
first election timer to expire (each is random in 300–600 ms) wins the race. Suppose **N3** is first:

```
   N1 (follower)                 N2 (follower)                 N3 (follower, timer fires first)
        │                             │                             │
        │                             │            run_ticker: election_timer_expired() → true
        │                             │            start_election()
        │                             │              └ node.start_election(): term 0→1, voted_for=3,
        │                             │                 SAVE to raft.state, role=Candidate
        │◄───── RequestVote(term 1, cand 3, last idx 0, last term 0) ──┤
        │                             │◄── RequestVote(...) ───────────┤
 handle_vote_request:          handle_vote_request:                    │
  term 1 > 0 → step down,       (same)                                  │
  adopt term 1,                                                         │
  log up to date ✓,                                                     │
  voted_for = 3 (SAVED)                                                 │
        ├────── RequestVoteReply(term 1, granted ✓) ─────────────────►  │
        │                             ├── RequestVoteReply(granted ✓) ► │
        │                             │                  request_vote → record_vote_response(1, ✓)
        │                             │                    votes = {3, 1} = 2 ≥ quorum(2)
        │                             │                    become_leader():
        │                             │                      append Noop [1:t1], next_index={1:1,2:1},
        │                             │                      match_index={1:0,2:0}, role=Leader
        │                             │                  broadcast_append() ──► heartbeats start
```

Function chain on **N3**: `run_ticker` → `start_election` (server) → `RaftNode::start_election` →
(spawn) `request_vote` → `rpc` → *(over TCP)* → peer's `run_peer_server` → `handle_peer` →
`handle_peer_message` → `RaftNode::handle_vote_request` → *(reply over TCP)* → back in `request_vote` →
`RaftNode::record_vote_response` → `become_leader` → `broadcast_append`.

Also note: when N1/N2 **grant** the vote, `handle_peer_message` calls `reset_election_timer()`, so they
won't start rival elections while N3 is collecting votes.

## 5.2 A client write: `SET name baiju` at the leader

*(Assume N3's election no-op, entry 1, is already committed, so `leader_commit = 1` below.)*

```
 client            N3 (leader)                                   N1 (follower)           N2 (follower)
   │  SET name baiju │                                                │                      │
   ├────────────────►│ handle_client → parse_request → execute_command
   │                 │   → replicate_write(Set)
   │                 │     node.append_client_command:
   │                 │       log.append([2:t1 SET name baiju])  ← fsync'd, index = 2
   │                 │     broadcast_append() → spawn replicate_to(1), replicate_to(2)
   │                 │                                                │                      │
   │                 │──AppendEntries(prev=1 (t1), entries=[2:SET], leader_commit=1)───────►│ (also to N1)
   │                 │                                    handle_append_entries:
   │                 │                                      term ok → role=Follower, leader_id=3
   │                 │                                      append_from_leader: check term_at(1)=t1 ✓,
   │                 │                                        idx 2 None → append [2:SET] (fsync)
   │                 │                                      commit = max(c, min(leader_commit=1, last_new=2)) = 1
   │                 │◄──────────────── AppendEntriesReply(term 1, success) ────────────────┤
   │                 │ handle_append_entries_reply(N2): replicated = 1+1 = 2
   │                 │   match_index[N2]=2, next_index[N2]=3
   │                 │   advance_commit_index: index 2, term t1 == current ✓
   │                 │     count = me ✓ + N2 ✓ = 2 ≥ quorum(2) → commit_index = 2     ← COMMITTED
   │                 │ apply_committed: store.set("name","baiju"), mark_applied(2)
   │                 │ replicate_write's polling loop sees last_applied(2) >= index(2)
   │◄── +OK ─────────┤
   │                 │
   │                 │  (replicate_to continues because it sent entries → sends another AppendEntries
   │                 │   with entries=[], leader_commit=2 so followers learn the new commit index)
   │                 │──AppendEntries(prev=2, entries=[], leader_commit=2)──────────────────►│
   │                 │                                    commit = min(2, last_new=2+0) = 2
   │                 │                                    apply_committed → store.set(...)     ← followers apply
```

Key moments to notice:

1. The entry is **fsync'd on the leader before anything is sent** (`RaftLog::append`).
2. `+OK` is returned only after a **majority** has it *and* the leader **applied** it.
3. Followers apply the entry **one message later**, when they learn `leader_commit = 2`.

## 5.3 What a follower does with a heartbeat (no entries)

```
 AppendEntries(term 1, prev=2, prev_term=1, entries=[], leader_commit=2)
   handle_peer_message → RaftNode::handle_append_entries
      term == mine                                 → no step down
      role = Follower, leader_id = Some(3)
      append_from_leader(prev=2,t1, [], commit)    → check term_at(2)==t1 ✓ → loop has nothing → true
      commit_index = max(commit, min(2, 2+0))
   reply.term == args.term                         → reset_election_timer()   ← the follower stays calm
   apply_committed()                               → apply anything newly committed
```
A heartbeat does three jobs at once: **(1)** prove the leader is alive (resets timers), **(2)** carry the
latest `leader_commit`, **(3)** verify the follower's log (the `prev` check) so the leader notices
mismatches early.

## 5.4 The leader dies — a new election

```
 term 1:  N3 leader  ✕ crashes
 N1: election timer fires (≈300-600 ms after the last heartbeat)
   start_election: term 1→2, vote self, persist
   RequestVote(term 2, cand 1, last idx 2, last term 1) ──► N2
   N2.handle_vote_request: term 2 > 1 → adopt; candidate log up to date (same last term/idx) ✓;
                           not voted in term 2 → GRANT (persist)
   N1 votes = {1,2} = 2 ≥ quorum → become_leader: next = last_index+1 = 3, append Noop [3:t2]
   N1 broadcast_append → N2 gets [3:t2 Noop]; match → commit_index 3 (Noop is current-term ✓)
   N3 later restarts: its log is [1][2], term 1 (loaded from disk).
     N1's heartbeat has term 2 > 1 → N3.step_down_for_higher_term(2) → follower; then it receives [3:t2].
```
**Why the data is safe:** entry 2 (`SET name baiju`) was committed (on N3 and N2) before the crash. N1's
only way to win was N2's vote, and N2 had entry 2. The vote rule "candidate's log must be at least as up
to date" guarantees any winner has all committed entries.

## 5.5 Repairing a follower whose log is wrong

Situation: N2 was an old leader that accepted entries nobody else got, then was cut off. Now N1 is the
leader (term 3).

```
 N1 (leader) log:  [1:t1][2:t1][3:t2][4:t3 Noop]
 N2 (old)    log:  [1:t1][2:t1][3:t1][4:t1][5:t1]       ← entries 3,4,5 are from a dead leader

 become_leader: next_index[N2] = 4 (last_index was 3 before the noop)

 Round 1: build_append_entries → prev=3 (t2), entries=[4:t3]
          N2: term_at(3) = t1 ≠ t2  → success=false
          leader: next_index[N2] = min(4, prev=3).max(1) = 3
 Round 2: prev=2 (t1), entries=[3:t2, 4:t3]
          N2: term_at(2) = t1 ✓. idx3 exists with term t1 ≠ t2 → CASE B: 3 > commit_index
              → truncate_from(3), append [3:t2][4:t3]       → success=true
          leader: match_index[N2]=4, next_index[N2]=5
 N2's log:  [1:t1][2:t1][3:t2][4:t3]   ✓ identical to the leader's. Entry [5:t1] is gone for good.
```
In `server.rs` this whole sequence happens inside **one** `replicate_to` call thanks to `keep_going`
(failure → loop immediately). Tests: `leader_backs_up_and_repairs_a_divergent_follower`
(`tests.rs`), `follower_replaces_divergent_tail_with_leader_entries`.

## 5.6 Compaction and a snapshot for a lagging follower

Config: `snapshot_threshold = 10`. N3 is down while N1 (leader) and N2 take 40 writes.

```
 Every time apply_committed runs on N1/N2:
   compaction_point(10): last_applied(≥10) ≥ base_index(0)+10  → Some((last_applied, its term))
   compact_if_needed:   snapshot_entries() → RaftSnapshot{index 10, ...}.write_atomic("raft.snapshot")
                        compact_log_to(10) → raft.base=(10,t), raft.log rewritten without entries 1..10
   ... again at 20, 30, 40.   Leader log now starts at index 41.

 N3 comes back (empty log, next_index[N3] = 1 from the election).
   replicate_to(N3):  needs_snapshot(N3): next_index(1) < first_index(41) → true
   send_snapshot → try_send_snapshot: read raft.snapshot (index 40), encode, send chunks:
       InstallSnapshot{offset 0, data[0..256K], done: false?}  ...  last chunk done: true
   N3.handle_install_snapshot:  observe_leader ✓ → buffer chunks (in_order checks) → on `done`:
       install_received_snapshot:
         write raft.snapshot  →  store.install_snapshot(entries)  →  finish_snapshot_install(40, t)
            (log.compact_to(40,t): N3 has no matching entry → discards its whole log; base=(40,t))
            commit_index = 40, last_applied = 40
   N1 gets InstallSnapshotReply{term}: handle_install_snapshot_reply → match[N3]=40, next[N3]=41
   replicate_to loops (`continue`) → build_append_entries(N3): prev=40, entries=[41...] → normal replication
```

## 5.7 A `GET` with the read barrier

```
 client: GET name → N3 (leader)
   read_barrier:
     ① role == Leader ✓;  term_at(commit_index) == current_term ✓ (my Noop is committed)
     ② spawn heartbeat_acknowledged(N1), (N2) in parallel:
            build_append_entries(peer, 0)  → empty AppendEntries
            reply.term == my term  → ack          (N1 acks, N2 is down → no ack)
        acknowledgements = 1 (me) + 1 (N1) = 2 ≥ quorum(2) ✓
     ③ wait until last_applied >= read_index
   store.get("name") → "baiju"       →  $5\r\nbaiju\r\n
```
If this leader had been deposed while paused, the heartbeat replies would carry a **higher term** →
`handle_append_entries_reply` steps it down → no majority ack → the client gets
`-ERR not leader…`/`could not confirm leadership` instead of stale data.

## 5.8 Crash and restart of one node

```
 RaftNode::open(id, members, "raft.log")
   PersistentState::load("raft.state")  → current_term, voted_for           (so it won't double-vote)
   RaftLog::open("raft.log")            → loads base + all records; cuts a torn last record
   if base_index > 0 → requires raft.snapshot (read_meta) else refuse to start
   role = Follower;  commit_index = last_applied = base_index  (volatile state is rebuilt)
 LsmStore::open(...)                    → the key-value data recovers by ITS OWN WAL
 RaftServer::new → run
   first election timer starts; as soon as a leader's AppendEntries arrives, the follower learns
   leader_commit and `apply_committed` re-applies entries (idempotent) up to the real commit index.
```

---

# Part 6 — Locks and concurrency map

Many tasks run at once, so shared data is behind locks.

| Lock | Protects | Type |
|---|---|---|
| `node: Mutex<RaftNode>` | term, vote, log, commit index, role, progress | tokio (async) |
| `store: Mutex<LsmStore>` | the key-value data | tokio |
| `apply_lock: Mutex<()>` | "only one task applies entries / snapshots / installs at a time" | tokio |
| `incoming_snapshot` | the half-received snapshot | tokio |
| `election_deadline` | when to start an election | `std` (instant, never held across `.await`) |

**Two rules that make this deadlock-free:**

1. **`node` and `store` are never held at the same time.** Look at `apply_committed`: it locks `node` to
   read pending entries (released), then locks `store` to apply (released), then locks `node` again for
   `mark_applied`. `compact_if_needed` and `install_received_snapshot` follow the same pattern.
2. **`apply_lock` is the outermost lock.** It may be held while taking `node` or `store`, but nothing
   holds `node`/`store` while waiting for `apply_lock`.

**Pattern for short critical sections:**

```rust
let args = {
    let node = self.node.lock().await;       // take the lock
    node.build_append_entries(peer, 64)      // do the quick, non-blocking work
};                                           // lock released at the closing brace
self.rpc(...).await;                         // NEVER hold the `node` lock across a network wait
```
Holding `node` across an `rpc().await` would freeze the whole node for up to 200 ms per call.

---

# Part 7 — Safety rule → code → "what if I delete it?"

This is the most valuable table in the guide. Each row is a rule Raft needs, the line(s) that enforce it,
and what would break. (Rows marked ✔ were verified by deliberately breaking the code and watching a test
or the simulator fail.)

| # | Rule | Where | If you remove it … |
|---|---|---|---|
| 1 | **One vote per term** | `node.rs:187` `can_vote` | Two candidates both win a term → two leaders. ✔ simulator: "two leaders in term 1". |
| 2 | **Persist the vote before replying** | `node.rs:191-198` | A node could vote, crash, forget, and vote again in the same term. |
| 3 | **Persist term + vote before asking for votes** | `node.rs:145-149` | Same double-vote risk after a crash mid-election. |
| 4 | **Candidate's log must be up to date** | `node.rs:184-185` | A node missing committed entries could become leader and overwrite them. ✔ simulator: "leader lacks entry committed earlier". |
| 5 | **Higher term ⇒ step down** | `node.rs:108-127` and its 6 callers | A deposed leader keeps leading ("zombie leader"). |
| 6 | **Reject stale-term requests** | `node.rs:170`, `250` | Old leaders could still modify logs. |
| 7 | **`prev_log_index/term` consistency check** | `log.rs:345` | Logs could silently diverge; Log Matching breaks. |
| 8 | **Don't truncate on a matching entry** | `log.rs:359-366` (Case A) | A delayed/duplicate message would delete newer entries. |
| 9 | **Never overwrite a committed entry** | `log.rs:369` | Committed (acknowledged!) data could be lost. |
| 10 | **Commit only up to entries this message verified** | `node.rs:276-283` | A follower commits a stale tail the leader never confirmed. ✔ unit test `heartbeat_does_not_commit_unverified_stale_tail`. |
| 11 | **Count replicas only for current-term entries** | `node.rs:524` | Figure-8: an "old" entry gets committed then overwritten. ✔ unit test `leader_does_not_commit_old_term_entry_by_counting_replicas`. |
| 12 | **New leader appends a Noop** | `node.rs:411-414` | Old-term entries stay uncommitted until a client writes. |
| 13 | **`+OK` only after apply** | `server.rs:868` | You'd acknowledge writes that aren't on a majority. |
| 14 | **Snapshot file durable before log entries dropped** | `server.rs:367-368` | A crash could lose entries that exist nowhere else. |
| 15 | **Torn-tail cleanup** | `log.rs:100-104` | A half-written last record would make restarts fail. |
| 16 | **Skip stale records after compaction crash** | `log.rs:84-87` | A crash mid-compaction would make the log unreadable. |
| 17 | **Don't compact past `last_applied`** | `node.rs:312` | The log could drop entries the store hasn't applied. |
| 18 | **Majority must acknowledge before a read** | `server.rs:750-773` | A deposed leader could return stale data. |
| 19 | **Ignore stale replies** | `node.rs:473-479` | A reply from an old term could wrongly advance progress. |
| 20 | **`match_index` only moves forward** | `node.rs:488-489` | A reordered reply could move progress backwards. |
| 21 | **Bound allocations from untrusted lengths** | `rpc.rs:115`, `codec.rs`, `snapshot.rs:55` | A malicious length field could crash a node with a huge allocation. |

---

# Part 8 — Files on disk and crash-safety ordering

For node 1 started with `main_raft -- 1`:

```
 raft_data/node1/
 ├── raft.state       21 B   current_term + voted_for           written atomically (temp+rename)
 ├── raft.log                the Raft log: [len][index,term,command] records, fsync after every append
 ├── raft.base        20 B   (only after a compaction) base_index + base_term   atomic
 ├── raft.snapshot           (only after a snapshot) RaftSnapshot bytes         atomic
 ├── wal.log, sstables/      the LsmStore's own files (its own crash recovery)
 └── *.tmp                   leftovers of an interrupted atomic write — harmless
```

**The ordering rules (always "make it durable first, then change memory / delete the old"):**

| Operation | Order |
|---|---|
| Append a log entry | write to file → fsync → *then* push to the in-memory `Vec` |
| Change term/vote | save state file (atomic) → *then* act on the new value (undo in memory if the save failed) |
| Compact the log | **(caller)** save snapshot → `raft.base` → rewrite `raft.log` (temp+rename) → update memory |
| Install a received snapshot | save `raft.snapshot` → replace store contents → update log/commit/applied |
| Acknowledge a client write | commit on a majority → apply to the store → *then* `+OK` |

---

# Part 9 — Exercises

Do these in order. Try before you look at the hint.

1. **Trace by hand.** In `tests.rs`, read `leader_replicates_commits_and_followers_learn_the_commit`.
   Draw the three logs, `commit_index` of each node, and `match_index` of the leader after every call.
   *Hint: the leader's no-op is entry 1, the client write is entry 2.*
2. **Why `max`?** In `handle_append_entries_reply`, why is it `(*matched).max(replicated)` and not just
   `replicated`? *Hint: two replies can arrive in the opposite order.*
3. **Predict, then run.** In `node.rs:184-185`, change `>=` to `>`. Which tests fail? Why?
4. **Break rule #11.** Replace line 524's condition with `if false {`. Run `cargo test --lib raft::tests`.
   Which test fails and what scenario does it model?
5. **Index math.** `base_index = 10`, 5 entries stored. What are `first_index`, `last_index`,
   `entry_at(12)`, `entry_at(10)`, `term_at(10)`, `term_at(9)`?
6. **Why does `become_leader` compute `next` before appending the no-op?** What would be wasted if it
   didn't? *Hint: the first AppendEntries to each follower.*
7. **Three outcomes.** List the three possible results of `append_from_leader` and name the situation
   for each (`Ok(true)`, `Ok(false)`, `Err`).
8. **Crash drills.** Using the diagram in 4.7, list what the file system contains after a crash
   between *B* and *C* in `compact_to`, and which line of `open` rescues you.
9. **Why two locks?** Why is there both `store` and `apply_lock` instead of just `store`?
10. **Linearizable read.** The old leader N3 is paused for 5 seconds; N1 becomes leader and commits `SET x 2`.
    N3 wakes up and a client sends `GET x` to it. Walk through `read_barrier` and show exactly where it
    refuses.
11. **Simulator.** Write a new test in `sim.rs`: 5 nodes, partition into `{0,1}` and `{2,3,4}`, write on
    both sides, heal, and assert the logs converge. Which invariant would catch a bug if the old leader
    kept its stale entries?
12. **Extension.** Add a `Get`-style `Command` variant? Where would you have to touch `codec.rs`, `rpc.rs`
    and `server.rs`? (Hint: `COMMAND_*` tags, `put_command`/`read_command`, the match in `apply_committed`.)

**Short answers**

- (2) Replies can be reordered/duplicated; progress must never go backwards.
- (5) first 11, last 15, `entry_at(12)` = 2nd stored entry, `entry_at(10)` = `None`, `term_at(10)` = `base_term`, `term_at(9)` = `None`.
- (6) The first message would start after the no-op and the no-op would need a second message; computing `next` earlier lets one message carry it.
- (7) `Ok(true)` accepted (appended, or already present); `Ok(false)` consistency check failed → leader backs up; `Err` = a protocol violation (overwriting committed data, conflicting commands with same index+term) or I/O failure.
- (9) `store` protects individual reads/writes; `apply_lock` makes the *whole* "read pending → apply → mark applied → maybe snapshot" sequence atomic with respect to other appliers.
- (10) ② fails: its heartbeats get replies with a higher term → `handle_append_entries_reply` steps down, acknowledgements < quorum → error (or ① fails because `role != Leader`).

---

# Part 10 — Known simplifications and quirks

Things you may notice while reading — none are bugs in the protocol, but good to know:

1. **Stale comment:** the header comment of `server.rs` (lines 6–7) still says reads are served *without a
   quorum check*. That was true before `read_barrier` was added; it is no longer true.
2. **Duplicated helpers:** `persistence.rs` has its own atomic-write/`temp_path_for`/`sync_directory`
   instead of using `fsutil.rs`.
3. **One TCP connection per RPC** (`rpc`, line 567). Correct but slow; a connection pool would help.
4. **No checksums** on log records: torn *tails* are handled, but a record corrupted in the middle is
   only caught by length/index sanity checks.
5. **`commit_index` isn't persisted.** After a restart it starts at the snapshot index and is
   re-learned from the leader; entries are re-applied (safe because `SET`/`DELETE` are idempotent).
6. **Snapshots are assembled in memory** on the follower (capped at 256 MiB).
7. **No membership changes** (adding/removing nodes at runtime — "joint consensus" in the paper). The
   cluster `[1, 2, 3]` is fixed in `main_raft.rs`.
8. **`DELETE`'s `NotFound` check** reads the leader's store before the write; it's guarded by
   `read_barrier`, so it's linearizable, but it costs a heartbeat round.
9. **`read_barrier` doesn't count peers that need a snapshot** (`heartbeat_acknowledged` returns `false`
   for them), which only matters during catch-up.

---

*End of guide. If a function still doesn't click: find it in Part 4, run its test, break one line, and
watch what fails — then Part 7 will tell you why.*


---

# Appendix A — What changed after V9

Everything above still holds. This appendix lists what was added on top, with a diagram of each flow.

## A.1 New and changed files

| File | What it is |
|---|---|
| `raft/types.rs` | + `ClusterConfig` (voters + learners), `ConfigChange`, `Command::Config`, `Command::Txn`, `TxnCommand`, `TxnOp` |
| `raft/codec.rs` | + encoding for config and transaction commands |
| `raft/log.rs` | + `base_config`, `latest_config`, `config_as_of`; `compact_to` now takes the config; base file format `RBS2` |
| `raft/snapshot.rs` | snapshot now carries the **config as of its index** (magic `RAFTSNP2`) |
| `raft/node.rs` | + membership (`propose_config_change`, `can_campaign`, `voter_peers`, `after_commit`, `open_joining`) |
| `raft/server.rs` | + dynamic peers, vote-request stickiness, `change_membership`, `info`, `stop`, transaction entry points |
| `raft/txn.rs` | **new**: what Prepare / Decide / Finish do to the store |
| `raft/sim.rs` | simulator now has spare servers and random membership changes |
| `shard/` | **new**: hash, ring, routing table, `ShardNode` (multi-Raft), `rebalance`, `txn` |
| `bin/main_shard.rs` | **new**: one sharded server |

## A.2 Membership changes (one server at a time)

```
 voters {1,2,3}                                       a new server 4 joins
   │
   │ propose AddLearner(4)        -> config entry in the log   (quorum still 2: a learner never counts)
   │   4 receives the log (or a snapshot, which carries the config)
   │ wait until 4 has (almost) caught up
   │ propose Promote(4)           -> voters {1,2,3,4}, quorum 3
   │
   │ propose Remove(1)            -> voters {2,3,4}
   ▼
 each step is ONE config entry, and takes effect on a server the moment it is APPENDED
```

Rules that keep it safe (each is a test, and breaking each one makes a test fail):

| Rule | Where |
|---|---|
| change the voter set by at most one server | `ConfigChange::apply` |
| only one change in flight | `has_pending_config` |
| leader must have committed an entry in its own term first | `propose_config_change` |
| a config in the log is in force immediately; an overwritten one is rolled back | `refresh_config` |
| votes count only from voters of the candidate's own config | `counted_votes` |
| a server that is a voter in its latest **or last committed** config may campaign | `can_campaign` |
| refuse vote requests only from servers we *know* are not voters | `handle_vote_request` |
| a leader that removed itself steps down once the change commits | `after_commit` |

Two real bugs the simulator found and that are fixed: a leader that crashed while removing itself could
strand the group (hence `can_campaign`), and a not-yet-informed voter vetoed the only possible leader
(hence "refuse only when we *know*").

## A.3 Sharding and multi-Raft

```
 key --stable_hash % partitions--> partition --routing table (epoch)--> servers --> Raft group --> LsmStore
```

One `ShardNode` hosts one Raft group per partition it holds. All groups share one peer port; each peer
message is prefixed with a **group id** (`encode_envelope`). Any server accepts any client command: it
runs it locally if it leads the partition, otherwise forwards it (`FORWARD <hops> <epoch> <cmd>`). A
server whose routing table is older than its peer's is told so and the two **sync tables**.

## A.4 Rebalancing a partition (`REBALANCE 1,2,3,4,5`)

```
 for every partition whose replica set must change:
   new server:  HOST p -> ADD_LEARNER -> wait until caught up -> PROMOTE
   old server:  REMOVE -> UNHOST p (data set aside, never deleted)
   publish the routing table with this one partition changed and epoch + 1
   pause (throttle)
```
The table flips only after the data is in place. Every step first looks at the group's real state, so
re-running `REBALANCE` after a crash just finishes the job.

## A.5 Cross-partition transactions (`TXN SET a 1 ; SET b 2 ; DELETE c`)

```
 coordinator partition C = the lowest partition touched

 PREPARE (Raft entry in every participant)   lock keys, vote YES / NO (a locked key => NO, no waiting)
 DECIDE  (Raft entry in C)                   COMMIT if all YES else ABORT   <- the first decision wins
 FINISH  (Raft entry in every participant)   apply or discard, release locks

 a participant stuck in "prepared" longer than the timeout asks C for the decision;
 if there is none it records ABORT in C (so a slow coordinator that wakes up later loses)
```
Atomic across partitions, and no stuck locks if a coordinator dies, because the decision is a replicated
log entry. Not provided: reads inside a transaction, or hiding a half-visible commit from plain reads.

## A.6 What the test suite proves now

* `cargo test --lib` — 164 tests (unit, simulator, and real servers on localhost).
* `cargo test --lib raft::sim -- --ignored` — the long fault-injection soak, including servers joining and
  leaving while nodes crash and the network partitions.
