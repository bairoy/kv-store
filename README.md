# kv-store

A **sharded, replicated key-value store written from scratch in Rust**: an LSM-tree
storage engine, Raft consensus, consistent-hash partitioning with online
rebalancing, cross-partition transactions, exactly-once client writes, and a
chaos-testing harness that kills real processes while checking the guarantees.

The system is developed in stages, from a single-node storage engine through
replication, consensus, sharding and production hardening. Scope and known
limitations are listed under [Limitations](#limitations).

```text
                       client library (request ids, retry, backoff, discovery)
                                          │
                 any server accepts any command and routes it
        ┌──────────────┬──────────────────┼──────────────────┬──────────────┐
     server 1       server 2           server 3           server 4       (server 5 joins later)
   ┌───────────┐  ┌───────────┐     ┌───────────┐      ┌───────────┐
   │ p0 p1 p3… │  │ p0 p2 p3… │     │ p0 p1 p2… │      │ p1 p2 p3… │   one Raft group per
   └───────────┘  └───────────┘     └───────────┘      └───────────┘   partition per server
   key ──hash──► partition (fixed count) ──routing table──► 3 replicas; one is the Raft leader
```

## What it does

| Layer | What is implemented |
|---|---|
| **Storage (V1–V7)** | Write-ahead log, memtable, SSTables with bloom filters and a sparse index, compaction with crash recovery, atomic snapshot install |
| **Replication (V8)** | Leader/follower log shipping (retained alongside Raft) |
| **Consensus (V9)** | Raft: leader election, log replication, commit rules, persistent term/vote, snapshots and log compaction, InstallSnapshot, linearizable reads, single-server membership changes (learner → voter → removal) |
| **Sharding (V10)** | Consistent-hash ring with virtual nodes for placement, a fixed partition count, an epoch-versioned routing table, multi-Raft (one Raft group per partition per server), online rebalancing driven by Raft membership changes, two-phase-commit transactions across partitions where every step is a Raft entry |
| **Hardening (V11)** | Exactly-once writes (replicated session table), a retrying client library, structured logs with request ids, Prometheus metrics, backpressure (`BUSY`), a chaos tester, an operations runbook |

## Try it

```bash
cargo build --bins

# four servers, each in its own terminal (data goes to ./shard_data)
cargo run --bin main_shard -- 1
cargo run --bin main_shard -- 2
cargo run --bin main_shard -- 3
cargo run --bin main_shard -- 4

# a client (it finds the other servers by itself)
cargo run --bin client 127.0.0.1:7201
> SET user:1 alice
+OK
> GET user:1
alice
> ROUTE user:1
key=user:1 partition=4 epoch=1 replicas=[1, 3, 4] hosted_here=false
> REQ me 1 INCR visits
1
> REQ me 1 INCR visits          # a retry of the same request: counted once
1
> TXN SET a 1 ; SET b 2 ; DELETE c      # atomic, even across partitions
+OK
> STATUS
```

(Example session; partition numbers, ids and epochs will differ on your machine.)

Kill a server (`Ctrl-C`) and keep using the client: leaders are re-elected and
requests keep working. Start a fifth server and move data onto it **while the
cluster serves traffic**:

```bash
cargo run --bin main_shard -- 5
> REBALANCE 1,2,3,4,5
moved 4 partition(s) onto servers [1, 2, 3, 4, 5]; routing table is now at epoch 9
```

### Commands

| Command | Meaning |
|---|---|
| `SET k v`, `GET k`, `DELETE k` | the basics (linearizable reads) |
| `REQ <client> <seq> <command>` | the same write, tagged so a retry is applied once |
| `REQ <client> <seq> INCR k` | add one to an integer; **requires** the tag |
| `TXN SET a 1 ; DELETE b ; …` | atomic multi-key write across partitions |
| `ROUTE k`, `STATUS`, `GROUPINFO p` | where a key lives, who leads what, one group's state |
| `REBALANCE 1,2,3` | make exactly these servers hold the data |

Errors starting `BUSY` mean the server shed load and **did not apply** the write;
anything else that is ambiguous (a timeout) is resolved by retrying with the same
`REQ` number. The bundled client does all of this for you.

### Metrics and logs

```bash
curl -s 127.0.0.1:9301/metrics | grep -E 'raft_commit_seconds|requests_total|session_apply'
RUST_LOG=debug cargo run --bin main_shard -- 1     # every line of a request carries req=<client>:<seq>
```

Metrics include WAL and Raft-log fsync latency, commit latency, elections, leader
changes, replication lag per follower, keys per partition, requests by outcome,
deduplication hits, rejected connections and writes.

## Deploy

### Docker Compose (four servers, token authentication and TLS)

```bash
scripts/gen-certs.sh certs                       # private CA + server certificate
mkdir -p secrets && openssl rand -hex 24 > secrets/kv_token && chmod 644 secrets/kv_token
docker compose up --build -d                     # node1..node4, healthchecked, one volume each

export KV_TLS_CA=certs/ca.pem KV_AUTH_TOKEN_FILE=secrets/kv_token
cargo run --bin client 127.0.0.1:7201            # client port of node1
> SET greeting hello
```

`docker compose --profile expand up -d` starts a fifth server; add it with
`REBALANCE 1,2,3,4,5`. Client ports are published on `127.0.0.1` only; put a TLS-terminating
proxy or a private network in front of them for remote clients.

### Configuration

Servers are configured with environment variables.

| Variable | Meaning | Default |
|---|---|---|
| `SHARD_SERVERS` | the cluster's servers: `1=node1,2=10.0.0.7:9300:7300,...` (`id=host[:peer_port[:client_port]]`) | five servers on 127.0.0.1 |
| `SHARD_PEER_PORT`, `SHARD_CLIENT_PORT` | ports for entries that give none | 9200+id, 7200+id |
| `SHARD_INITIAL` | servers that hold data when the cluster is first created | all listed (four locally) |
| `SHARD_BIND_HOST` | address to listen on | `127.0.0.1` locally, `0.0.0.0` when `SHARD_SERVERS` is set |
| `SHARD_ID` | server id (instead of the command-line argument) | |
| `SHARD_DATA_DIR` | where data lives | `shard_data` |
| `SHARD_PARTITIONS`, `SHARD_REPLICATION` | partition count and replicas per partition (fixed at creation) | 8, 3 |
| `SHARD_MAX_CONNECTIONS`, `SHARD_MAX_PENDING_WRITES` | backpressure limits | 1024 each |
| `SHARD_METRICS_BIND`, `SHARD_METRICS_PORT` | metrics endpoint | `127.0.0.1`, 9300+id |
| `RAFT_SNAPSHOT_THRESHOLD` | log entries between snapshots | 1000 |
| `RUST_LOG` | log level | `info` |

### Security

| Variable | Meaning |
|---|---|
| `KV_AUTH_TOKEN` or `KV_AUTH_TOKEN_FILE` | shared secret. Clients send `AUTH <token>` first; servers present the same secret to each other on the peer port. Without it a connection can do nothing. |
| `KV_TLS_CERT`, `KV_TLS_KEY`, `KV_TLS_CA` | encrypt both ports (TLS 1.2/1.3). Servers present the certificate; clients and peers verify it against the CA. All three must be set together. |

Clients (the `client` tool, `chaos`, `bench`, and `ClientOptions::from_env()` in code) read the
token and `KV_TLS_CA` from the same variables. A server listening on a non-loopback address
without a token or TLS logs a warning at startup. `scripts/gen-certs.sh` creates a CA and a
certificate valid for `localhost`, `127.0.0.1` and `node1`..`node8`.

## Live demo

A small web page shows the cluster as it runs: which server leads each of the 8 partitions, live
metrics, a key/value playground, an exactly-once experiment, a cross-partition transaction, and
buttons that `kill -9` a real server so you can watch leadership move while reads and writes keep
working. It is served by a gateway (`src/bin/gateway`, page in `web/index.html`) that runs the four
servers as child processes in *demo mode*.

```bash
cargo run --release --features gateway --bin gateway -- --demo      # then open http://localhost:8080
```

Deploy it on any host that runs containers (Fly.io, Render, Railway, a VPS):

```bash
docker build -f Dockerfile.demo -t kv-store-demo .
docker run -p 8080:8080 kv-store-demo
```

On Railway: create a project from the GitHub repository. `railway.toml` selects `Dockerfile.demo` and the
`/healthz` check; then generate a public domain for the service (port 8080) and give it at least 512 MB.

The container listens on `$PORT` (default 8080) and exposes `/healthz`. Run **one** instance (the
cluster lives inside it) with at least 512 MB of memory; storage is throw-away. Behind a proxy that
sets `X-Forwarded-For` (the image enables `GATEWAY_TRUST_PROXY=1`) rate limits apply per visitor.

The public surface is deliberately small: reads and writes are confined to the `demo:` key
namespace, values are limited to 200 printable characters, writes are rate limited per visitor and
globally, no admin command (`REBALANCE`, raw `TXN`, `GROUPOP`) is reachable, a crash needs all
servers healthy and has a cooldown, and the page is served with a strict content security policy.

| Variable | Meaning | Default |
|---|---|---|
| `PORT`, `GATEWAY_BIND` | where the gateway listens | `0.0.0.0:8080` |
| `GATEWAY_SERVERS` | `1=host:port,...` client addresses (proxy mode, without `--demo`) | `127.0.0.1:7201`-`7204` |
| `GATEWAY_METRICS` | `1=host:port,...` metrics addresses | client host, port 9300+id |
| `GATEWAY_TRUST_PROXY` | take the visitor address from `X-Forwarded-For` | off |
| `GATEWAY_DEMO_DIR`, `GATEWAY_SERVER_BINARY` | demo data directory and server binary | temp dir, `main_shard` next to the gateway |

Without `--demo` the gateway proxies to an existing cluster (including a secured one: it reads
`KV_AUTH_TOKEN` and `KV_TLS_CA` like the other tools) and the crash buttons are hidden.

## How it is tested

~210 unit and integration tests (`cargo test --lib`), plus things that go beyond
unit tests:

- **A deterministic Raft simulator** injects crashes, message loss, partitions and
  membership changes from a seeded schedule and checks the Raft invariants after
  every step (at most one leader per term, committed entries never lost or
  rewritten, logs match). Two long random-fault soaks are `#[ignore]`d:
  `cargo test --lib -- --ignored`.
- **Mutation testing, by hand:** key safety rules (vote rules, commit-only-current-term,
  membership rules, 2PC rules, deduplication) were removed one at a time and a test
  had to fail; two real liveness bugs were found this way and fixed.
- **Chaos tests on real processes** (`cargo run --bin chaos`): four `main_shard`
  servers under write load while a nemesis `kill -9`s and `SIGSTOP`s them (and
  optionally rebalances). Afterwards it checks that no acknowledged write is
  lost, every `INCR` counted exactly once, transactions are atomic, no partition
  had two leaders in a term, and replicas converged.
  Recorded runs all pass (15 s control; 45 s with 8 kills; 90 s with kills **and** a
  full rebalance under load; 50 s with 6 freezes and 4 kills).
  **The checker can fail:** built with `--features fault-injection` and
  `KV_FAULT_NO_DEDUP=1` (deduplication off) it reports
  `an INCR was applied more than once`, and the counter even moves backwards
  because Raft replays entries after a restart.

## Benchmarks

On one 8-core macOS laptop, release build, all servers and the load generator on
the same machine (`cargo run --release --bin bench`):

| load | result |
|---|---|
| 1 writer, 100-byte values | 44 writes/s, p50 22 ms, p99 38 ms |
| 8 writers | 60 writes/s, p50 120 ms, p99 326 ms |

These are **bound by disk flush latency, not CPU**: `raft_log_fsync_seconds`
averaged about 12 ms per fsync here (Rust's `sync_all` is a full flush on macOS)
and a write waits for fsyncs on a majority of replicas. Expect much higher numbers
on a Linux server with an SSD. A chaos run rides out a frozen or killed server with
a worst single-operation latency of about 3–5 s (client retries across the
election).

## Limitations

- **Security.** One shared secret for clients and servers (no per-user accounts or roles), server-side TLS only (no client certificates), no encryption at rest, manual certificate rotation.
- **Fsync on async threads.** Disk flushes block tokio worker threads, so heavy
  write load can delay heartbeats; `raft_elections_total` shows it.
- **Read load can cause elections.** Each linearizable read confirms leadership
  with a heartbeat round over fresh TCP connections; read-heavy load makes
  followers time out. A shared read barrier or leader leases would fix it.
- **Faults are process-level.** The chaos harness kills and freezes processes;
  there are no packet-level partitions or delays (macOS has no `tc`).
- **Rebalancing is manual** (`REBALANCE`), one partition at a time, and the
  partition count is fixed at creation. There is no leadership-transfer command.
- **Transactions** support writes only (no reads inside a transaction) and use
  abort-on-conflict (no waiting, so no deadlock). Plain reads do not see
  transaction locks.
- **The session table is bounded** (10 000 clients, oldest registration
  forgotten first); a forgotten client that returns can be re-applied.

## Layout

```text
src/
  storage/      memtable, bloom filter, sparse index, SSTable, LSM store (WAL, compaction, snapshot install)
  raft/         types, codec, rpc, log, persistence, snapshot, node (protocol), server (async),
                session (exactly-once), txn (2PC state machine), sim (fault-injection simulator)
  shard/        hash, ring, routing table, node (multi-Raft + routing), rebalance, txn (coordinator)
  client/       the retrying client library
  bin/gateway/  HTTP gateway for the live demo (feature `gateway`), page in web/index.html
  net/security  token authentication and TLS
  net/          framing, protocol, request parsing
  metrics.rs    counters, gauges, histograms, /metrics endpoint
  logging.rs    tracing setup
  legacy/ service/ replication/   earlier single-node and leader/follower implementations
  bin/          main_shard, main_raft, client, chaos, bench, … (one per version)
```

## References

The Raft paper and Diego Ongaro's thesis (membership changes, client sessions),
Amazon's Dynamo paper (consistent hashing), the LevelDB/RocksDB designs (LSM
trees), and the Jepsen approach to testing distributed systems.

## License

MIT, see `LICENSE`.
