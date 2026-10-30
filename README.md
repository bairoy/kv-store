# kv-store

A distributed, replicated key-value database written in Rust. Data is partitioned across servers, every partition is replicated with the Raft consensus protocol, and the cluster keeps serving reads and writes when servers fail.

## Features

- **Storage engine:** log-structured merge tree with a write-ahead log, memtable, SSTables, bloom filters, a sparse index, crash-safe compaction and atomic snapshot installation.
- **Consensus:** Raft per partition with leader election, log replication, persistent term and vote, snapshots and log compaction, linearizable reads and single-server membership changes.
- **Sharding:** consistent hashing with virtual nodes, an epoch-versioned routing table, and online rebalancing driven by Raft membership changes. Any server accepts any request and routes it.
- **Transactions:** atomic multi-key writes across partitions using two-phase commit, where every step is a replicated Raft entry.
- **Exactly-once writes:** request ids and a replicated session table make client retries safe. The bundled client library handles retries, backoff and failover.
- **Observability:** Prometheus-format metrics, structured logs with a request id on every line.
- **Backpressure:** bounded connections and in-flight writes; overload returns `BUSY` before any data is written.
- **Security:** shared-secret authentication and TLS on both client and peer ports.
- **Verification:** a deterministic fault-injection simulator and a chaos tester that kills real processes under load.

## Architecture

```text
                    client library (request ids, retries, backoff, failover)
                                          │
                 any server accepts any command and routes it
        ┌──────────────┬──────────────────┼──────────────────┬──────────────┐
     server 1       server 2           server 3           server 4
   ┌───────────┐  ┌───────────┐     ┌───────────┐      ┌───────────┐
   │ p0 p1 p3… │  │ p0 p2 p3… │     │ p0 p1 p2… │      │ p1 p2 p3… │   one Raft group per
   └───────────┘  └───────────┘     └───────────┘      └───────────┘   partition per server

   key ──hash──► partition (fixed count) ──routing table──► 3 replicas, one Raft leader
```

## Quick start

```bash
cargo build --bins

# four servers, each in its own terminal (data is written to ./shard_data)
cargo run --bin main_shard -- 1
cargo run --bin main_shard -- 2
cargo run --bin main_shard -- 3
cargo run --bin main_shard -- 4

# interactive client (discovers the other servers automatically)
cargo run --bin client 127.0.0.1:7201
> SET user:1 alice
+OK
> GET user:1
alice
> REQ me 1 INCR visits
1
> REQ me 1 INCR visits            # retry of the same request: counted once
1
> TXN SET a 1 ; SET b 2 ; DELETE c
+OK
> STATUS
```

Stop a server and keep using the client: leaders are re-elected and requests continue. Add a fifth server and move data onto it while the cluster serves traffic:

```bash
cargo run --bin main_shard -- 5
> REBALANCE 1,2,3,4,5
```

### Commands

| Command | Description |
|---|---|
| `SET k v`, `GET k`, `DELETE k` | Basic operations; reads are linearizable |
| `REQ <client> <seq> <command>` | The same write tagged with a request id, so a retry is applied once |
| `REQ <client> <seq> INCR k` | Atomically add one to an integer (requires the tag) |
| `TXN SET a 1 ; DELETE b ; …` | Atomic multi-key write across partitions |
| `ROUTE k`, `STATUS`, `GROUPINFO p` | Key placement, leadership overview, state of one partition group |
| `REBALANCE 1,2,3` | Make exactly these servers hold the data |

A reply beginning with `BUSY` means the server shed load and did not apply the write. Ambiguous outcomes such as timeouts are resolved by retrying with the same `REQ` number; the client library does this automatically.

## Live demo

A web page shows the running cluster: which server leads each partition, live metrics, a key/value playground, an exactly-once experiment, a cross-partition transaction, and buttons that crash a real server so leadership changes can be observed while requests keep succeeding. It is served by a gateway (`src/bin/gateway`, page in `web/index.html`) that starts the four servers as child processes.

```bash
cargo run --release --features gateway --bin gateway -- --demo     # open http://localhost:8080
```

The public interface is deliberately small: access is confined to the `demo:` key namespace, values are limited to 200 printable characters, writes are rate limited per visitor and globally, no administrative command is reachable, crashes require all servers to be healthy and have a cooldown, and the page is served with a strict content security policy.

## Deployment

### Single container (live demo)

```bash
docker build -f Dockerfile.demo -t kv-store-demo .
docker run -p 8080:8080 kv-store-demo
```

The container listens on `$PORT` (default 8080) and exposes `/healthz`. Run one instance with at least 512 MB of memory; storage is ephemeral. Behind a reverse proxy that sets `X-Forwarded-For`, the image applies rate limits per visitor (`GATEWAY_TRUST_PROXY=1`).

On Railway, create a project from the GitHub repository. `railway.toml` selects `Dockerfile.demo` and the `/healthz` check; then generate a public domain for port 8080.

### Multi-node cluster (Docker Compose)

```bash
scripts/gen-certs.sh certs                                  # private CA and server certificate
mkdir -p secrets && openssl rand -hex 24 > secrets/kv_token && chmod 644 secrets/kv_token
docker compose up --build -d                                # four nodes with health checks and volumes

export KV_TLS_CA=certs/ca.pem KV_AUTH_TOKEN_FILE=secrets/kv_token
cargo run --bin client 127.0.0.1:7201
```

`docker compose --profile expand up -d` starts a fifth node, which joins with `REBALANCE 1,2,3,4,5`. Client ports are published on `127.0.0.1` only; use a private network or a TLS-terminating proxy for remote clients.

## Configuration

Servers are configured through environment variables.

| Variable | Description | Default |
|---|---|---|
| `SHARD_SERVERS` | Cluster members as `id=host[:peer_port[:client_port]]`, comma separated | five servers on 127.0.0.1 |
| `SHARD_PEER_PORT`, `SHARD_CLIENT_PORT` | Ports for entries that give none | `9200+id`, `7200+id` |
| `SHARD_INITIAL` | Servers that hold data when the cluster is created | all listed (four locally) |
| `SHARD_BIND_HOST` | Address to listen on | `127.0.0.1` locally, `0.0.0.0` when `SHARD_SERVERS` is set |
| `SHARD_ID` | Server id (alternative to the command-line argument) | |
| `SHARD_DATA_DIR` | Data directory | `shard_data` |
| `SHARD_PARTITIONS`, `SHARD_REPLICATION` | Partition count and replicas per partition (fixed at creation) | 8, 3 |
| `SHARD_MAX_CONNECTIONS`, `SHARD_MAX_PENDING_WRITES` | Backpressure limits | 1024 each |
| `SHARD_METRICS_BIND`, `SHARD_METRICS_PORT` | Metrics endpoint | `127.0.0.1`, `9300+id` |
| `RAFT_SNAPSHOT_THRESHOLD` | Log entries between snapshots | 1000 |
| `RUST_LOG` | Log level | `info` |

Gateway:

| Variable | Description | Default |
|---|---|---|
| `PORT`, `GATEWAY_BIND` | Listen address | `0.0.0.0:8080` |
| `GATEWAY_SERVERS` | `id=host:port,...` client addresses (proxy mode, without `--demo`) | `127.0.0.1:7201`–`7204` |
| `GATEWAY_METRICS` | `id=host:port,...` metrics addresses | client host, port `9300+id` |
| `GATEWAY_TRUST_PROXY` | Take the visitor address from `X-Forwarded-For` | off |
| `GATEWAY_DEMO_DIR`, `GATEWAY_SERVER_BINARY` | Demo data directory and server binary | temp dir, `main_shard` next to the gateway |

Without `--demo` the gateway proxies to an existing cluster, including a secured one, and the crash controls are hidden.

## Security

| Variable | Description |
|---|---|
| `KV_AUTH_TOKEN` or `KV_AUTH_TOKEN_FILE` | Shared secret. Clients send `AUTH <token>` first; servers present the same secret to each other on the peer port. Without it a connection cannot issue commands. |
| `KV_TLS_CERT`, `KV_TLS_KEY`, `KV_TLS_CA` | Encrypt both ports (TLS 1.2/1.3). Servers present the certificate; clients and peers verify it against the CA. All three must be set together. |

The client tool, `chaos`, `bench` and `ClientOptions::from_env()` read the token and `KV_TLS_CA` from the same variables. A server listening on a non-loopback address without a token or TLS logs a warning at startup. `scripts/gen-certs.sh` creates a CA and a certificate valid for `localhost`, `127.0.0.1` and `node1`–`node8`.

## Observability

```bash
curl -s 127.0.0.1:9301/metrics | grep -E 'raft_commit_seconds|requests_total|session_apply'
RUST_LOG=debug cargo run --bin main_shard -- 1      # every log line of a request carries req=<client>:<seq>
```

Metrics cover WAL and Raft-log fsync latency, commit latency, elections and leader changes, replication lag per follower, keys per partition, requests by outcome, deduplicated retries, and rejected connections and writes.

## Reliability and testing

The project has roughly 230 unit and integration tests (`cargo test --lib`), plus:

- **Fault-injection simulator.** A deterministic Raft simulator injects crashes, message loss, partitions and membership changes from a seeded schedule and checks the invariants after every step: at most one leader per term, committed entries never lost or rewritten, matching logs. Two long random-fault soaks are `#[ignore]`d (`cargo test --lib -- --ignored`).
- **Mutation testing.** Key safety rules (vote rules, commit rules, membership changes, two-phase commit, deduplication) were removed one at a time to confirm that a test fails each time. Two liveness bugs were found and fixed this way.
- **Chaos testing on real processes** (`cargo run --bin chaos`). Four servers run under write load while faults are injected (`kill -9`, `SIGSTOP`, optional rebalancing). Afterwards the tool verifies that no acknowledged write was lost, every `INCR` was counted exactly once, transactions were atomic, no partition had two leaders in a term, and replicas converged. The checker is itself validated: with deduplication deliberately disabled (`--features fault-injection`, `KV_FAULT_NO_DEDUP=1`) it reports the violation.

## Performance

Measured on one 8-core macOS laptop with a release build; all servers and the load generator on the same machine (`cargo run --release --bin bench`).

| Load | Result |
|---|---|
| 1 writer, 100-byte values | 44 writes/s, p50 22 ms, p99 38 ms |
| 8 writers | 60 writes/s, p50 120 ms, p99 326 ms |

Throughput is bound by disk flush latency rather than CPU: the Raft log fsync averaged about 12 ms on this machine, and a write waits for fsyncs on a majority of replicas. Higher numbers are expected on Linux servers with SSDs. During chaos runs, clients ride out a killed or frozen server with a worst single-operation latency of about 3–5 s, which covers the election.

## Limitations

- **Security model:** one shared secret for clients and servers (no per-user accounts or roles), server-side TLS only, no encryption at rest, manual certificate rotation.
- **Disk flushes on async threads:** they block runtime worker threads, so heavy write load can delay heartbeats (visible in `raft_elections_total`).
- **Read-heavy load:** each linearizable read confirms leadership with a heartbeat round, which can trigger elections under sustained load. A shared read barrier or leader leases would address this.
- **Fault model in tests:** process-level only; packet-level partitions and delays are not simulated.
- **Rebalancing** is operator-triggered, moves one partition at a time, and the partition count is fixed at creation. There is no leadership-transfer command.
- **Transactions** support writes only and abort on conflict instead of waiting; plain reads do not observe transaction locks.
- **Session table** is bounded (10,000 clients, oldest registration evicted first); an evicted client that returns can have a retry re-applied.

## Project layout

```text
src/
  storage/        memtable, bloom filter, sparse index, SSTable, LSM store (WAL, compaction, snapshot install)
  raft/           types, codec, rpc, log, persistence, snapshot, node (protocol), server (async),
                  session (exactly-once), txn (two-phase commit state machine), sim (simulator)
  shard/          hash, ring, routing table, cluster config, node (multi-Raft and routing), rebalance, txn
  client/         retrying client library
  net/            framing, protocol, request parsing, security (token authentication and TLS)
  metrics.rs      counters, gauges, histograms, /metrics endpoint
  logging.rs      structured logging
  legacy/ service/ replication/   standalone server and leader/follower replication
  bin/            main_shard, main_raft, client, chaos, bench, gateway (live demo)
web/              live demo page
scripts/          certificate generation
```

## References

The Raft paper and Diego Ongaro's thesis (membership changes, client sessions); Amazon's Dynamo paper (consistent hashing); the LevelDB and RocksDB designs (LSM trees); the Jepsen approach to testing distributed systems.

## License

MIT. See `LICENSE`.
