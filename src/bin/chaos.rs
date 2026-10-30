//! Chaos test: run the real cluster, hurt it, and check what it promised.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use kv_store::client::{Client, ClientError, ClientOptions, Reply};

const SERVERS: [u64; 4] = [1, 2, 3, 4];
const PARTITIONS: u32 = 8;

fn client_address(server: u64) -> String {
    format!("127.0.0.1:{}", 7200 + server)
}

struct Options {
    seconds: u64,
    seed: u64,
    workers: usize,
    rebalance: bool,
    faults: bool,
    keep_data: bool,
}

fn parse_options() -> Options {
    let mut options = Options {
        seconds: 30,
        seed: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(1),
        workers: 4,
        rebalance: false,
        faults: true,
        keep_data: false,
    };

    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        let mut number = |name: &str| -> u64 {
            args.next()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| {
                    eprintln!("{name} needs a number");
                    std::process::exit(2);
                })
        };

        match arg.as_str() {
            "--seconds" => options.seconds = number("--seconds"),
            "--seed" => options.seed = number("--seed"),
            "--workers" => options.workers = number("--workers") as usize,
            "--rebalance" => options.rebalance = true,
            "--no-faults" => options.faults = false,
            "--keep-data" => options.keep_data = true,
            other => {
                eprintln!("unknown option {other}; see the header of src/bin/chaos.rs");
                std::process::exit(2);
            }
        }
    }

    options
}

#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

struct Cluster {
    binary: PathBuf,
    dir: PathBuf,
    children: HashMap<u64, Child>,
    frozen: BTreeSet<u64>,
}

impl Cluster {
    fn start_server(&mut self, server: u64) {
        let log = File::options()
            .create(true)
            .append(true)
            .open(self.dir.join(format!("server-{server}.log")))
            .expect("open server log");

        let child = Command::new(&self.binary)
            .arg(server.to_string())
            .current_dir(&self.dir)
            .env("SHARD_TXN_TIMEOUT_MS", "3000")
            .env("SHARD_REBALANCE_PAUSE_MS", "100")
            .stdout(Stdio::from(log.try_clone().expect("clone log")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("start main_shard (run `cargo build --bins` first)");

        self.children.insert(server, child);
    }

    fn crash(&mut self, server: u64) {
        if let Some(mut child) = self.children.remove(&server) {
            let _ = child.kill();
            let _ = child.wait();
        }

        self.frozen.remove(&server);
    }

    fn signal(&self, server: u64, signal: &str) {
        if let Some(child) = self.children.get(&server) {
            let _ = Command::new("kill")
                .arg(signal)
                .arg(child.id().to_string())
                .status();
        }
    }

    fn freeze(&mut self, server: u64) {
        self.signal(server, "-STOP");
        self.frozen.insert(server);
    }

    fn thaw(&mut self, server: u64) {
        self.signal(server, "-CONT");
        self.frozen.remove(&server);
    }

    fn is_up(&self, server: u64) -> bool {
        self.children.contains_key(&server)
    }

    fn heal(&mut self) {
        for server in self.frozen.clone() {
            self.thaw(server);
        }

        for server in SERVERS {
            if !self.is_up(server) {
                self.start_server(server);
            }
        }
    }

    fn shutdown(&mut self) {
        for server in SERVERS {
            self.thaw(server);
            self.crash(server);
        }
    }
}

type SharedCluster = Arc<Mutex<Cluster>>;

fn lock(cluster: &SharedCluster) -> std::sync::MutexGuard<'_, Cluster> {
    cluster.lock().unwrap_or_else(|e| e.into_inner())
}

fn all_clients() -> Vec<String> {
    SERVERS
        .iter()
        .map(|server| client_address(*server))
        .collect()
}

fn workload_options() -> ClientOptions {
    ClientOptions {
        request_timeout: Duration::from_secs(4),
        max_attempts: 40,
        deadline: Duration::from_secs(40),
        base_backoff: Duration::from_millis(30),
        max_backoff: Duration::from_millis(500),
        ..base_options()
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Status {
    Acked,
    Maybe,
    Aborted,
}

#[derive(Default)]
struct WorkerReport {
    operations: u64,
    failures: u64,
    incr_acked: u64,
    incr_maybe: u64,
    last_counter: i64,
    sets: Vec<(String, String, Status)>,
    pairs: Vec<(String, String, String, Status)>,
    violations: Vec<String>,
    latencies: Vec<Duration>,
    retries: u64,
}

fn run_worker(worker: usize, seed: u64, stop: Arc<AtomicBool>) -> WorkerReport {
    let mut client = Client::with_options(all_clients(), workload_options());
    let mut rng = Rng::new(seed ^ ((worker as u64 + 1) * 0x1000_0001));
    let mut report = WorkerReport::default();
    let counter_key = format!("ctr:{worker}");
    let mut n = 0u64;

    while !stop.load(Ordering::SeqCst) {
        n += 1;
        report.operations += 1;
        let started = Instant::now();
        let pick = rng.below(100);

        if pick < 45 {
            match client.incr(&counter_key) {
                Ok(value) => {
                    if value <= report.last_counter {
                        report.violations.push(format!(
                            "{counter_key}: INCR returned {value} after {} (a retry counted twice, or an ack was lost)",
                            report.last_counter
                        ));
                    }
                    report.last_counter = value;
                    report.incr_acked += 1;
                }
                Err(ClientError::Unavailable { maybe_applied, .. }) => {
                    report.failures += 1;
                    if maybe_applied {
                        report.incr_maybe += 1;
                    }
                }
                Err(other) => report
                    .violations
                    .push(format!("{counter_key}: unexpected {other}")),
            }
        } else if pick < 75 {
            let key = format!("s:{worker}:{n}");
            let value = format!("v{n}-{}", rng.below(1_000_000));

            match client.set(&key, &value) {
                Ok(()) => report.sets.push((key, value, Status::Acked)),
                Err(ClientError::Unavailable { maybe_applied, .. }) => {
                    report.failures += 1;
                    if maybe_applied {
                        report.sets.push((key, value, Status::Maybe));
                    }
                }
                Err(other) => report
                    .violations
                    .push(format!("SET {key}: unexpected {other}")),
            }
        } else if pick < 85 {
            let acked: Vec<&(String, String, Status)> = report
                .sets
                .iter()
                .filter(|(_, _, status)| *status == Status::Acked)
                .collect();

            if !acked.is_empty() {
                let (key, value, _) = acked[rng.below(acked.len() as u64) as usize].clone();

                match client.get(&key) {
                    Ok(Some(found)) if found == value => {}
                    Ok(found) => report.violations.push(format!(
                        "GET {key}: acked value {value:?} but read {found:?}"
                    )),
                    Err(_) => report.failures += 1,
                }
            }
        } else if pick < 95 {
            let a = format!("ta:{worker}:{n}");
            let b = format!("tb:{worker}:{n}");
            let value = format!("t{n}");

            match client.txn(&[(&a, Some(&value)), (&b, Some(&value))]) {
                Ok(()) => report.pairs.push((a, b, value, Status::Acked)),
                Err(ClientError::Aborted(_)) => report.pairs.push((a, b, value, Status::Aborted)),
                Err(ClientError::Unavailable { maybe_applied, .. }) => {
                    report.failures += 1;
                    if maybe_applied {
                        report.pairs.push((a, b, value, Status::Maybe));
                    }
                }
                Err(other) => report
                    .violations
                    .push(format!("TXN {a}/{b}: unexpected {other}")),
            }
        }

        report.latencies.push(started.elapsed());
        thread::sleep(Duration::from_millis(5));
    }

    report.retries = client.stats().retries;
    report
}

#[derive(Default)]
struct FaultLog {
    crashes: u64,
    freezes: u64,
    rebalances: u64,
}

fn run_nemesis(
    cluster: SharedCluster,
    seed: u64,
    rebalance: bool,
    stop: Arc<AtomicBool>,
) -> FaultLog {
    let mut rng = Rng::new(seed ^ 0xFA17);
    let mut log = FaultLog::default();
    let started = Instant::now();
    let stamp = |text: String| println!("[{:>5.1}s] {text}", started.elapsed().as_secs_f64());

    let sleep_while_running = |millis: u64| {
        let until = Instant::now() + Duration::from_millis(millis);

        while Instant::now() < until && !stop.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(50));
        }
    };

    while !stop.load(Ordering::SeqCst) {
        sleep_while_running(1000 + rng.below(2500));

        if stop.load(Ordering::SeqCst) {
            break;
        }

        let victim = SERVERS[rng.below(SERVERS.len() as u64) as usize];
        let action = rng.below(if rebalance { 10 } else { 7 });

        match action {
            0..=3 => {
                stamp(format!("kill -9 server {victim}"));
                lock(&cluster).crash(victim);
                log.crashes += 1;

                sleep_while_running(1500 + rng.below(3000));

                stamp(format!("restart server {victim}"));
                lock(&cluster).start_server(victim);
            }
            4..=6 => {
                stamp(format!("freeze server {victim} (SIGSTOP)"));
                lock(&cluster).freeze(victim);
                log.freezes += 1;

                sleep_while_running(1000 + rng.below(3000));

                stamp(format!("thaw server {victim}"));
                lock(&cluster).thaw(victim);
            }
            _ => {
                stamp("rebalance: take server 4 out".to_string());
                admin("REBALANCE 1,2,3", Duration::from_secs(120));
                stamp("rebalance: bring server 4 back".to_string());
                admin("REBALANCE 1,2,3,4", Duration::from_secs(120));
                log.rebalances += 1;
            }
        }
    }

    log
}

fn admin(line: &str, timeout: Duration) -> Option<Reply> {
    let options = ClientOptions {
        admin_timeout: timeout,
        ..base_options()
    };

    for server in SERVERS {
        let mut client = Client::with_options(vec![client_address(server)], options.clone());

        if let Ok(reply) = client.execute_line(line) {
            return Some(reply);
        }
    }

    None
}

type LeaderTerms = BTreeMap<(u32, u64), BTreeSet<u64>>;

fn run_monitor(stop: Arc<AtomicBool>) -> LeaderTerms {
    let mut seen = LeaderTerms::new();
    let options = ClientOptions {
        request_timeout: Duration::from_millis(400),
        admin_timeout: Duration::from_millis(400),
        ..base_options()
    };

    while !stop.load(Ordering::SeqCst) {
        for server in SERVERS {
            let mut client = Client::with_options(vec![client_address(server)], options.clone());

            for partition in 0..PARTITIONS {
                if let Ok(Reply::Value(text)) =
                    client.execute_line(&format!("GROUPINFO {partition}"))
                    && let Some((role, term, _)) = parse_group_info(&text)
                    && role == "Leader"
                {
                    seen.entry((partition, term)).or_default().insert(server);
                }
            }
        }

        thread::sleep(Duration::from_millis(300));
    }

    seen
}

fn parse_group_info(text: &str) -> Option<(String, u64, u64)> {
    let field = |name: &str| {
        text.split_whitespace()
            .find_map(|part| part.strip_prefix(&format!("{name}=")))
    };

    Some((
        field("role")?.to_string(),
        field("term")?.parse().ok()?,
        field("commit")?.parse().ok()?,
    ))
}

fn verify_workload(reports: &[WorkerReport]) -> Vec<String> {
    let mut violations = Vec::new();
    let options = ClientOptions {
        max_attempts: 60,
        deadline: Duration::from_secs(60),
        ..workload_options()
    };
    let mut client = Client::with_options(all_clients(), options);

    let mut read = |key: &str| -> Result<Option<String>, String> {
        client.get(key).map_err(|error| error.to_string())
    };

    for (worker, report) in reports.iter().enumerate() {
        violations.extend(report.violations.iter().cloned());

        let key = format!("ctr:{worker}");

        match read(&key) {
            Ok(found) => {
                let value: i64 = found.and_then(|text| text.parse().ok()).unwrap_or(0);
                let low = report.incr_acked as i64;
                let high = low + report.incr_maybe as i64;

                if value < low || value > high {
                    violations.push(format!(
                        "{key}: final value {value} outside [{low}, {high}] \
                         (acked {}, unknown {}): {}",
                        report.incr_acked,
                        report.incr_maybe,
                        if value > high {
                            "an INCR was applied more than once"
                        } else {
                            "an acknowledged INCR was lost"
                        }
                    ));
                }
            }
            Err(error) => violations.push(format!("{key}: could not be read back: {error}")),
        }

        for (key, value, status) in &report.sets {
            match read(key) {
                Ok(found) => match (status, found) {
                    (Status::Acked, Some(found)) if &found == value => {}
                    (Status::Acked, found) => violations
                        .push(format!("acked SET {key}={value:?} reads back as {found:?}")),
                    (Status::Maybe, Some(found)) if &found != value => violations.push(format!(
                        "unknown SET {key}={value:?} reads back as {found:?}"
                    )),
                    _ => {}
                },
                Err(error) => violations.push(format!("{key}: could not be read back: {error}")),
            }
        }

        for (a, b, value, status) in &report.pairs {
            let (first, second) = match (read(a), read(b)) {
                (Ok(first), Ok(second)) => (first, second),
                (Err(error), _) | (_, Err(error)) => {
                    violations.push(format!("{a}/{b}: could not be read back: {error}"));
                    continue;
                }
            };

            let both = first.as_deref() == Some(value.as_str())
                && second.as_deref() == Some(value.as_str());
            let neither = first.is_none() && second.is_none();

            let ok = match status {
                Status::Acked => both,
                Status::Maybe => both || neither,
                Status::Aborted => neither,
            };

            if !ok {
                violations.push(format!(
                    "transaction {status:?} on {a}/{b}: found {first:?} and {second:?} (not atomic)"
                ));
            }
        }
    }

    violations
}

fn verify_leaders(seen: &LeaderTerms) -> Vec<String> {
    seen.iter()
        .filter(|(_, leaders)| leaders.len() > 1)
        .map(|((partition, term), leaders)| {
            format!("partition {partition} had leaders {leaders:?} in the same term {term}")
        })
        .collect()
}

fn verify_convergence() -> Vec<String> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let options = ClientOptions {
        request_timeout: Duration::from_secs(2),
        admin_timeout: Duration::from_secs(2),
        ..base_options()
    };

    loop {
        let mut commits: BTreeMap<u32, BTreeMap<u64, u64>> = BTreeMap::new();

        for server in SERVERS {
            let mut client = Client::with_options(vec![client_address(server)], options.clone());

            for partition in 0..PARTITIONS {
                if let Ok(Reply::Value(text)) =
                    client.execute_line(&format!("GROUPINFO {partition}"))
                    && let Some((_, _, commit)) = parse_group_info(&text)
                {
                    commits.entry(partition).or_default().insert(server, commit);
                }
            }
        }

        let diverged: Vec<String> = commits
            .iter()
            .filter(|(_, by_server)| by_server.values().collect::<BTreeSet<_>>().len() > 1)
            .map(|(partition, by_server)| {
                format!(
                    "partition {partition}: replicas disagree on the commit index {by_server:?}"
                )
            })
            .collect();

        if diverged.is_empty() || Instant::now() >= deadline {
            return diverged;
        }

        thread::sleep(Duration::from_millis(500));
    }
}

fn find_binary() -> PathBuf {
    let exe = std::env::current_exe().expect("current exe");
    let sibling = exe.with_file_name("main_shard");

    if !sibling.exists() {
        eprintln!(
            "{} not found: run `cargo build --bins` first (same profile as chaos)",
            sibling.display()
        );
        std::process::exit(2);
    }

    sibling
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }

    sorted[((sorted.len() - 1) as f64 * fraction) as usize]
}

fn main() {
    let options = parse_options();
    let dir = std::env::current_dir().expect("cwd").join("chaos_data");

    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create chaos_data");

    println!(
        "chaos: seed {} (rerun with --seed {} to repeat the fault schedule)",
        options.seed, options.seed
    );
    println!(
        "chaos: {} s, {} workers, faults {}, rebalance {}",
        options.seconds,
        options.workers,
        if options.faults { "on" } else { "off" },
        if options.rebalance { "on" } else { "off" },
    );

    let cluster: SharedCluster = Arc::new(Mutex::new(Cluster {
        binary: find_binary(),
        dir: dir.clone(),
        children: HashMap::new(),
        frozen: BTreeSet::new(),
    }));

    for server in SERVERS {
        lock(&cluster).start_server(server);
    }

    let mut probe = Client::with_options(
        all_clients(),
        ClientOptions {
            deadline: Duration::from_secs(90),
            max_attempts: 200,
            ..base_options()
        },
    );

    if let Err(error) = probe.set("chaos-ready", "1") {
        eprintln!("chaos: the cluster never became ready: {error}");
        lock(&cluster).shutdown();
        std::process::exit(2);
    }

    println!("chaos: cluster ready, starting the workload");

    let stop = Arc::new(AtomicBool::new(false));
    let monitor_stop = Arc::new(AtomicBool::new(false));

    let workers: Vec<_> = (0..options.workers)
        .map(|worker| {
            let stop = Arc::clone(&stop);
            let seed = options.seed;
            thread::spawn(move || run_worker(worker, seed, stop))
        })
        .collect();

    let monitor = {
        let stop = Arc::clone(&monitor_stop);
        thread::spawn(move || run_monitor(stop))
    };

    let nemesis = if options.faults {
        let cluster = Arc::clone(&cluster);
        let stop = Arc::clone(&stop);
        let (seed, rebalance) = (options.seed, options.rebalance);
        Some(thread::spawn(move || {
            run_nemesis(cluster, seed, rebalance, stop)
        }))
    } else {
        None
    };

    thread::sleep(Duration::from_secs(options.seconds));
    stop.store(true, Ordering::SeqCst);

    let faults = nemesis.map(|handle| handle.join().unwrap_or_default());
    let reports: Vec<WorkerReport> = workers
        .into_iter()
        .map(|handle| handle.join().expect("worker panicked"))
        .collect();

    println!("chaos: faults stopped; healing the cluster and checking invariants");
    lock(&cluster).heal();

    let mut violations = verify_workload(&reports);
    violations.extend(verify_convergence());

    monitor_stop.store(true, Ordering::SeqCst);
    let leaders = monitor.join().unwrap_or_default();
    violations.extend(verify_leaders(&leaders));

    lock(&cluster).shutdown();

    let mut latencies: Vec<Duration> = reports
        .iter()
        .flat_map(|report| report.latencies.iter().copied())
        .collect();
    latencies.sort();

    println!();
    println!(
        "================= chaos report (seed {}) =================",
        options.seed
    );
    println!(
        "operations      {}  (failed after all retries: {}, client retries: {})",
        reports.iter().map(|r| r.operations).sum::<u64>(),
        reports.iter().map(|r| r.failures).sum::<u64>(),
        reports.iter().map(|r| r.retries).sum::<u64>()
    );
    println!(
        "INCR            acked {}, unknown {}",
        reports.iter().map(|r| r.incr_acked).sum::<u64>(),
        reports.iter().map(|r| r.incr_maybe).sum::<u64>()
    );
    println!(
        "SET             {} recorded, TXN pairs {}",
        reports.iter().map(|r| r.sets.len()).sum::<usize>(),
        reports.iter().map(|r| r.pairs.len()).sum::<usize>()
    );

    if let Some(faults) = faults {
        println!(
            "faults          {} crashes (kill -9), {} freezes (SIGSTOP), {} rebalances",
            faults.crashes, faults.freezes, faults.rebalances
        );
    }

    println!(
        "latency         p50 {:?}  p99 {:?}  max {:?}   (max ~ the longest outage a client rode out)",
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.99),
        latencies.last().copied().unwrap_or_default()
    );
    println!(
        "leader terms    {} (partition, term) pairs observed",
        leaders.len()
    );

    if violations.is_empty() {
        println!("RESULT          PASS: every invariant held");
    } else {
        println!("RESULT          FAIL: {} violation(s)", violations.len());

        for violation in violations.iter().take(20) {
            println!("  - {violation}");
        }
    }

    if !options.keep_data && violations.is_empty() {
        let _ = std::fs::remove_dir_all(Path::new(&dir));
    } else {
        println!("data and server logs kept in {}", dir.display());
    }

    std::process::exit(if violations.is_empty() { 0 } else { 1 });
}

fn base_options() -> ClientOptions {
    ClientOptions::from_env().unwrap_or_else(|error| {
        eprintln!("security configuration: {error}");
        std::process::exit(2);
    })
}
