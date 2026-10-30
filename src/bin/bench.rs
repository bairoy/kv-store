//! Load generator: how fast is the cluster, and how slow is its tail?

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use kv_store::client::{Client, ClientOptions};

struct Options {
    servers: Vec<String>,
    threads: usize,
    seconds: u64,
    value_size: usize,
    mode: String,
}

fn parse() -> Options {
    let mut options = Options {
        servers: (1..=4).map(|n| format!("127.0.0.1:{}", 7200 + n)).collect(),
        threads: 8,
        seconds: 10,
        value_size: 100,
        mode: "set".to_string(),
    };

    let mut args = std::env::args().skip(1);

    while let Some(arg) = args.next() {
        let value = args.next().unwrap_or_else(|| {
            eprintln!("{arg} needs a value");
            std::process::exit(2);
        });

        match arg.as_str() {
            "--servers" => options.servers = value.split(',').map(str::to_string).collect(),
            "--threads" => options.threads = value.parse().unwrap_or(8),
            "--seconds" => options.seconds = value.parse().unwrap_or(10),
            "--value-size" => options.value_size = value.parse().unwrap_or(100),
            "--mode" => options.mode = value,
            other => {
                eprintln!("unknown option {other}");
                std::process::exit(2);
            }
        }
    }

    options
}

fn percentile(sorted: &[Duration], fraction: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }

    sorted[((sorted.len() - 1) as f64 * fraction) as usize]
}

fn main() {
    let options = parse();
    let value = "x".repeat(options.value_size);

    let mut probe = Client::with_options(
        options.servers.clone(),
        ClientOptions {
            deadline: Duration::from_secs(60),
            max_attempts: 200,
            ..base_options()
        },
    );

    if let Err(error) = probe.set("bench-ready", "1") {
        eprintln!("bench: the cluster is not accepting writes: {error}");
        std::process::exit(1);
    }

    println!(
        "bench: {} threads, {} s, {}-byte values, mode {}",
        options.threads, options.seconds, options.value_size, options.mode
    );

    let stop = Arc::new(AtomicBool::new(false));
    let started = Instant::now();

    let handles: Vec<_> = (0..options.threads)
        .map(|thread_number| {
            let servers = options.servers.clone();
            let stop = Arc::clone(&stop);
            let value = value.clone();
            let mode = options.mode.clone();

            thread::spawn(move || {
                let mut client = Client::with_options(servers, base_options());
                let mut latencies = Vec::new();
                let mut errors = 0u64;
                let mut n = 0u64;

                while !stop.load(Ordering::SeqCst) {
                    n += 1;
                    let key = format!("bench:{thread_number}:{}", n % 1000);
                    let begun = Instant::now();

                    let write = match mode.as_str() {
                        "get" => false,
                        "mixed" => n.is_multiple_of(2),
                        _ => true,
                    };

                    let ok = if write {
                        client.set(&key, &value).is_ok()
                    } else {
                        client.get(&key).is_ok()
                    };

                    if ok {
                        latencies.push(begun.elapsed());
                    } else {
                        errors += 1;
                    }
                }

                (latencies, errors)
            })
        })
        .collect();

    thread::sleep(Duration::from_secs(options.seconds));
    stop.store(true, Ordering::SeqCst);

    let elapsed = started.elapsed().as_secs_f64();
    let mut latencies = Vec::new();
    let mut errors = 0;

    for handle in handles {
        let (mut thread_latencies, thread_errors) = handle.join().expect("bench thread");
        latencies.append(&mut thread_latencies);
        errors += thread_errors;
    }

    latencies.sort();

    println!("operations   {} ok, {errors} failed", latencies.len());
    println!("throughput   {:.0} ops/s", latencies.len() as f64 / elapsed);
    println!(
        "latency      p50 {:?}  p95 {:?}  p99 {:?}  max {:?}",
        percentile(&latencies, 0.50),
        percentile(&latencies, 0.95),
        percentile(&latencies, 0.99),
        latencies.last().copied().unwrap_or_default()
    );
}

fn base_options() -> ClientOptions {
    ClientOptions::from_env().unwrap_or_else(|error| {
        eprintln!("security configuration: {error}");
        std::process::exit(2);
    })
}
