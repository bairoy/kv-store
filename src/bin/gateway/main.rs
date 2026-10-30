//! HTTP gateway and web page for the public demo.

mod demo;
mod limits;
mod status;

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use kv_store::client::{Client, ClientError, ClientOptions, Reply};
use serde_json::{Value, json};

use demo::Demo;
use limits::{RateLimiter, valid_key, valid_value};

const INDEX_HTML: &str = include_str!("../../../web/index.html");
const CACHE_TTL: Duration = Duration::from_millis(500);
const NAMESPACE: &str = "demo:";

struct AppState {
    servers: Vec<(u64, String)>,
    metrics: Vec<(u64, String)>,
    options: ClientOptions,
    limiter: RateLimiter,
    trust_proxy: bool,
    demo: Option<Arc<Demo>>,
    cache: Mutex<Option<(Instant, Value)>>,
}

type Api = (StatusCode, Json<Value>);

fn error(status: StatusCode, message: impl Into<String>) -> Api {
    (status, Json(json!({ "error": message.into() })))
}

impl AppState {
    fn addresses(&self) -> Vec<String> {
        self.servers
            .iter()
            .map(|(_, address)| address.clone())
            .collect()
    }

    fn client(&self) -> Client {
        let options = ClientOptions {
            request_timeout: Duration::from_millis(1500),
            deadline: Duration::from_secs(8),
            max_attempts: 8,
            ..self.options.clone()
        };

        Client::with_options(self.addresses(), options)
    }

    fn allow(&self, ip: &str, kind: &str, capacity: f64, per_minute: f64) -> bool {
        self.limiter
            .allow(&format!("{ip}|{kind}"), capacity, per_minute)
    }

    fn client_ip(&self, headers: &HeaderMap, peer: SocketAddr) -> String {
        if self.trust_proxy
            && let Some(forwarded) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok())
            && let Some(first) = forwarded.split(',').next()
        {
            return first.trim().to_string();
        }

        peer.ip().to_string()
    }

    fn build_cluster(&self) -> Value {
        let status_options = ClientOptions {
            request_timeout: Duration::from_millis(500),
            admin_timeout: Duration::from_millis(800),
            ..self.options.clone()
        };

        let mut statuses: BTreeMap<u64, Option<status::ServerStatus>> = BTreeMap::new();
        let mut metrics_text: HashMap<u64, String> = HashMap::new();

        std::thread::scope(|scope| {
            let handles: Vec<_> = self
                .servers
                .iter()
                .map(|(id, address)| {
                    let options = status_options.clone();
                    let metrics_address = self
                        .metrics
                        .iter()
                        .find(|(metrics_id, _)| metrics_id == id)
                        .map(|(_, address)| address.clone());

                    scope.spawn(move || {
                        let mut client = Client::with_options(vec![address.clone()], options);

                        let parsed = match client.execute_line("STATUS") {
                            Ok(Reply::Value(text)) => status::parse_status(&text),
                            _ => None,
                        };

                        let metrics = parsed
                            .as_ref()
                            .and_then(|_| metrics_address.and_then(|a| status::http_get(&a)));

                        (*id, parsed, metrics)
                    })
                })
                .collect();

            for handle in handles {
                if let Ok((id, parsed, metrics)) = handle.join() {
                    statuses.insert(id, parsed);

                    if let Some(text) = metrics {
                        metrics_text.insert(id, text);
                    }
                }
            }
        });

        let epoch = statuses
            .values()
            .flatten()
            .map(|s| s.epoch)
            .max()
            .unwrap_or(0);
        let partitions = statuses
            .values()
            .flatten()
            .map(|s| s.partitions_total)
            .max()
            .unwrap_or(8);

        let grid = status::grid(&statuses, partitions);
        let ready = grid.iter().all(|row| !row["leader"].is_null());

        let sum = |name: &str, label: Option<&str>| -> f64 {
            metrics_text
                .values()
                .map(|text| status::metric_sum(text, name, label))
                .fold(0.0, |total, part| total + part)
        };

        let commits = sum("raft_commit_seconds_count", None);
        let commit_seconds = sum("raft_commit_seconds_sum", None);

        let servers: Vec<Value> = self
            .servers
            .iter()
            .map(|(id, _)| {
                let up = statuses.get(id).is_some_and(|s| s.is_some());
                let state = match (&self.demo, up) {
                    (_, true) => "running",
                    (Some(demo), false) if !demo.is_running(*id) => "restarting",
                    _ => "down",
                };
                let hosting = statuses
                    .get(id)
                    .and_then(|s| s.as_ref())
                    .map_or(0, |s| s.hosted.len());

                json!({ "id": id, "up": up, "state": state, "hosting": hosting })
            })
            .collect();

        let events: Vec<Value> = self
            .demo
            .as_ref()
            .map(|demo| {
                demo.events()
                    .into_iter()
                    .map(|(seconds_ago, text)| json!({ "ago": seconds_ago, "text": text }))
                    .collect()
            })
            .unwrap_or_default();

        json!({
            "mode": if self.demo.is_some() { "demo" } else { "proxy" },
            "chaos": self.demo.is_some(),
            "ready": ready,
            "epoch": epoch,
            "partitions": partitions,
            "servers": servers,
            "grid": grid,
            "stats": {
                "elections": sum("raft_elections_total", None),
                "commits": commits,
                "avg_commit_ms": if commits > 0.0 { commit_seconds / commits * 1000.0 } else { 0.0 },
                "requests": sum("requests_total", Some("outcome=\"ok\"")),
                "duplicates_avoided": sum("session_retries_answered_total", None)
                    + sum("session_apply_total", Some("effect=\"duplicate\"")),
            },
            "events": events,
        })
    }
}

fn parse_route(text: &str) -> (Option<u32>, Vec<u64>) {
    let partition = text
        .split_whitespace()
        .find_map(|word| word.strip_prefix("partition="))
        .and_then(|value| value.parse().ok());

    let replicas = text
        .split_once("replicas=[")
        .and_then(|(_, rest)| rest.split_once(']'))
        .map(|(list, _)| {
            list.split(',')
                .filter_map(|id| id.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default();

    (partition, replicas)
}

fn route_info(client: &mut Client, key: &str) -> (Option<u32>, Vec<u64>) {
    match client.execute_line(&format!("ROUTE {key}")) {
        Ok(Reply::Value(text)) => parse_route(&text),
        _ => (None, Vec::new()),
    }
}

fn describe(error: ClientError) -> Api {
    let status = match error {
        ClientError::Rejected(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::BAD_GATEWAY,
    };

    self::error(status, error.to_string())
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Result<T, Api> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "internal error"))
}

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; img-src data:; base-uri 'none'; form-action 'none'",
            ),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::X_FRAME_OPTIONS, "DENY"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        INDEX_HTML,
    )
}

async fn health() -> &'static str {
    "ok"
}

async fn cluster(State(state): State<Arc<AppState>>) -> Result<Json<Value>, Api> {
    {
        let cache = state.cache.lock().unwrap_or_else(|e| e.into_inner());

        if let Some((at, value)) = cache.as_ref()
            && at.elapsed() < CACHE_TTL
        {
            return Ok(Json(value.clone()));
        }
    }

    let worker = Arc::clone(&state);
    let value = blocking(move || worker.build_cluster()).await?;

    *state.cache.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), value.clone()));

    Ok(Json(value))
}

async fn kv_get(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, Api> {
    let key = query.get("key").cloned().unwrap_or_default();

    if !valid_key(&key) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "key: 1-40 letters, digits, '-', '_' or '.'",
        ));
    }

    let ip = state.client_ip(&headers, peer);

    if !state.allow(&ip, "read", 30.0, 60.0) {
        return Err(error(StatusCode::TOO_MANY_REQUESTS, "slow down a little"));
    }

    let worker = Arc::clone(&state);

    blocking(move || {
        let mut client = worker.client();
        let full = format!("{NAMESPACE}{key}");

        let value = client.get(&full).map_err(describe)?;
        let (partition, replicas) = route_info(&mut client, &full);

        Ok(Json(json!({
            "key": key, "value": value, "partition": partition, "replicas": replicas,
        })))
    })
    .await?
}

async fn kv_put(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Json<Value>, Api> {
    let key = body["key"].as_str().unwrap_or_default().to_string();
    let value = body["value"].as_str().unwrap_or_default().to_string();

    if !valid_key(&key) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "key: 1-40 letters, digits, '-', '_' or '.'",
        ));
    }

    if !valid_value(&value) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "value: at most 200 printable characters",
        ));
    }

    let ip = state.client_ip(&headers, peer);

    if !state.allow(&ip, "write", 5.0, 20.0) || !state.allow("all", "write", 300.0, 600.0) {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "write limit reached; try again in a moment",
        ));
    }

    let worker = Arc::clone(&state);

    blocking(move || {
        let mut client = worker.client();
        let full = format!("{NAMESPACE}{key}");

        client.set(&full, &value).map_err(describe)?;
        let (partition, replicas) = route_info(&mut client, &full);

        Ok(Json(json!({
            "ok": true, "key": key, "partition": partition, "replicas": replicas,
        })))
    })
    .await?
}

fn counter_value(reply: Result<Reply, ClientError>) -> Value {
    match reply {
        Ok(Reply::Value(text)) => text.parse::<i64>().map_or(Value::Null, Value::from),
        _ => Value::Null,
    }
}

async fn exactly_once(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, Api> {
    let ip = state.client_ip(&headers, peer);

    if !state.allow(&ip, "demo", 3.0, 6.0) {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "try again in a few seconds",
        ));
    }

    let worker = Arc::clone(&state);

    blocking(move || {
        let mut client = worker.client();
        let id = format!("{}-eo", client.id());
        let key = format!("{NAMESPACE}counter");

        let line = format!("REQ {id} 1 INCR {key}");
        let first = counter_value(client.execute_line(&line));
        let retry = counter_value(client.execute_line(&line));

        let naive_first = counter_value(client.execute_line(&format!("REQ {id}-a 1 INCR {key}")));
        let naive_second = counter_value(client.execute_line(&format!("REQ {id}-b 1 INCR {key}")));

        let counter = client.get(&key).ok().flatten();

        Ok(Json(json!({
            "protected": { "first_attempt": first, "retry_after_lost_reply": retry },
            "naive": { "first_attempt": naive_first, "retry_as_new_request": naive_second },
            "counter_now": counter,
        })))
    })
    .await?
}

async fn transaction(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Result<Json<Value>, Api> {
    let ip = state.client_ip(&headers, peer);

    if !state.allow(&ip, "demo", 3.0, 6.0) {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "try again in a few seconds",
        ));
    }

    let worker = Arc::clone(&state);

    blocking(move || {
        let mut client = worker.client();

        let first = format!("{NAMESPACE}txn-a");
        let (first_partition, first_replicas) = route_info(&mut client, &first);

        let mut second = format!("{NAMESPACE}txn-b");
        let mut second_route = route_info(&mut client, &second);

        for attempt in 0..16 {
            if second_route.0 != first_partition {
                break;
            }

            second = format!("{NAMESPACE}txn-b{attempt}");
            second_route = route_info(&mut client, &second);
        }

        let value = format!(
            "{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.subsec_nanos() % 10_000)
                .unwrap_or(0)
        );

        client
            .txn(&[(&first, Some(&value)), (&second, Some(&value))])
            .map_err(describe)?;

        Ok(Json(json!({
            "value": value,
            "first": { "key": first.trim_start_matches(NAMESPACE), "partition": first_partition, "replicas": first_replicas,
                       "read_back": client.get(&first).ok().flatten() },
            "second": { "key": second.trim_start_matches(NAMESPACE), "partition": second_route.0, "replicas": second_route.1,
                        "read_back": client.get(&second).ok().flatten() },
        })))
    })
    .await?
}

async fn kill(
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Result<Json<Value>, Api> {
    let Some(demo) = state.demo.clone() else {
        return Err(error(
            StatusCode::NOT_FOUND,
            "crash buttons exist only in demo mode",
        ));
    };

    let ip = state.client_ip(&headers, peer);

    if !state.allow(&ip, "chaos", 2.0, 3.0) {
        return Err(error(
            StatusCode::TOO_MANY_REQUESTS,
            "that is enough chaos for now",
        ));
    }

    let message = blocking(move || demo.kill(id))
        .await?
        .map_err(|message| error(StatusCode::CONFLICT, message))?;

    *state.cache.lock().unwrap_or_else(|e| e.into_inner()) = None;

    Ok(Json(json!({ "ok": true, "message": message })))
}

fn parse_servers(text: &str) -> Vec<(u64, String)> {
    text.split(',')
        .filter_map(|entry| {
            let (id, address) = entry.split_once('=')?;
            Some((id.trim().parse().ok()?, address.trim().to_string()))
        })
        .collect()
}

fn server_binary() -> PathBuf {
    std::env::var_os("GATEWAY_SERVER_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .expect("current exe")
                .with_file_name("main_shard")
        })
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }

            return;
        }
    }

    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    kv_store::logging::init();

    let demo_mode = std::env::args().any(|arg| arg == "--demo");

    let servers = std::env::var("GATEWAY_SERVERS")
        .ok()
        .map(|text| parse_servers(&text))
        .filter(|servers| !servers.is_empty())
        .unwrap_or_else(|| {
            (1..=4)
                .map(|id| (id, format!("127.0.0.1:{}", 7200 + id)))
                .collect()
        });

    let metrics = std::env::var("GATEWAY_METRICS")
        .ok()
        .map(|text| parse_servers(&text))
        .filter(|metrics| !metrics.is_empty())
        .unwrap_or_else(|| {
            servers
                .iter()
                .map(|(id, address)| {
                    let host = address
                        .rsplit_once(':')
                        .map_or(address.as_str(), |(h, _)| h);
                    (*id, format!("{host}:{}", 9300 + id))
                })
                .collect()
        });

    let options = ClientOptions::from_env()?;

    let demo = if demo_mode {
        let dir = std::env::var_os("GATEWAY_DEMO_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("kv-demo"));

        let ids: Vec<u64> = servers.iter().map(|(id, _)| *id).collect();
        let demo = Arc::new(Demo::start(server_binary(), dir, ids)?);

        let supervised = Arc::clone(&demo);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                supervised.tick();
            }
        });

        Some(demo)
    } else {
        None
    };

    let state = Arc::new(AppState {
        servers,
        metrics,
        options,
        limiter: RateLimiter::new(),
        trust_proxy: std::env::var("GATEWAY_TRUST_PROXY").is_ok_and(|v| v == "1"),
        demo: demo.clone(),
        cache: Mutex::new(None),
    });

    let app = Router::new()
        .route("/", get(index))
        .route("/healthz", get(health))
        .route("/api/cluster", get(cluster))
        .route("/api/kv", get(kv_get).put(kv_put))
        .route("/api/demo/exactly-once", post(exactly_once))
        .route("/api/demo/transaction", post(transaction))
        .route("/api/chaos/kill/{id}", post(kill))
        .with_state(state);

    let bind = std::env::var("GATEWAY_BIND").unwrap_or_else(|_| {
        format!(
            "0.0.0.0:{}",
            std::env::var("PORT").unwrap_or_else(|_| "8080".to_string())
        )
    });

    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!(
        "gateway listening on http://{bind} ({})",
        if demo_mode { "demo mode" } else { "proxy mode" }
    );

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;

    if let Some(demo) = demo {
        demo.shutdown();
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_route_reply_is_parsed() {
        let (partition, replicas) =
            parse_route("key=demo:a partition=3 epoch=1 replicas=[4, 2, 1] hosted_here=true");

        assert_eq!(partition, Some(3));
        assert_eq!(replicas, vec![4, 2, 1]);
        assert_eq!(parse_route("nonsense"), (None, vec![]));
    }

    #[test]
    fn server_lists_are_parsed() {
        assert_eq!(
            parse_servers("1=a:1, 2=b:2,bad,x=c:3"),
            vec![(1, "a:1".to_string()), (2, "b:2".to_string())]
        );
    }

    #[test]
    fn the_page_is_embedded_and_self_contained() {
        assert!(INDEX_HTML.contains("<title>"));
        assert!(!INDEX_HTML.contains("http://") || INDEX_HTML.contains("href=\"https://"));
        assert!(!INDEX_HTML.contains("<script src="));
    }
}
