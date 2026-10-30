//! Process-wide metrics in the Prometheus text format.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const BUCKETS: [f64; 14] = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Clone)]
enum Value {
    Counter(u64),
    Gauge(i64),
    Histogram {
        counts: [u64; BUCKETS.len() + 1],
        sum: f64,
        count: u64,
    },
}

#[derive(Default)]
struct Registry {
    series: BTreeMap<String, Value>,
}

fn registry() -> &'static Mutex<Registry> {
    static REGISTRY: OnceLock<Mutex<Registry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Registry::default()))
}

fn with_registry<T>(action: impl FnOnce(&mut Registry) -> T) -> T {
    let mut guard = registry().lock().unwrap_or_else(|e| e.into_inner());
    action(&mut guard)
}

fn series_key(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }

    let mut labels = labels.to_vec();
    labels.sort();

    let body = labels
        .iter()
        .map(|(key, value)| format!("{key}=\"{}\"", escape(value)))
        .collect::<Vec<_>>()
        .join(",");

    format!("{name}{{{body}}}")
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub fn inc(name: &str, labels: &[(&str, &str)]) {
    add(name, labels, 1);
}

pub fn add(name: &str, labels: &[(&str, &str)], amount: u64) {
    let key = series_key(name, labels);

    with_registry(|registry| {
        if let Value::Counter(total) = registry.series.entry(key).or_insert(Value::Counter(0)) {
            *total += amount;
        }
    });
}

pub fn set_gauge(name: &str, labels: &[(&str, &str)], value: i64) {
    let key = series_key(name, labels);

    with_registry(|registry| {
        registry.series.insert(key, Value::Gauge(value));
    });
}

pub fn observe(name: &str, labels: &[(&str, &str)], seconds: f64) {
    let key = series_key(name, labels);

    with_registry(|registry| {
        let entry = registry.series.entry(key).or_insert(Value::Histogram {
            counts: [0; BUCKETS.len() + 1],
            sum: 0.0,
            count: 0,
        });

        if let Value::Histogram { counts, sum, count } = entry {
            let slot = BUCKETS
                .iter()
                .position(|bound| seconds <= *bound)
                .unwrap_or(BUCKETS.len());

            counts[slot] += 1;
            *sum += seconds;
            *count += 1;
        }
    });
}

pub fn observe_duration(name: &str, labels: &[(&str, &str)], elapsed: Duration) {
    observe(name, labels, elapsed.as_secs_f64());
}

pub fn counter_value(name: &str, labels: &[(&str, &str)]) -> u64 {
    let key = series_key(name, labels);

    with_registry(|registry| match registry.series.get(&key) {
        Some(Value::Counter(total)) => *total,
        _ => 0,
    })
}

pub fn gauge_value(name: &str, labels: &[(&str, &str)]) -> Option<i64> {
    let key = series_key(name, labels);

    with_registry(|registry| match registry.series.get(&key) {
        Some(Value::Gauge(value)) => Some(*value),
        _ => None,
    })
}

pub fn histogram_count(name: &str, labels: &[(&str, &str)]) -> u64 {
    let key = series_key(name, labels);

    with_registry(|registry| match registry.series.get(&key) {
        Some(Value::Histogram { count, .. }) => *count,
        _ => 0,
    })
}

fn base_name(series: &str) -> &str {
    series.split('{').next().unwrap_or(series)
}

pub fn render() -> String {
    let series = with_registry(|registry| registry.series.clone());

    let mut out = String::new();
    let mut last_name = String::new();

    for (key, value) in &series {
        let name = base_name(key).to_string();

        if name != last_name {
            let kind = match value {
                Value::Counter(_) => "counter",
                Value::Gauge(_) => "gauge",
                Value::Histogram { .. } => "histogram",
            };

            out.push_str(&format!("# TYPE {name} {kind}\n"));
            last_name = name.clone();
        }

        let labels = key
            .strip_prefix(&name)
            .and_then(|rest| rest.strip_prefix('{'))
            .and_then(|rest| rest.strip_suffix('}'))
            .unwrap_or("");

        match value {
            Value::Counter(total) => out.push_str(&format!("{key} {total}\n")),
            Value::Gauge(current) => out.push_str(&format!("{key} {current}\n")),
            Value::Histogram { counts, sum, count } => {
                let with = |extra: String| {
                    if labels.is_empty() {
                        format!("{{{extra}}}")
                    } else {
                        format!("{{{labels},{extra}}}")
                    }
                };

                let mut running = 0;

                for (slot, bound) in BUCKETS.iter().enumerate() {
                    running += counts[slot];
                    out.push_str(&format!(
                        "{name}_bucket{} {running}\n",
                        with(format!("le=\"{bound}\""))
                    ));
                }

                out.push_str(&format!(
                    "{name}_bucket{} {count}\n",
                    with("le=\"+Inf\"".to_string())
                ));

                let suffix = if labels.is_empty() {
                    String::new()
                } else {
                    format!("{{{labels}}}")
                };

                out.push_str(&format!("{name}_sum{suffix} {sum}\n"));
                out.push_str(&format!("{name}_count{suffix} {count}\n"));
            }
        }
    }

    out
}

pub async fn serve(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    loop {
        let (mut stream, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut request = [0u8; 1024];
            let _ = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut request)).await;

            let body = render();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );

            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_add_up_per_label_set() {
        inc("test_counter_total", &[("outcome", "ok")]);
        inc("test_counter_total", &[("outcome", "ok")]);
        inc("test_counter_total", &[("outcome", "busy")]);

        assert_eq!(counter_value("test_counter_total", &[("outcome", "ok")]), 2);
        assert_eq!(
            counter_value("test_counter_total", &[("outcome", "busy")]),
            1
        );
        assert_eq!(counter_value("test_counter_total", &[("outcome", "x")]), 0);
    }

    #[test]
    fn label_order_does_not_matter() {
        inc("test_order_total", &[("a", "1"), ("b", "2")]);
        inc("test_order_total", &[("b", "2"), ("a", "1")]);

        assert_eq!(
            counter_value("test_order_total", &[("a", "1"), ("b", "2")]),
            2
        );
    }

    #[test]
    fn a_gauge_holds_the_latest_value() {
        set_gauge("test_gauge", &[], 5);
        set_gauge("test_gauge", &[], 3);

        assert_eq!(gauge_value("test_gauge", &[]), Some(3));
    }

    #[test]
    fn histograms_render_cumulative_buckets_sum_and_count() {
        observe("test_latency_seconds", &[("p", "1")], 0.0004);
        observe("test_latency_seconds", &[("p", "1")], 0.02);
        observe("test_latency_seconds", &[("p", "1")], 100.0);

        let text = render();

        assert!(text.contains("# TYPE test_latency_seconds histogram"));
        assert!(text.contains("test_latency_seconds_bucket{p=\"1\",le=\"0.0005\"} 1"));
        assert!(text.contains("test_latency_seconds_bucket{p=\"1\",le=\"0.025\"} 2"));
        assert!(text.contains("test_latency_seconds_bucket{p=\"1\",le=\"+Inf\"} 3"));
        assert!(text.contains("test_latency_seconds_count{p=\"1\"} 3"));
        assert_eq!(histogram_count("test_latency_seconds", &[("p", "1")]), 3);
    }

    #[test]
    fn rendered_counters_use_the_text_format() {
        add("test_render_total", &[], 7);

        let text = render();

        assert!(text.contains("# TYPE test_render_total counter"));
        assert!(
            text.contains("\ntest_render_total 7\n") || text.starts_with("test_render_total 7")
        );
    }

    #[tokio::test]
    async fn the_http_endpoint_serves_the_metrics() {
        add("test_http_total", &[], 1);

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        drop(listener);

        let serve_address = address.clone();
        tokio::spawn(async move {
            let _ = serve(&serve_address).await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut stream = tokio::net::TcpStream::connect(&address).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();

        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("test_http_total 1"));
    }
}
