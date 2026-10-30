//! Abuse limits for a public demo: rate limiting and input validation.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct Bucket {
    tokens: f64,
    last: Instant,
}

#[derive(Default)]
pub struct RateLimiter {
    buckets: Mutex<HashMap<String, Bucket>>,
}

impl RateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn allow(&self, key: &str, capacity: f64, per_minute: f64) -> bool {
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());

        if buckets.len() > 10_000 {
            buckets.retain(|_, bucket| now.duration_since(bucket.last) < Duration::from_secs(600));
        }

        let bucket = buckets.entry(key.to_string()).or_insert(Bucket {
            tokens: capacity,
            last: now,
        });

        let refill = now.duration_since(bucket.last).as_secs_f64() * per_minute / 60.0;
        bucket.tokens = (bucket.tokens + refill).min(capacity);
        bucket.last = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

pub fn valid_key(key: &str) -> bool {
    (1..=40).contains(&key.len())
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub fn valid_value(value: &str) -> bool {
    value.len() <= 200 && !value.contains(" ; ") && !value.chars().any(char::is_control)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bucket_allows_its_burst_then_refuses() {
        let limiter = RateLimiter::new();

        for _ in 0..3 {
            assert!(limiter.allow("ip|write", 3.0, 0.0));
        }

        assert!(!limiter.allow("ip|write", 3.0, 0.0));
    }

    #[test]
    fn buckets_are_independent_per_key() {
        let limiter = RateLimiter::new();

        assert!(limiter.allow("a", 1.0, 0.0));
        assert!(!limiter.allow("a", 1.0, 0.0));
        assert!(limiter.allow("b", 1.0, 0.0));
    }

    #[test]
    fn tokens_come_back_over_time() {
        let limiter = RateLimiter::new();

        assert!(limiter.allow("a", 1.0, 6000.0));
        assert!(!limiter.allow("a", 1.0, 6000.0));
        std::thread::sleep(Duration::from_millis(30));
        assert!(limiter.allow("a", 1.0, 6000.0));
    }

    #[test]
    fn keys_and_values_are_validated() {
        assert!(valid_key("user-1.name_x"));
        assert!(!valid_key(""));
        assert!(!valid_key("has space"));
        assert!(!valid_key("tab\t"));
        assert!(!valid_key(&"k".repeat(41)));

        assert!(valid_value("hello world"));
        assert!(valid_value(""));
        assert!(!valid_value(&"v".repeat(201)));
        assert!(!valid_value("line\nbreak"));
        assert!(!valid_value("a ; SET b c"));
        assert!(!valid_value("\u{1}secret"));
    }
}
