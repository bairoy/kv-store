//! A client library that is safe to retry with.

use std::collections::HashMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::net::framing::{FrameDecoder, encode_frame};
use crate::net::request::split_session;
use crate::net::security::{client_config, server_name};

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub request_timeout: Duration,
    pub max_attempts: u32,
    pub deadline: Duration,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    pub idempotent_writes: bool,
    pub admin_timeout: Duration,
    pub auth_token: Option<String>,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub leader_addresses: HashMap<u64, String>,
}

impl ClientOptions {
    pub fn from_env() -> io::Result<Self> {
        let auth_token = match std::env::var("KV_AUTH_TOKEN_FILE") {
            Ok(path) => Some(std::fs::read_to_string(path)?.trim().to_string()),
            Err(_) => std::env::var("KV_AUTH_TOKEN").ok(),
        }
        .filter(|token| !token.is_empty());

        let tls = match std::env::var("KV_TLS_CA") {
            Ok(ca) => Some(client_config(Path::new(&ca))?),
            Err(_) => None,
        };

        Ok(Self {
            auth_token,
            tls,
            ..Self::default()
        })
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(3),
            max_attempts: 12,
            deadline: Duration::from_secs(20),
            base_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_secs(1),
            idempotent_writes: true,
            admin_timeout: Duration::from_secs(900),
            auth_token: None,
            tls: None,
            leader_addresses: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    Rejected(String),
    Aborted(String),
    Ambiguous(String),
    Unavailable {
        attempts: u32,
        last_error: String,
        maybe_applied: bool,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Rejected(message) => write!(f, "rejected: {message}"),
            ClientError::Aborted(message) => write!(f, "aborted: {message}"),
            ClientError::Ambiguous(message) => {
                write!(
                    f,
                    "outcome unknown (not retried without a request id): {message}"
                )
            }
            ClientError::Unavailable {
                attempts,
                last_error,
                maybe_applied,
            } => write!(
                f,
                "unavailable after {attempts} attempts: {last_error}{}",
                if *maybe_applied {
                    " (the write may have been applied)"
                } else {
                    ""
                }
            ),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Ok,
    Value(String),
    NotFound,
    Error(String),
}

pub fn parse_reply(text: &str) -> Reply {
    if text == "+OK" {
        Reply::Ok
    } else if text.starts_with("$-1") {
        Reply::NotFound
    } else if let Some(rest) = text.strip_prefix('$') {
        match rest.split_once("\r\n") {
            Some((length, body)) => {
                let length: usize = length.parse().unwrap_or(body.len());
                Reply::Value(body.get(..length).unwrap_or(body).to_string())
            }
            None => Reply::Error(format!("malformed reply {text:?}")),
        }
    } else if let Some(message) = text.strip_prefix("-ERR ") {
        Reply::Error(message.to_string())
    } else {
        Reply::Error(format!("unrecognised reply {text:?}"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    NotApplied,
    Unknown,
    Permanent,
    Aborted,
}

pub fn classify_error(message: &str) -> Failure {
    const NOT_APPLIED: [&str; 7] = [
        "BUSY",
        "not leader",
        "no leader elected",
        "STALE_EPOCH",
        "has not committed an entry in its term",
        "could not confirm leadership",
        "too many redirects",
    ];

    const UNKNOWN: [&str; 5] = [
        "outcome unknown",
        "timed out",
        "unavailable",
        "leadership changed",
        "could not record the decision",
    ];

    if message.starts_with("transaction aborted") {
        Failure::Aborted
    } else if NOT_APPLIED.iter().any(|marker| message.contains(marker)) {
        Failure::NotApplied
    } else if UNKNOWN.iter().any(|marker| message.contains(marker)) {
        Failure::Unknown
    } else {
        Failure::Permanent
    }
}

pub fn backoff_ceiling(options: &ClientOptions, attempt: u32) -> Duration {
    options
        .base_backoff
        .saturating_mul(1u32.checked_shl(attempt.min(16)).unwrap_or(u32::MAX))
        .min(options.max_backoff)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ClientStats {
    pub requests: u64,
    pub attempts: u64,
    pub retries: u64,
    pub ambiguous_failures: u64,
}

pub struct Client {
    servers: Vec<String>,
    options: ClientOptions,
    id: String,
    seq: u64,
    preferred: usize,
    connections: HashMap<String, Connection>,
    random: u64,
    stats: ClientStats,
}

trait Transport: Read + Write {}

impl<T: Read + Write> Transport for T {}

struct Connection {
    stream: Box<dyn Transport>,
    decoder: FrameDecoder,
}

enum Attempt {
    Reply(Reply),
    Unreachable(String),
    NoReply(String),
}

impl Client {
    pub fn new(servers: Vec<String>) -> Self {
        Self::with_options(servers, ClientOptions::default())
    }

    pub fn with_options(servers: Vec<String>, options: ClientOptions) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or(1);

        let seed = nanos ^ (u64::from(std::process::id()) << 32) ^ 0x9E37_79B9_7F4A_7C15;

        Self {
            servers,
            options,
            id: format!("c{:x}", seed & 0xFFFF_FFFF_FFFF),
            seq: 0,
            preferred: 0,
            connections: HashMap::new(),
            random: seed | 1,
            stats: ClientStats::default(),
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn stats(&self) -> ClientStats {
        self.stats
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<(), ClientError> {
        match self.write(&format!("SET {key} {value}"))? {
            Reply::Ok => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    pub fn get(&mut self, key: &str) -> Result<Option<String>, ClientError> {
        match self.read(&format!("GET {key}"))? {
            Reply::Value(value) => Ok(Some(value)),
            Reply::NotFound => Ok(None),
            other => Err(unexpected(&other)),
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<bool, ClientError> {
        match self.write(&format!("DELETE {key}"))? {
            Reply::Ok => Ok(true),
            Reply::NotFound => Ok(false),
            other => Err(unexpected(&other)),
        }
    }

    pub fn incr(&mut self, key: &str) -> Result<i64, ClientError> {
        if !self.options.idempotent_writes {
            return Err(ClientError::Rejected(
                "INCR needs idempotent_writes (a retry would count twice)".to_string(),
            ));
        }

        match self.write(&format!("INCR {key}"))? {
            Reply::Value(value) => value
                .parse()
                .map_err(|_| ClientError::Rejected(format!("not a number: {value}"))),
            other => Err(unexpected(&other)),
        }
    }

    pub fn txn(&mut self, writes: &[(&str, Option<&str>)]) -> Result<(), ClientError> {
        let body = writes
            .iter()
            .map(|(key, value)| match value {
                Some(value) => format!("SET {key} {value}"),
                None => format!("DELETE {key}"),
            })
            .collect::<Vec<_>>()
            .join(" ; ");

        match self.write(&format!("TXN {body}"))? {
            Reply::Ok => Ok(()),
            other => Err(unexpected(&other)),
        }
    }

    pub fn execute_line(&mut self, line: &str) -> Result<Reply, ClientError> {
        let command = match split_session(line) {
            Ok(Some((_, inner))) => inner,
            _ => line,
        };

        let verb = command.split_whitespace().next().unwrap_or("");

        match verb {
            "GET" => self.read(line),
            "SET" | "DELETE" | "INCR" | "TXN" => self.write(line),
            _ => self.admin(line),
        }
    }

    fn read(&mut self, line: &str) -> Result<Reply, ClientError> {
        self.stats.requests += 1;
        self.run(line.to_string(), true)
    }

    fn write(&mut self, line: &str) -> Result<Reply, ClientError> {
        self.stats.requests += 1;

        let tagged = split_session(line)
            .map(|found| found.is_some())
            .unwrap_or(false);

        if self.options.idempotent_writes && !tagged {
            self.seq += 1;
            let line = format!("REQ {} {} {line}", self.id, self.seq);
            return self.run(line, true);
        }

        self.run(line.to_string(), tagged)
    }

    fn admin(&mut self, line: &str) -> Result<Reply, ClientError> {
        self.stats.requests += 1;

        let frame = encode_frame(line.as_bytes())
            .map_err(|error| ClientError::Rejected(format!("invalid command: {error}")))?;

        let mut last_error = "no servers".to_string();

        for offset in 0..self.servers.len() {
            let server = self.servers[(self.preferred + offset) % self.servers.len()].clone();

            let mut connection = match self.connect_with(&server, self.options.admin_timeout) {
                Ok(connection) => connection,
                Err(error) => {
                    last_error = format!("{server}: {error}");
                    continue;
                }
            };

            if let Err(error) = connection.stream.write_all(&frame) {
                last_error = format!("{server}: {error}");
                continue;
            }

            return match read_reply(&mut connection) {
                Ok(text) => Ok(parse_reply(&text)),
                Err(error) => Err(ClientError::Unavailable {
                    attempts: 1,
                    last_error: format!("{server}: {error}"),
                    maybe_applied: true,
                }),
            };
        }

        Err(ClientError::Unavailable {
            attempts: 1,
            last_error,
            maybe_applied: false,
        })
    }

    fn run(&mut self, line: String, retry_ambiguous: bool) -> Result<Reply, ClientError> {
        let started = Instant::now();
        let mut last_error = "no attempt made".to_string();
        let mut server_index = self.preferred % self.servers.len();
        let mut backoff_step = 0;
        let mut maybe_applied = false;
        let mut attempts = 0;

        for attempt_number in 0..self.options.max_attempts {
            if attempt_number > 0 {
                self.stats.retries += 1;
            }

            if started.elapsed() >= self.options.deadline {
                break;
            }

            self.stats.attempts += 1;
            attempts += 1;

            let server = self.servers[server_index].clone();
            let mut redirected = false;

            match self.attempt(&server, &line) {
                Attempt::Reply(Reply::Error(message)) => match classify_error(&message) {
                    Failure::Permanent => return Err(ClientError::Rejected(message)),
                    Failure::Aborted => return Err(ClientError::Aborted(message)),
                    Failure::Unknown if !retry_ambiguous => {
                        self.stats.ambiguous_failures += 1;
                        return Err(ClientError::Ambiguous(message));
                    }
                    Failure::Unknown => {
                        self.stats.ambiguous_failures += 1;
                        maybe_applied = true;
                        last_error = message;
                    }
                    Failure::NotApplied => {
                        if let Some(next) = self.leader_hint(&message) {
                            server_index = next;
                            redirected = true;
                        } else {
                            server_index = (server_index + 1) % self.servers.len();
                        }

                        last_error = message;
                    }
                },
                Attempt::Reply(reply) => {
                    self.preferred = server_index;
                    return Ok(reply);
                }
                Attempt::Unreachable(error) => {
                    server_index = (server_index + 1) % self.servers.len();
                    last_error = error;
                }
                Attempt::NoReply(error) => {
                    self.stats.ambiguous_failures += 1;

                    if !retry_ambiguous {
                        return Err(ClientError::Ambiguous(error));
                    }

                    maybe_applied = true;

                    server_index = (server_index + 1) % self.servers.len();
                    last_error = error;
                }
            }

            if !redirected {
                let ceiling = backoff_ceiling(&self.options, backoff_step);
                backoff_step += 1;
                sleep(self.jitter(ceiling));
            }
        }

        Err(ClientError::Unavailable {
            attempts,
            last_error,
            maybe_applied,
        })
    }

    fn leader_hint(&self, message: &str) -> Option<usize> {
        let node: u64 = message
            .split("leader is node ")
            .nth(1)?
            .split(|c: char| !c.is_ascii_digit())
            .next()?
            .parse()
            .ok()?;

        let address = self.options.leader_addresses.get(&node)?;

        self.servers.iter().position(|server| server == address)
    }

    fn attempt(&mut self, server: &str, line: &str) -> Attempt {
        let frame = match encode_frame(line.as_bytes()) {
            Ok(frame) => frame,
            Err(error) => {
                return Attempt::Reply(Reply::Error(format!("invalid command: {error}")));
            }
        };

        if !self.connections.contains_key(server) {
            match self.connect(server) {
                Ok(connection) => {
                    self.connections.insert(server.to_string(), connection);
                }
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    return Attempt::Reply(Reply::Error(format!("authentication failed: {error}")));
                }
                Err(error) => return Attempt::Unreachable(format!("{server}: {error}")),
            }
        }

        let Some(connection) = self.connections.get_mut(server) else {
            return Attempt::Unreachable(format!("{server}: no connection"));
        };

        if let Err(error) = connection.stream.write_all(&frame) {
            self.connections.remove(server);
            return Attempt::Unreachable(format!("{server}: {error}"));
        }

        let result = read_reply(connection);

        match result {
            Ok(text) => Attempt::Reply(parse_reply(&text)),
            Err(error) => {
                self.connections.remove(server);
                Attempt::NoReply(format!("{server}: {error}"))
            }
        }
    }

    fn connect(&self, server: &str) -> io::Result<Connection> {
        self.connect_with(server, self.options.request_timeout)
    }

    fn connect_with(&self, server: &str, timeout: Duration) -> io::Result<Connection> {
        let address = server
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bad address"))?;

        let tcp = TcpStream::connect_timeout(&address, self.options.request_timeout)?;
        tcp.set_read_timeout(Some(timeout))?;
        tcp.set_write_timeout(Some(timeout))?;
        tcp.set_nodelay(true)?;

        let stream: Box<dyn Transport> = match &self.options.tls {
            Some(config) => {
                let name = server_name(server)?;
                let session = rustls::ClientConnection::new(Arc::clone(config), name)
                    .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;

                Box::new(rustls::StreamOwned::new(session, tcp))
            }
            None => Box::new(tcp),
        };

        let mut connection = Connection {
            stream,
            decoder: FrameDecoder::new(),
        };

        if let Some(token) = &self.options.auth_token {
            let frame = encode_frame(format!("AUTH {token}").as_bytes())?;
            connection.stream.write_all(&frame)?;

            let reply = read_reply(&mut connection)?;

            if reply != "+OK" {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, reply));
            }
        }

        Ok(connection)
    }

    fn jitter(&mut self, ceiling: Duration) -> Duration {
        let mut x = self.random;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.random = x;

        let millis = ceiling.as_millis() as u64;

        Duration::from_millis(if millis == 0 { 0 } else { x % (millis + 1) })
    }
}

fn read_reply(connection: &mut Connection) -> io::Result<String> {
    loop {
        if let Some(frame) = connection.decoder.next_frame()? {
            return String::from_utf8(frame)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "reply is not UTF-8"));
        }

        if connection.decoder.read_from(&mut connection.stream)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "server closed the connection",
            ));
        }
    }
}

fn unexpected(reply: &Reply) -> ClientError {
    ClientError::Rejected(format!("unexpected reply {reply:?}"))
}

#[cfg(test)]
mod tests;
