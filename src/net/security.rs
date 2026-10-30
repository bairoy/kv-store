//! Authentication and transport encryption for client and peer connections.

use std::fmt;
use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::net::framing::{FrameDecoder, encode_frame};
use crate::raft::server::read_frame;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const FAILED_AUTH_DELAY: Duration = Duration::from_millis(250);

pub const NOAUTH: &str = "NOAUTH authentication required";
pub const BAD_TOKEN: &str = "NOAUTH invalid token";

pub enum Stream {
    Plain(TcpStream),
    Server(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    Client(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Stream::Server(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
            Stream::Client(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

pub struct Security {
    token: Option<String>,
    acceptor: Option<TlsAcceptor>,
    connector: Option<TlsConnector>,
}

impl fmt::Debug for Security {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Security")
            .field("token", &self.token.as_ref().map(|_| "<set>"))
            .field("tls", &self.acceptor.is_some())
            .finish()
    }
}

pub struct TlsFiles<'a> {
    pub cert: &'a Path,
    pub key: &'a Path,
    pub ca: &'a Path,
}

impl Security {
    pub fn plain() -> Arc<Self> {
        Arc::new(Self {
            token: None,
            acceptor: None,
            connector: None,
        })
    }

    pub fn new(token: Option<String>, tls: Option<TlsFiles<'_>>) -> io::Result<Arc<Self>> {
        let token = token.filter(|token| !token.is_empty());

        let (acceptor, connector) = match tls {
            Some(files) => (
                Some(TlsAcceptor::from(server_config(files.cert, files.key)?)),
                Some(TlsConnector::from(client_config(files.ca)?)),
            ),
            None => (None, None),
        };

        Ok(Arc::new(Self {
            token,
            acceptor,
            connector,
        }))
    }

    pub fn from_env() -> io::Result<Arc<Self>> {
        let token = match std::env::var("KV_AUTH_TOKEN_FILE") {
            Ok(path) => Some(std::fs::read_to_string(path)?.trim().to_string()),
            Err(_) => std::env::var("KV_AUTH_TOKEN").ok(),
        };

        let cert = std::env::var("KV_TLS_CERT").ok();
        let key = std::env::var("KV_TLS_KEY").ok();
        let ca = std::env::var("KV_TLS_CA").ok();

        let tls = match (&cert, &key, &ca) {
            (Some(cert), Some(key), Some(ca)) => Some(TlsFiles {
                cert: Path::new(cert),
                key: Path::new(key),
                ca: Path::new(ca),
            }),
            (None, None, None) => None,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "KV_TLS_CERT, KV_TLS_KEY and KV_TLS_CA must be set together",
                ));
            }
        };

        Self::new(token, tls)
    }

    pub fn requires_token(&self) -> bool {
        self.token.is_some()
    }

    pub fn tls_enabled(&self) -> bool {
        self.acceptor.is_some()
    }

    pub fn auth_line(&self) -> Option<String> {
        self.token.as_ref().map(|token| format!("AUTH {token}"))
    }

    fn token_matches(&self, given: &str) -> bool {
        match &self.token {
            Some(token) => constant_time_eq(token.as_bytes(), given.as_bytes()),
            None => true,
        }
    }

    pub async fn accept(&self, tcp: TcpStream) -> io::Result<Stream> {
        match &self.acceptor {
            Some(acceptor) => {
                let stream = timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
                    .await
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")
                    })??;

                Ok(Stream::Server(Box::new(stream)))
            }
            None => Ok(Stream::Plain(tcp)),
        }
    }

    pub async fn connect(&self, address: &str) -> io::Result<Stream> {
        let tcp = TcpStream::connect(address).await?;

        match &self.connector {
            Some(connector) => {
                let name = server_name(address)?;
                let stream = connector.connect(name, tcp).await?;

                Ok(Stream::Client(Box::new(stream)))
            }
            None => Ok(Stream::Plain(tcp)),
        }
    }

    pub async fn connect_client(&self, address: &str) -> io::Result<Stream> {
        let mut stream = self.connect(address).await?;

        if let Some(line) = self.auth_line() {
            stream.write_all(&encode_frame(line.as_bytes())?).await?;

            let mut decoder = FrameDecoder::new();
            let reply = read_frame(&mut stream, &mut decoder)
                .await?
                .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))?;

            if reply != b"+OK" {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    String::from_utf8_lossy(&reply).into_owned(),
                ));
            }
        }

        Ok(stream)
    }

    pub async fn client_gate(&self, authenticated: &mut bool, request: &str) -> Option<String> {
        if let Some(given) = request.strip_prefix("AUTH ") {
            if self.token_matches(given.trim_end()) {
                *authenticated = true;
                return Some("+OK".to_string());
            }

            sleep(FAILED_AUTH_DELAY).await;
            return Some(format!("-ERR {BAD_TOKEN}"));
        }

        if self.requires_token() && !*authenticated {
            sleep(FAILED_AUTH_DELAY).await;
            return Some(format!("-ERR {NOAUTH}"));
        }

        None
    }

    pub fn peer_gate(&self, authenticated: &mut bool, payload: &[u8]) -> io::Result<bool> {
        if payload.starts_with(b"AUTH ") {
            if self.peer_frame_is_valid_auth(payload) {
                *authenticated = true;
                return Ok(true);
            }

            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "invalid peer token",
            ));
        }

        if self.requires_token() && !*authenticated {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "peer did not authenticate",
            ));
        }

        Ok(false)
    }

    pub fn peer_frame_is_valid_auth(&self, payload: &[u8]) -> bool {
        payload
            .strip_prefix(b"AUTH ")
            .and_then(|given| std::str::from_utf8(given).ok())
            .is_some_and(|given| self.token_matches(given))
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn read_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_file_iter(path)
        .map_err(|error| invalid(format!("{}: {error}", path.display())))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| invalid(format!("{}: {error}", path.display())))
}

pub fn server_config(cert: &Path, key: &Path) -> io::Result<Arc<ServerConfig>> {
    let certs = read_certs(cert)?;
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|error| invalid(format!("{}: {error}", key.display())))?;

    let config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| invalid(error.to_string()))?;

    Ok(Arc::new(config))
}

pub fn client_config(ca: &Path) -> io::Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();

    for cert in read_certs(ca)? {
        roots
            .add(cert)
            .map_err(|error| invalid(format!("{}: {error}", ca.display())))?;
    }

    let config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|error| invalid(error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(Arc::new(config))
}

pub fn server_name(address: &str) -> io::Result<ServerName<'static>> {
    let host = address
        .rsplit_once(':')
        .map_or(address, |(host, _)| host)
        .trim_start_matches('[')
        .trim_end_matches(']');

    match host.parse::<IpAddr>() {
        Ok(ip) => Ok(ServerName::IpAddress(ip.into())),
        Err(_) => ServerName::try_from(host.to_string())
            .map_err(|_| invalid(format!("{host:?} is not a valid server name"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn tokened(token: &str) -> Arc<Security> {
        Security::new(Some(token.to_string()), None).unwrap()
    }

    #[test]
    fn tokens_are_compared_in_full() {
        let security = tokened("s3cret");

        assert!(security.token_matches("s3cret"));
        assert!(!security.token_matches("s3cre"));
        assert!(!security.token_matches("s3crett"));
        assert!(!security.token_matches(""));
        assert!(Security::plain().token_matches("anything"));
    }

    #[test]
    fn an_empty_token_means_no_authentication() {
        let security = Security::new(Some(String::new()), None).unwrap();

        assert!(!security.requires_token());
        assert_eq!(security.auth_line(), None);
    }

    #[test]
    fn the_debug_output_never_shows_the_token() {
        let text = format!("{:?}", tokened("hunter2"));

        assert!(!text.contains("hunter2"));
    }

    #[tokio::test]
    async fn the_client_port_refuses_everything_until_auth_succeeds() {
        let security = tokened("s3cret");
        let mut authenticated = false;

        let reply = security.client_gate(&mut authenticated, "GET a").await;
        assert_eq!(reply, Some(format!("-ERR {NOAUTH}")));
        assert!(!authenticated);

        let reply = security.client_gate(&mut authenticated, "AUTH wrong").await;
        assert_eq!(reply, Some(format!("-ERR {BAD_TOKEN}")));
        assert!(!authenticated);

        let reply = security
            .client_gate(&mut authenticated, "AUTH s3cret")
            .await;
        assert_eq!(reply, Some("+OK".to_string()));
        assert!(authenticated);

        assert_eq!(
            security.client_gate(&mut authenticated, "GET a").await,
            None
        );
    }

    #[tokio::test]
    async fn an_open_server_lets_everything_through_and_accepts_auth() {
        let security = Security::plain();
        let mut authenticated = false;

        assert_eq!(
            security.client_gate(&mut authenticated, "GET a").await,
            None
        );
        assert_eq!(
            security
                .client_gate(&mut authenticated, "AUTH whatever")
                .await,
            Some("+OK".to_string())
        );
    }

    #[test]
    fn the_peer_auth_frame_is_checked() {
        let security = tokened("s3cret");

        assert!(security.peer_frame_is_valid_auth(b"AUTH s3cret"));
        assert!(!security.peer_frame_is_valid_auth(b"AUTH nope"));
        assert!(!security.peer_frame_is_valid_auth(b"\x00\x00\x00\x01rest"));
        assert!(!security.peer_frame_is_valid_auth(b"AUTH "));
    }

    #[test]
    fn server_names_come_from_the_host_part() {
        assert!(matches!(
            server_name("127.0.0.1:7201").unwrap(),
            ServerName::IpAddress(_)
        ));
        assert!(matches!(
            server_name("node1:7200").unwrap(),
            ServerName::DnsName(_)
        ));
        assert!(matches!(
            server_name("[::1]:7200").unwrap(),
            ServerName::IpAddress(_)
        ));
    }

    #[test]
    fn missing_or_bad_tls_files_are_reported() {
        assert!(
            server_config(Path::new("/nonexistent.pem"), Path::new("/nonexistent.key")).is_err()
        );
        assert!(client_config(Path::new("/nonexistent.pem")).is_err());
    }

    fn make_certs() -> Option<std::path::PathBuf> {
        let dir = std::env::temp_dir().join(format!(
            "kv_store_tls_tests_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));

        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/gen-certs.sh");
        let status = Command::new("sh")
            .arg(script)
            .arg(&dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .ok()?;

        status.success().then_some(dir)
    }

    fn tls_security(dir: &Path, token: Option<&str>) -> Arc<Security> {
        Security::new(
            token.map(str::to_string),
            Some(TlsFiles {
                cert: &dir.join("server.pem"),
                key: &dir.join("server.key"),
                ca: &dir.join("ca.pem"),
            }),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn traffic_is_encrypted_and_authenticated_end_to_end() {
        let Some(dir) = make_certs() else {
            eprintln!("openssl not available: skipping the TLS test");
            return;
        };

        let security = tls_security(&dir, Some("s3cret"));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let server_security = Arc::clone(&security);
        tokio::spawn(async move {
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let security = Arc::clone(&server_security);

                tokio::spawn(async move {
                    let Ok(mut stream) = security.accept(tcp).await else {
                        return;
                    };
                    let mut decoder = FrameDecoder::new();
                    let mut authenticated = false;

                    while let Ok(Some(frame)) = read_frame(&mut stream, &mut decoder).await {
                        let request = String::from_utf8_lossy(&frame).into_owned();

                        let reply = match security.client_gate(&mut authenticated, &request).await {
                            Some(reply) => reply,
                            None => format!("+echo {request}"),
                        };

                        let _ = stream
                            .write_all(&encode_frame(reply.as_bytes()).unwrap())
                            .await;
                    }
                });
            }
        });

        let mut stream = security.connect_client(&address).await.unwrap();
        stream
            .write_all(&encode_frame(b"hello").unwrap())
            .await
            .unwrap();

        let mut decoder = FrameDecoder::new();
        let reply = read_frame(&mut stream, &mut decoder)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reply, b"+echo hello");

        let wrong = tls_security(&dir, Some("wrong"));
        let error = wrong.connect_client(&address).await.err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);

        let mut tcp = TcpStream::connect(&address).await.unwrap();
        tcp.write_all(&encode_frame(b"AUTH s3cret").unwrap())
            .await
            .unwrap();
        let mut buffer = [0u8; 64];
        let read = timeout(Duration::from_secs(2), tcp.read(&mut buffer)).await;
        assert!(
            !matches!(read, Ok(Ok(count)) if buffer[..count].windows(3).any(|w| w == b"+OK")),
            "a plaintext client must not get a normal reply"
        );

        let other_dir = make_certs().unwrap();
        let stranger = tls_security(&other_dir, Some("s3cret"));
        assert!(stranger.connect_client(&address).await.is_err());

        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(other_dir);
    }
}
