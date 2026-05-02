//! TLS-wrapped RTMP I/O for rtmps:// endpoints (Facebook / Vimeo / Instagram).
//!
//! Implements `bytesio::TNetIO` over `Framed<TlsStream<TcpStream>, BytesCodec>`
//! so the existing negotiate / read-loop / packetizer machinery in `session.rs`
//! is transparent to the wire encryption.
//!
//! Production code consumes `tls_client_config()` which is built once per
//! process from `webpki-roots`. Tests inject their own self-signed CA via
//! `testing::set_tls_client_config_for_tests` (called once per test binary).
//!
//! See `docs/superpowers/specs/2026-05-01-rs-rtmp-push-rtmps-tls-design.md`.

use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use bytesio::bytesio::{NetType, TNetIO};
use bytesio::bytesio_errors::{BytesIOError, BytesIOErrorValue};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_util::codec::{BytesCodec, Framed};

use crate::PushError;

type TlsClientStream = tokio_rustls::client::TlsStream<TcpStream>;

/// `TNetIO` implementation over a `tokio_rustls` client TLS stream.
/// Mirrors `bytesio::TcpIO` exactly except for the underlying transport.
pub struct TlsIO {
    stream: Framed<TlsClientStream, BytesCodec>,
}

impl TlsIO {
    pub fn new(stream: TlsClientStream) -> Self {
        Self {
            stream: Framed::new(stream, BytesCodec::new()),
        }
    }
}

#[async_trait]
impl TNetIO for TlsIO {
    fn get_net_type(&self) -> NetType {
        // bytesio has no TLS variant; TCP is the closest semantic peer
        // (the underlying transport is still a TCP stream — the TLS layer
        // is purely cryptographic and transparent to RTMP framing).
        NetType::TCP
    }

    async fn write(&mut self, bytes: Bytes) -> Result<(), BytesIOError> {
        self.stream.send(bytes).await?;
        Ok(())
    }

    async fn read_timeout(&mut self, duration: Duration) -> Result<BytesMut, BytesIOError> {
        match tokio::time::timeout(duration, self.read()).await {
            Ok(d) => d,
            Err(err) => Err(BytesIOError {
                value: BytesIOErrorValue::TimeoutError(err),
            }),
        }
    }

    async fn read(&mut self) -> Result<BytesMut, BytesIOError> {
        match self.stream.next().await {
            Some(Ok(b)) => Ok(b),
            Some(Err(err)) => Err(BytesIOError {
                value: BytesIOErrorValue::IOError(err),
            }),
            None => Err(BytesIOError {
                value: BytesIOErrorValue::NoneReturn,
            }),
        }
    }
}

// -------------------------------------------------------------------------
// Client config (production = webpki-roots; tests = override)
// -------------------------------------------------------------------------

static TLS_CONFIG_OVERRIDE: OnceLock<Arc<ClientConfig>> = OnceLock::new();
static TLS_CONFIG_DEFAULT: OnceLock<Arc<ClientConfig>> = OnceLock::new();

/// Returns the rustls `ClientConfig` to use for outbound rtmps:// connections.
/// First call wins; subsequent calls return the cached `Arc`.
pub fn tls_client_config() -> Arc<ClientConfig> {
    if let Some(o) = TLS_CONFIG_OVERRIDE.get() {
        return o.clone();
    }
    TLS_CONFIG_DEFAULT.get_or_init(build_default_config).clone()
}

/// Install rustls's `ring` crypto provider as the process-default. Idempotent:
/// subsequent calls (or calls from other code paths) silently no-op. rustls
/// 0.23 panics on `ClientConfig::builder()` when no provider is installed.
pub(crate) fn ensure_default_crypto_provider() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // `install_default` returns Err if a provider is already installed
        // (e.g., another test already called us). Either outcome is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn build_default_config() -> Arc<ClientConfig> {
    ensure_default_crypto_provider();
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(cfg)
}

// -------------------------------------------------------------------------
// Connect helper
// -------------------------------------------------------------------------

/// Wrap an already-connected `TcpStream` in a TLS client stream, performing
/// the rustls handshake. Returns a `TlsIO` ready for the RTMP negotiate
/// sequence. SNI is taken from `host`.
pub async fn connect_tls(tcp: TcpStream, host: &str) -> Result<TlsIO, PushError> {
    let server_name = ServerName::try_from(host.to_string())
        .map_err(|e| PushError::TlsHandshakeFailed(format!("invalid SNI '{host}': {e}")))?;
    let connector = TlsConnector::from(tls_client_config());
    let tls = connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| PushError::TlsHandshakeFailed(e.to_string()))?;
    Ok(TlsIO::new(tls))
}

// -------------------------------------------------------------------------
// Test override
// -------------------------------------------------------------------------

/// Test-only entry points. `#[doc(hidden)]` so it does not appear in public
/// rustdoc; production code never calls these.
#[doc(hidden)]
pub mod testing {
    use super::*;

    /// Install a custom rustls `ClientConfig` (e.g. a `RootCertStore` containing
    /// only a self-signed test CA). Must be called once per test binary BEFORE
    /// any rtmps:// connect attempt. Subsequent calls in the same process are
    /// silently ignored.
    pub fn set_tls_client_config_for_tests(cfg: Arc<ClientConfig>) {
        super::ensure_default_crypto_provider();
        // First-call-wins enforced by panic, not silent ignore: if a test binary
        // grows multiple `#[tokio::test]` functions that each call this with
        // different configs (e.g. different self-signed certs), the second
        // caller's cert will NOT be in the trust store and the connect would
        // silently fail with `UnknownIssuer`. Panic loudly so the test author
        // sees the problem instead of debugging a phantom handshake error.
        if TLS_CONFIG_OVERRIDE.set(cfg).is_err() {
            panic!(
                "set_tls_client_config_for_tests called twice in the same test \
                 binary; integration tests are not isolated. Either factor the \
                 conflicting tests into separate test files, or share one config."
            );
        }
    }

    /// Idempotently install rustls's `ring` crypto provider. Tests that build
    /// their own `ServerConfig` / `ClientConfig` (e.g. the TLS bridge harness)
    /// call this before touching any rustls Builder so they don't panic on a
    /// missing process-level provider.
    pub fn ensure_default_crypto_provider() {
        super::ensure_default_crypto_provider();
    }
}
