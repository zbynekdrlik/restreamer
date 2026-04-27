//! xiu `ClientSession` adapter.
//!
//! `Session::connect` performs the TCP dial + RTMP handshake/connect/publish
//! sequence using xiu's own `ClientSession` state machine.  Media-tag writes
//! (Task 6) are stubs that return `Ok(())` for now.

use std::io;
use std::time::Duration;

use rtmp::session::client_session::{ClientSession, ClientSessionType};
use streamhub::StreamsHub;
use tokio::net::TcpStream;

use crate::PushError;

// -------------------------------------------------------------------------
// Session
// -------------------------------------------------------------------------

/// An active RTMP push session backed by an xiu `ClientSession`.
///
/// The session runs the RTMP state machine (handshake -> connect ->
/// createStream -> publish) on a spawned task.  `send_audio_tag` /
/// `send_video_tag` are stubs until Task 6 wires FrameData sends.
pub struct Session {
    /// Background task running `ClientSession::run`.
    session_handle: tokio::task::JoinHandle<Result<(), rtmp::session::errors::SessionError>>,
    /// Background task keeping the `StreamsHub` event loop alive.
    _hub_handle: tokio::task::JoinHandle<()>,
}

impl Session {
    /// Dial `url`, perform RTMP handshake + connect + publish, and return a
    /// live `Session`.
    ///
    /// `url` must be of the form `rtmp://host[:port]/app/stream`.
    /// `timeout_ms` is applied to the TCP connect step only.
    pub async fn connect(url: &str, timeout_ms: u64) -> Result<Self, PushError> {
        // --- 1. Parse URL ---------------------------------------------------
        let (host, port, app, stream_name) = parse_rtmp_url(url)?;

        // --- 2. TCP connect -------------------------------------------------
        let addr = format!("{host}:{port}");
        let stream =
            tokio::time::timeout(Duration::from_millis(timeout_ms), TcpStream::connect(&addr))
                .await
                .map_err(|_| PushError::Timeout)?
                .map_err(PushError::HandshakeFailed)?;

        // --- 3. StreamsHub (event bus required by ClientSession) -------------
        let mut hub = StreamsHub::new(None);
        // For the push client we do NOT need rtmp_push_enabled - that flag
        // makes the hub emit BroadcastEvent::Publish to relay clients.  We
        // are already the push client; the hub is only needed to satisfy the
        // ClientSession constructor.
        let event_sender = hub.get_hub_event_sender();

        let hub_handle = tokio::spawn(async move {
            hub.run().await;
        });

        // --- 4. ClientSession -----------------------------------------------
        let mut client_session = ClientSession::new(
            stream,
            ClientSessionType::Push,
            addr, // raw_domain_name (host:port per xiu convention)
            app,
            stream_name,
            event_sender,
            0, // gop_num - no GOP cache for push
        );

        let session_handle = tokio::spawn(async move { client_session.run().await });

        // --- 5. Wait for handshake/connect/publish to complete --------------
        // 500 ms is intentionally coarse - Task 10 replaces this with a
        // proper AMF onStatus signal.  On a loopback TCP connection the full
        // RTMP handshake + connect + createStream + publish completes well
        // under 100 ms.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // --- 6. Check for early session failure -----------------------------
        if session_handle.is_finished() {
            // Retrieve the error from the completed task.
            let err_msg = match session_handle.await {
                Ok(Ok(())) => "session exited cleanly before media flow".to_string(),
                Ok(Err(e)) => e.to_string(),
                Err(join_err) => join_err.to_string(),
            };

            hub_handle.abort();

            // Classify: "handshake" in the message -> HandshakeFailed.
            let io_err = io::Error::other(err_msg.clone());
            if err_msg.to_lowercase().contains("handshake") {
                return Err(PushError::HandshakeFailed(io_err));
            }
            return Err(PushError::IoError(io_err));
        }

        Ok(Self {
            session_handle,
            _hub_handle: hub_handle,
        })
    }

    /// Send an audio FLV tag body.  Stub for Task 4 - FrameData send is Task 6.
    pub async fn send_audio_tag(
        &mut self,
        _timestamp_ms: u32,
        _body: &[u8],
    ) -> Result<(), PushError> {
        if self.session_handle.is_finished() {
            return Err(PushError::RemoteClosed(io::Error::from(
                io::ErrorKind::ConnectionReset,
            )));
        }
        Ok(())
    }

    /// Send a video FLV tag body.  Stub for Task 4 - FrameData send is Task 6.
    pub async fn send_video_tag(
        &mut self,
        _timestamp_ms: u32,
        _body: &[u8],
    ) -> Result<(), PushError> {
        if self.session_handle.is_finished() {
            return Err(PushError::RemoteClosed(io::Error::from(
                io::ErrorKind::ConnectionReset,
            )));
        }
        Ok(())
    }

    /// Abort the session and hub tasks and drop the session.
    pub async fn close(self) {
        self.session_handle.abort();
        self._hub_handle.abort();
    }
}

// -------------------------------------------------------------------------
// URL parsing
// -------------------------------------------------------------------------

/// Parse `rtmp://host[:port]/app/stream` into `(host, port, app, stream)`.
///
/// - Default port is 1935.
/// - Both `app` and `stream` must be non-empty; otherwise returns `IoError`.
fn parse_rtmp_url(url: &str) -> Result<(String, u16, String, String), PushError> {
    let rest = url
        .strip_prefix("rtmp://")
        .ok_or_else(|| bad_url("must start with rtmp://", url))?;

    // Split host[:port] from /app/stream
    let slash = rest
        .find('/')
        .ok_or_else(|| bad_url("missing /app/stream path", url))?;

    let authority = &rest[..slash];
    let path = &rest[slash + 1..]; // strip leading '/'

    // Parse host and optional port
    let (host, port) = if let Some(colon) = authority.rfind(':') {
        let h = &authority[..colon];
        let p: u16 = authority[colon + 1..]
            .parse()
            .map_err(|_| bad_url("invalid port number", url))?;
        (h.to_string(), p)
    } else {
        (authority.to_string(), 1935u16)
    };

    if host.is_empty() {
        return Err(bad_url("host is empty", url));
    }

    // Split path into app/stream (take first '/' as separator)
    let slash2 = path
        .find('/')
        .ok_or_else(|| bad_url("path must contain /app/stream (two components)", url))?;

    let app = path[..slash2].to_string();
    let stream = path[slash2 + 1..].to_string();

    if app.is_empty() {
        return Err(bad_url("app name is empty", url));
    }
    if stream.is_empty() {
        return Err(bad_url("stream name is empty", url));
    }

    Ok((host, port, app, stream))
}

fn bad_url(reason: &str, url: &str) -> PushError {
    PushError::IoError(io::Error::other(format!("bad RTMP URL ({reason}): {url}")))
}

// -------------------------------------------------------------------------
// Unit tests for the URL parser
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::parse_rtmp_url;

    #[test]
    fn parse_standard_url() {
        let (host, port, app, stream) = parse_rtmp_url("rtmp://a.example.com/live/test").unwrap();
        assert_eq!(host, "a.example.com");
        assert_eq!(port, 1935);
        assert_eq!(app, "live");
        assert_eq!(stream, "test");
    }

    #[test]
    fn parse_url_with_port() {
        let (host, port, app, stream) =
            parse_rtmp_url("rtmp://127.0.0.1:19350/live/mykey").unwrap();
        assert_eq!(host, "127.0.0.1");
        assert_eq!(port, 19350);
        assert_eq!(app, "live");
        assert_eq!(stream, "mykey");
    }

    #[test]
    fn rejects_non_rtmp_scheme() {
        assert!(parse_rtmp_url("http://host/live/test").is_err());
    }

    #[test]
    fn rejects_missing_stream() {
        assert!(parse_rtmp_url("rtmp://host/live").is_err());
    }

    #[test]
    fn rejects_empty_app() {
        assert!(parse_rtmp_url("rtmp://host//stream").is_err());
    }
}
