//! TEST_FILE loopback RTMP accept-and-discard sink (#192).
//!
//! Under the Rust pusher, a `TEST_FILE` endpoint and its rescue loop dial
//! `build_rtmp_url(TestFile, key)`, which is `rtmp://127.0.0.1:1935/live/<key>`.
//! Before #192 nothing on the delivery VPS listened there. Every push died at
//! `Session::connect`, so a TEST_FILE endpoint silently never delivered. This
//! sink makes TEST_FILE a real, credential-free target. The full producer,
//! Rust-pusher and rescue path runs against it exactly as it does against
//! YouTube. The sink accepts the publish and throws the media away.
//!
//! ## Reuses xiu, writes no RTMP code
//! - Every connection is driven by xiu's `ServerSession`. That is the same
//!   protocol stack `rs-inpoint` runs on the host, so handshake, chunking,
//!   AMF and publish all come from xiu.
//! - We own the accept loop over our own `TcpListener` (the `rs-inpoint`
//!   `run_on_listener` pattern, #148). Bind errors therefore surface, and the
//!   bind address is under our control.
//! - xiu's `StreamsHub` is NOT used. Each session gets a small hub-event
//!   responder that answers `Publish` with a frame channel it drains and
//!   counts. Two reasons, both read in `streamhub-0.2.4/src/lib.rs`:
//!   (a) its `publish()` rejects a second publisher on the same `live/<key>`
//!   with `Exists` until the stale session times out, and the delivery's
//!   rescue pushers reuse the endpoint's key;
//!   (b) its transceiver `receive_event_loop` never exits on a closed channel,
//!   so dropping a hub that still has a published stream leaves a task
//!   spinning at full CPU.
//!   With the per-connection responder, nothing outlives the connection task.
//!
//! ## Safety and lifecycle
//! - The sink binds loopback ONLY. `TestFileSink::start` refuses any other
//!   address, so it is never reachable from outside the VPS.
//! - `TestFileSinkSlot` holds at most one sink. `reconcile` starts it while
//!   the endpoint set holds a TEST_FILE endpoint and stops it when none is
//!   left. The delivery API calls it after every endpoint-set change.
//!
//! ## Known characteristic
//! xiu's `ServerSession` hard-codes a 2 s client-read timeout. A publisher
//! that sends nothing for 2 s or more is disconnected, and the pusher then
//! reconnects. YouTube tolerates a longer gap. Steady-state delivery is
//! unaffected, because per-tag real-time pacing keeps bytes flowing.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rtmp::session::server_session::ServerSession;
use serde::Serialize;
use streamhub::define::{FrameData, FrameDataReceiver, StreamHubEvent, StreamHubEventReceiver};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};
use tokio::task::{JoinHandle, JoinSet};

/// Where a TEST_FILE endpoint's Rust pusher (and its rescue loop) dials.
/// `endpoint_rtmp_url::build_rtmp_url` formats the TEST_FILE URL from this
/// constant, so the pusher and the sink cannot drift apart.
pub const TEST_FILE_SINK_ADDR: &str = "127.0.0.1:1935";

/// How often a publishing connection logs its running tag/byte totals.
const PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Upper bound for draining a session's last hub events (its `UnPublish`)
/// after it ended. The drain normally finishes at once, because the session
/// dropped every sender; the bound guarantees a connection task always ends.
const SESSION_TAIL_DRAIN: Duration = Duration::from_secs(5);

/// Backoff after a failed `accept()` (e.g. fd exhaustion). The loop does not
/// spin and keeps the sink alive, the same policy as `rs-inpoint`.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Point-in-time copy of the sink's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct SinkCountersSnapshot {
    /// TCP connections accepted since the sink started.
    pub connections_accepted: u64,
    /// Connections open right now.
    pub active_connections: u64,
    /// RTMP publishes accepted.
    pub publishes: u64,
    /// Publishers that went away (connection ended after a publish).
    pub unpublishes: u64,
    /// Audio/video/metadata messages received and discarded.
    pub tags_received: u64,
    /// Payload bytes of those messages.
    pub bytes_received: u64,
}

/// A running sink's bound address and counters (exposed on `/api/status`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SinkStatus {
    pub local_addr: SocketAddr,
    pub counters: SinkCountersSnapshot,
}

/// Whether an endpoint set (given as its `service_type` strings) needs the
/// sink: true iff one of them is exactly `TEST_FILE`. That is the same exact
/// match `rs_ffmpeg::ServiceType::from_str` uses. The pusher kind does not
/// matter, because the rescue loop always pushes with the Rust pusher.
pub fn wants_test_file_sink<'a>(service_types: impl IntoIterator<Item = &'a str>) -> bool {
    service_types.into_iter().any(|st| {
        matches!(
            st.parse::<rs_ffmpeg::ServiceType>(),
            Ok(rs_ffmpeg::ServiceType::TestFile)
        )
    })
}

#[derive(Debug, Default)]
struct Counters {
    connections_accepted: AtomicU64,
    active_connections: AtomicU64,
    publishes: AtomicU64,
    unpublishes: AtomicU64,
    tags_received: AtomicU64,
    bytes_received: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> SinkCountersSnapshot {
        SinkCountersSnapshot {
            connections_accepted: self.connections_accepted.load(Ordering::SeqCst),
            active_connections: self.active_connections.load(Ordering::SeqCst),
            publishes: self.publishes.load(Ordering::SeqCst),
            unpublishes: self.unpublishes.load(Ordering::SeqCst),
            tags_received: self.tags_received.load(Ordering::SeqCst),
            bytes_received: self.bytes_received.load(Ordering::SeqCst),
        }
    }
}

/// Counts one open connection for as long as it lives. A `Drop` guard, so an
/// aborted connection task (sink shutdown) is uncounted too.
struct ActiveConnection<'a>(&'a Counters);

impl<'a> ActiveConnection<'a> {
    fn open(counters: &'a Counters) -> Self {
        counters.active_connections.fetch_add(1, Ordering::SeqCst);
        Self(counters)
    }
}

impl Drop for ActiveConnection<'_> {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

/// A bound, accepting sink. Dropping it aborts the accept task (and with it
/// every connection task); `shutdown` does the same and logs the totals.
struct TestFileSink {
    local_addr: SocketAddr,
    counters: Arc<Counters>,
    task: JoinHandle<()>,
}

impl TestFileSink {
    /// Bind `addr` (loopback only) and start accepting.
    async fn start(addr: &str) -> io::Result<Self> {
        let requested: SocketAddr = addr.parse().map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid TEST_FILE sink address {addr:?}: {e}"),
            )
        })?;
        if !requested.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "refusing non-loopback TEST_FILE sink address {requested}: \
                     the sink must never be reachable from outside the VPS"
                ),
            ));
        }
        let listener = TcpListener::bind(requested).await?;
        let local_addr = listener.local_addr()?;
        let counters = Arc::new(Counters::default());
        let task = tokio::spawn(accept_loop(listener, Arc::clone(&counters)));
        tracing::info!(
            %local_addr,
            "test_file_sink: listening (TEST_FILE loopback RTMP accept-and-discard sink)"
        );
        Ok(Self {
            local_addr,
            counters,
            task,
        })
    }

    fn status(&self) -> SinkStatus {
        SinkStatus {
            local_addr: self.local_addr,
            counters: self.counters.snapshot(),
        }
    }

    /// Stop accepting, close every connection, release the port.
    async fn shutdown(mut self) {
        self.task.abort();
        // Wait for the task to really end so the listener is closed (the port
        // is free) before this returns. A cancelled task yields Err(..).
        let _ = (&mut self.task).await;
        let c = self.counters.snapshot();
        tracing::info!(
            local_addr = %self.local_addr,
            connections = c.connections_accepted,
            publishes = c.publishes,
            tags = c.tags_received,
            bytes = c.bytes_received,
            "test_file_sink: stopped"
        );
    }
}

impl Drop for TestFileSink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Accept connections forever, one task per connection in a `JoinSet`.
/// Aborting this task drops the set, which aborts every connection too.
async fn accept_loop(listener: TcpListener, counters: Arc<Counters>) {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    counters.connections_accepted.fetch_add(1, Ordering::SeqCst);
                    tracing::info!(%peer, "test_file_sink: accepted RTMP connection");
                    connections.spawn(serve_connection(stream, peer, Arc::clone(&counters)));
                }
                Err(e) => {
                    tracing::warn!("test_file_sink: accept error (continuing): {e}");
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
            },
            // Reap finished connection tasks so the set does not grow forever.
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(e) = joined {
                    tracing::error!("test_file_sink: connection task failed: {e}");
                }
            }
        }
    }
}

/// Drive one RTMP connection through xiu's `ServerSession`. Its hub events go
/// to `respond_to_session`, which accepts the publish and discards the media.
async fn serve_connection(stream: TcpStream, peer: SocketAddr, counters: Arc<Counters>) {
    let _active = ActiveConnection::open(&counters);
    let (hub_tx, hub_rx) = mpsc::unbounded_channel();
    // gop_num 0: nothing is ever played back from this sink, so cache no GOPs.
    let mut session = ServerSession::new(stream, hub_tx, 0, None);
    // The stream this connection published (`live/<key>`), if it did.
    let mut published: Option<String> = None;

    let session_result = {
        let responder = respond_to_session(hub_rx, &counters, &mut published, peer);
        tokio::pin!(responder);
        let result = tokio::select! {
            result = session.run() => result.map_err(|e| e.to_string()),
            // Only ends once every hub sender is gone, and the session owns
            // the only one. Kept as a branch so a broken invariant cannot hang.
            () = &mut responder => Ok(()),
        };
        // Dropping the session drops its hub sender and frame sender. The
        // responder then sees the session's last events (its UnPublish) plus
        // any queued frames, and returns.
        drop(session);
        let _ = tokio::time::timeout(SESSION_TAIL_DRAIN, responder).await;
        result
    };

    if published.is_some() {
        counters.unpublishes.fetch_add(1, Ordering::SeqCst);
    }
    let totals = counters.snapshot();
    tracing::info!(
        %peer,
        stream = published.as_deref().unwrap_or("-"),
        sink_tags = totals.tags_received,
        sink_bytes = totals.bytes_received,
        end = %session_result.err().unwrap_or_else(|| "clean".to_string()),
        "test_file_sink: RTMP connection closed"
    );
}

/// Answer the hub events xiu's `ServerSession` sends, and count the media.
/// Returns once the session dropped its hub sender (i.e. it ended).
async fn respond_to_session(
    mut hub_rx: StreamHubEventReceiver,
    counters: &Counters,
    published: &mut Option<String>,
    peer: SocketAddr,
) {
    let mut frames: Option<FrameDataReceiver> = None;
    let mut progress = tokio::time::interval_at(
        tokio::time::Instant::now() + PROGRESS_LOG_INTERVAL,
        PROGRESS_LOG_INTERVAL,
    );
    loop {
        tokio::select! {
            event = hub_rx.recv() => match event {
                Some(event) => on_hub_event(event, &mut frames, counters, published, peer),
                None => {
                    // Session gone: its frame sender is gone too, so whatever
                    // is still queued can be drained without waiting.
                    if let Some(rx) = frames.as_mut() {
                        while let Ok(frame) = rx.try_recv() {
                            count_frame(&frame, counters);
                        }
                    }
                    return;
                }
            },
            frame = next_frame(&mut frames) => match frame {
                Some(frame) => count_frame(&frame, counters),
                None => frames = None,
            },
            _ = progress.tick() => {
                if let Some(stream) = published.as_deref() {
                    let totals = counters.snapshot();
                    tracing::info!(
                        %peer,
                        stream,
                        sink_tags = totals.tags_received,
                        sink_bytes = totals.bytes_received,
                        "test_file_sink: publishing (media discarded)"
                    );
                }
            }
        }
    }
}

/// The next frame of the current publish, or never while nothing publishes.
async fn next_frame(frames: &mut Option<FrameDataReceiver>) -> Option<FrameData> {
    match frames {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

fn on_hub_event(
    event: StreamHubEvent,
    frames: &mut Option<FrameDataReceiver>,
    counters: &Counters,
    published: &mut Option<String>,
    peer: SocketAddr,
) {
    match event {
        StreamHubEvent::Publish {
            identifier,
            result_sender,
            ..
        } => {
            let (frame_tx, frame_rx) = mpsc::unbounded_channel();
            // Frames only: no packet sender, and no statistics sender (xiu
            // treats a `None` statistics sender as "no statistics").
            if result_sender
                .send(Ok((Some(frame_tx), None, None)))
                .is_err()
            {
                tracing::warn!(%peer, stream = %identifier, "test_file_sink: publisher gone before its publish was accepted");
                return;
            }
            *frames = Some(frame_rx);
            *published = Some(identifier.to_string());
            counters.publishes.fetch_add(1, Ordering::SeqCst);
            tracing::info!(%peer, stream = %identifier, "test_file_sink: publish accepted (media is discarded)");
        }
        // UnPublish (the publisher left) and anything else. A `Subscribe`
        // (a player) is refused by dropping its result sender with the event:
        // this sink only accepts publishers.
        other => tracing::info!(
            %peer,
            event = %serde_json::to_string(&other).unwrap_or_else(|_| "<non-serializable>".to_string()),
            "test_file_sink: hub event (only publish is served)"
        ),
    }
}

fn count_frame(frame: &FrameData, counters: &Counters) {
    let len = match frame {
        FrameData::Video { data, .. }
        | FrameData::Audio { data, .. }
        | FrameData::MetaData { data, .. } => data.len() as u64,
        FrameData::MediaInfo { .. } => return,
    };
    counters.tags_received.fetch_add(1, Ordering::SeqCst);
    counters.bytes_received.fetch_add(len, Ordering::SeqCst);
}

/// Owner of the (at most one) running sink. Lives in the delivery `AppState`.
pub struct TestFileSinkSlot {
    addr: String,
    running: Mutex<Option<TestFileSink>>,
}

impl TestFileSinkSlot {
    /// A slot that binds `addr` when a sink is wanted (tests: `127.0.0.1:0`).
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            running: Mutex::new(None),
        }
    }

    /// The production slot: binds `TEST_FILE_SINK_ADDR`.
    pub fn production() -> Self {
        Self::new(TEST_FILE_SINK_ADDR)
    }

    /// Start the sink if `wanted` resolves true and none runs; stop it if it
    /// resolves false and one runs. `wanted` is awaited while the slot lock is
    /// held, so concurrent reconciles serialize and converge on the latest
    /// endpoint set. A failed start is logged and retried on the next call.
    pub async fn reconcile(&self, wanted: impl Future<Output = bool>) {
        let mut running = self.running.lock().await;
        let wanted = wanted.await;
        match (wanted, running.is_some()) {
            (true, false) => match TestFileSink::start(&self.addr).await {
                Ok(sink) => *running = Some(sink),
                Err(e) => tracing::error!(
                    addr = %self.addr,
                    "test_file_sink: cannot start -- TEST_FILE pushes will be refused until the next endpoint change: {e}"
                ),
            },
            (false, true) => {
                if let Some(sink) = running.take() {
                    sink.shutdown().await;
                }
            }
            _ => {}
        }
    }

    /// The running sink's address + counters, or `None` while none runs.
    pub async fn status(&self) -> Option<SinkStatus> {
        self.running.lock().await.as_ref().map(TestFileSink::status)
    }
}

#[cfg(test)]
#[path = "test_file_sink_tests.rs"]
mod tests;
