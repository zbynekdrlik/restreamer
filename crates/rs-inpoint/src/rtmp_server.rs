use std::sync::Arc;

use rtmp::session::server_session::ServerSession;
use streamhub::StreamsHub;
use streamhub::define::StreamHubEventSender;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{error, info};

use rs_core::models::InpointState;

use crate::flv_chunker::FlvChunkSink;
use crate::media_receiver::MediaReceiver;

/// Most frames a local player (VLC, a headphone probe) may have queued
/// before its play session is ended (#368): ~18 s at 4K60 (~107 frames/s
/// with audio), ~100-200 MB at 40 Mbit/s. The hub queues the whole cached
/// GOP at subscribe, so this stays above a GOP of up to ~18 s. Unbounded, a
/// player slower than real time grew xiu's queue to 8 GB on stream.lan. A
/// player that stops reading entirely is cut by xiu's write stall instead
/// (vendor/rtmp `PLAY_WRITE_STALL`, 10 s).
pub const PLAY_QUEUE_LIMIT_FRAMES: usize = 2000;

/// RTMP server that accepts connections from OBS/vMix on a configurable port.
///
/// Uses the xiu RTMP implementation for proper protocol handling including
/// full handshake, AMF command parsing, and H.264/AAC media extraction.
/// Media data flows through the StreamsHub to the MediaReceiver which
/// subscribes to the published stream and feeds the FlvChunkSink.
pub struct RtmpServer {
    address: String,
    shutdown_tx: broadcast::Sender<()>,
    /// Subscribed in `new`, so a shutdown sent before `serve` starts (the
    /// server task runs on the ingest runtime, after its bind) is buffered,
    /// never dropped for lack of a receiver (#368).
    shutdown_rx: broadcast::Receiver<()>,
    /// Set on every accepted session: see [`PLAY_QUEUE_LIMIT_FRAMES`].
    play_queue_limit: usize,
}

impl RtmpServer {
    pub fn new(bind: &str, port: u16) -> Self {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        Self {
            address: format!("{bind}:{port}"),
            shutdown_tx,
            shutdown_rx,
            play_queue_limit: PLAY_QUEUE_LIMIT_FRAMES,
        }
    }

    /// Overrides [`PLAY_QUEUE_LIMIT_FRAMES`] (tests use a small one).
    pub fn with_play_queue_limit(mut self, frames: usize) -> Self {
        self.play_queue_limit = frames;
        self
    }

    /// Returns a shutdown handle that can be used to stop the server.
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Run the RTMP server, binding the configured address and accepting
    /// connections until shutdown.
    pub async fn run(
        self,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
    ) -> Result<(), crate::InpointError> {
        // Bind here (instead of letting xiu bind by address string) so a bind
        // failure propagates as a real InpointError instead of being swallowed
        // into a log line, and so tests can hand us a pre-bound listener.
        let listener = TcpListener::bind(&self.address).await?;
        self.serve(listener, flv_chunk_sink, inpoint_state).await
    }

    /// Run the RTMP server on an ALREADY-BOUND listener, accepting connections
    /// until shutdown.
    ///
    /// This exists so a caller (notably the E2E tests) can reserve a free port
    /// by binding `127.0.0.1:0` and hand that exact listener over — the socket
    /// that reserved the port is the socket that accepts, so there is no
    /// pick-then-release-then-bind (TOCTOU) window that could race another
    /// binder onto the same port (see #148).
    pub async fn run_on_listener(
        self,
        listener: TcpListener,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
    ) -> Result<(), crate::InpointError> {
        self.serve(listener, flv_chunk_sink, inpoint_state).await
    }

    /// Shared server body: wire up the StreamsHub + MediaReceiver and run the
    /// accept loop on `listener` until shutdown. Used by both `run` (production,
    /// binds by address) and `run_on_listener` (tests, pre-bound listener).
    async fn serve(
        self,
        listener: TcpListener,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
    ) -> Result<(), crate::InpointError> {
        // Create the StreamsHub for media data routing
        let mut hub = StreamsHub::new(None);

        // Enable push so that BroadcastEvent::Publish is emitted to our
        // MediaReceiver when an RTMP publisher connects.
        hub.set_rtmp_push_enabled(true);

        let event_sender = hub.get_hub_event_sender();
        let event_consumer = hub.get_client_event_consumer();

        // Create media receiver that subscribes to published streams and
        // processes frame data into FLV chunks
        let media_receiver = MediaReceiver::new(
            event_consumer,
            event_sender.clone(),
            Arc::clone(&flv_chunk_sink),
            inpoint_state,
        );

        match listener.local_addr() {
            Ok(addr) => info!("RTMP server accepting on {addr}"),
            Err(_) => info!("RTMP server accepting on {}", self.address),
        }

        let mut shutdown_rx = self.shutdown_rx;

        // #106: capture the run outcome instead of swallowing it. A xiu bind
        // failure (e.g. a process grabbed the port in the TOCTOU window between
        // the runtime's pre-bind probe and this bind) used to be logged and
        // then dropped as `Ok(())`, which the orchestrator read as a clean stop
        // and gave up permanently — the exact silent-death this hardening
        // exists to kill. Propagate it so `run_inpoint_loop` restarts + the
        // next pre-bind probe surfaces the conflict on the dashboard.
        let run_result: Result<(), crate::InpointError> = tokio::select! {
            // Run the StreamsHub event loop. It only returns if its event
            // channel closed, i.e. the ingest is dead: surface that as a
            // failure so the orchestrator restarts the server (#367).
            _ = hub.run() => {
                error!("StreamsHub stopped unexpectedly -- ingest cannot continue");
                Err(crate::InpointError::Protocol(
                    "StreamsHub event loop stopped".to_string(),
                ))
            }
            // Run the RTMP accept loop (uses xiu's ServerSession per connection)
            result = Self::accept_loop(&listener, event_sender, self.play_queue_limit) => {
                match result {
                    Ok(()) => {
                        info!("RTMP server stopped");
                        Ok(())
                    }
                    Err(e) => {
                        error!("RTMP server error: {e}");
                        Err(crate::InpointError::Protocol(format!(
                            "rtmp server exited with error: {e}"
                        )))
                    }
                }
            }
            // Run the media receiver. A failure (the hub's event channel
            // closed) is surfaced as an error so the orchestrator restarts
            // the whole server instead of reading it as a clean stop (#367).
            result = media_receiver.run() => {
                match result {
                    Ok(()) => {
                        info!("Media receiver stopped");
                        Ok(())
                    }
                    Err(e) => {
                        error!("Media receiver failed: {e}");
                        Err(e)
                    }
                }
            }
            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                info!("RTMP server shutting down");
                Ok(())
            }
        };

        // Flush remaining data
        flv_chunk_sink.flush().await;

        run_result
    }

    /// Accept RTMP connections and drive each through xiu's `ServerSession`.
    ///
    /// This mirrors xiu's own `rtmp::rtmp::RtmpServer::run` accept loop (bind →
    /// accept → spawn `ServerSession::run`), but on a listener we own so the
    /// bind is under our control (see `run` / `run_on_listener`).
    async fn accept_loop(
        listener: &TcpListener,
        event_sender: StreamHubEventSender,
        play_queue_limit: usize,
    ) -> Result<(), crate::InpointError> {
        loop {
            let (tcp_stream, _peer) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    // A per-connection accept error (a peer that reset a queued
                    // connection — e.g. WSAECONNRESET / ECONNABORTED — or a
                    // momentary fd exhaustion) must NOT tear down live RTMP
                    // ingest. Log it and keep accepting; a short backoff avoids
                    // a busy-loop if the condition persists (e.g. EMFILE). Note
                    // this is a deliberate improvement over xiu's own accept
                    // loop, which propagated the first accept error.
                    error!("RTMP accept error (continuing): {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    continue;
                }
            };
            let mut session = ServerSession::new(
                tcp_stream,
                event_sender.clone(),
                1, // GOP cache size (matches the previous xiu server config)
                None,
            );
            session.common.play_queue_limit = Some(play_queue_limit);
            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    info!(
                        "RTMP session ended: app={}, stream={}, err={err}",
                        session.app_name, session.stream_name
                    );
                }
                retire_session(session);
            });
        }
    }
}

/// Drop a finished session on the runtime's blocking pool, never on the
/// ingest thread (#368). Dropping it drains and frees every frame still
/// queued for it: up to 8 GB and 7 s for a player that fell behind, while
/// OBS's socket went unread. Only a drop still queued when the runtime shuts
/// down runs on the thread that drops the runtime; no publisher is served
/// then.
fn retire_session<S: Send + 'static>(session: S) {
    drop(tokio::task::spawn_blocking(move || drop(session)));
}

#[cfg(test)]
#[allow(
    clippy::disallowed_methods,
    reason = "test code: #[tokio::test] expands to Runtime::block_on, and tests read chunk files back and share hub state synchronously; never on the ingest runtime (#368 clippy.toml)"
)]
mod tests {
    use super::*;

    /// Records the thread its drop runs on.
    struct DropProbe(std::sync::mpsc::Sender<Option<String>>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            let _ = self
                .0
                .send(std::thread::current().name().map(str::to_string));
        }
    }

    /// #368: a session that ended is never dropped on the ingest thread.
    /// Dropping a player's session freed its whole queue there (8 GB, 7 s
    /// on stream.lan), and OBS's socket went unread the whole time.
    #[test]
    fn a_finished_session_is_dropped_off_the_ingest_thread() {
        let dropped_on = std::thread::Builder::new()
            .name("ingest-under-test".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name("ingest-io-under-test")
                    .build()
                    .expect("runtime");
                let (tx, rx) = std::sync::mpsc::channel();
                runtime.block_on(async move { retire_session(DropProbe(tx)) });
                // Wait while the runtime is alive: its shutdown would run a
                // still-queued drop on this thread.
                rx.recv_timeout(std::time::Duration::from_secs(5))
            })
            .expect("spawn")
            .join()
            .expect("join")
            .expect("the session must be dropped");
        assert_eq!(dropped_on.as_deref(), Some("ingest-io-under-test"));
    }

    #[tokio::test]
    async fn server_binds_and_shuts_down() {
        let server = RtmpServer::new("127.0.0.1", 0);
        let shutdown = server.shutdown_handle();
        let flv_sink = Arc::new(FlvChunkSink::new_null());
        let inpoint_state = InpointState::new();

        let handle = tokio::spawn(async move { server.run(flv_sink, inpoint_state).await });

        // Give it a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown.send(());

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    /// #368: `run_inpoint_loop` stops the server with `shutdown.send(())`
    /// and then awaits its task. A stop or restart that lands before the
    /// server task got to subscribe (it runs on the ingest runtime, after the
    /// bind) must not be lost: a lost one left that await, and the inpoint,
    /// hanging forever.
    #[tokio::test]
    async fn a_shutdown_sent_before_the_server_runs_still_stops_it() {
        let server = RtmpServer::new("127.0.0.1", 0);
        let shutdown = server.shutdown_handle();
        let _ = shutdown.send(());

        let run = server.run(Arc::new(FlvChunkSink::new_null()), InpointState::new());
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), run)
            .await
            .expect("a shutdown sent before run() must still stop the server");
        assert!(
            result.is_ok(),
            "a requested stop is a clean stop: {result:?}"
        );
    }

    /// #106: a bind failure must PROPAGATE (Err), never be swallowed into
    /// Ok(()). The old code logged the xiu bind error and returned Ok, which
    /// the orchestrator read as a clean stop and gave up permanently — the
    /// silent-ingest death this hardening exists to kill.
    #[tokio::test]
    async fn run_returns_err_when_port_is_occupied() {
        // Hog a real port so xiu cannot bind it.
        let hog = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = hog.local_addr().unwrap().port();

        let server = RtmpServer::new("127.0.0.1", port);
        let flv_sink = Arc::new(FlvChunkSink::new_null());
        let inpoint_state = InpointState::new();

        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            server.run(flv_sink, inpoint_state),
        )
        .await
        .expect("run() must return promptly on a bind failure, not hang");

        assert!(
            result.is_err(),
            "a bind conflict must propagate as Err, not be swallowed into Ok"
        );
    }
}
