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

/// RTMP server that accepts connections from OBS/vMix on a configurable port.
///
/// Uses the xiu RTMP implementation for proper protocol handling including
/// full handshake, AMF command parsing, and H.264/AAC media extraction.
/// Media data flows through the StreamsHub to the MediaReceiver which
/// subscribes to the published stream and feeds the FlvChunkSink.
pub struct RtmpServer {
    address: String,
    shutdown_tx: broadcast::Sender<()>,
}

impl RtmpServer {
    pub fn new(bind: &str, port: u16) -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            address: format!("{bind}:{port}"),
            shutdown_tx,
        }
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

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::select! {
            // Run the StreamsHub event loop
            _ = hub.run() => {
                info!("StreamsHub stopped");
            }
            // Run the RTMP accept loop (uses xiu's ServerSession per connection)
            result = Self::accept_loop(&listener, event_sender) => {
                match result {
                    Ok(()) => info!("RTMP accept loop stopped"),
                    Err(e) => error!("RTMP accept loop error: {e}"),
                }
            }
            // Run the media receiver
            _ = media_receiver.run() => {
                info!("Media receiver stopped");
            }
            // Handle shutdown signal
            _ = shutdown_rx.recv() => {
                info!("RTMP server shutting down");
            }
        }

        // Flush remaining data
        flv_chunk_sink.flush().await;

        Ok(())
    }

    /// Accept RTMP connections and drive each through xiu's `ServerSession`.
    ///
    /// This mirrors xiu's own `rtmp::rtmp::RtmpServer::run` accept loop (bind →
    /// accept → spawn `ServerSession::run`), but on a listener we own so the
    /// bind is under our control (see `run` / `run_on_listener`).
    async fn accept_loop(
        listener: &TcpListener,
        event_sender: StreamHubEventSender,
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
            tokio::spawn(async move {
                if let Err(err) = session.run().await {
                    info!(
                        "RTMP session ended: app={}, stream={}, err={err}",
                        session.app_name, session.stream_name
                    );
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
