use std::sync::Arc;

use streamhub::StreamsHub;
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

    /// Run the RTMP server, accepting connections until shutdown.
    pub async fn run(
        self,
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

        // Create the xiu RTMP server with the hub's event sender
        let mut rtmp_server = rtmp::rtmp::RtmpServer::new(
            self.address.clone(),
            event_sender.clone(),
            1, // GOP cache size
            None,
        );

        // Create media receiver that subscribes to published streams and
        // processes frame data into FLV chunks
        let media_receiver = MediaReceiver::new(
            event_consumer,
            event_sender,
            Arc::clone(&flv_chunk_sink),
            inpoint_state,
        );

        info!("RTMP server starting on {}", self.address);

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // #106: capture the run outcome instead of swallowing it. A xiu bind
        // failure (e.g. a process grabbed the port in the TOCTOU window between
        // the runtime's pre-bind probe and this bind) used to be logged and
        // then dropped as `Ok(())`, which the orchestrator read as a clean stop
        // and gave up permanently — the exact silent-death this hardening
        // exists to kill. Propagate it so `run_inpoint_loop` restarts + the
        // next pre-bind probe surfaces the conflict on the dashboard.
        let run_result: Result<(), crate::InpointError> = tokio::select! {
            // Run the StreamsHub event loop
            _ = hub.run() => {
                info!("StreamsHub stopped");
                Ok(())
            }
            // Run the xiu RTMP server
            result = rtmp_server.run() => {
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
            // Run the media receiver
            _ = media_receiver.run() => {
                info!("Media receiver stopped");
                Ok(())
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
