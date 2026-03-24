use std::sync::Arc;

use streamhub::StreamsHub;
use tokio::sync::broadcast;
use tracing::{error, info};

use rs_core::models::InpointState;

use crate::chunker::ChunkSink;
use crate::flv_chunker::FlvChunkSink;
use crate::media_receiver::{ChunkFormat, MediaReceiver};

/// RTMP server that accepts connections from OBS/vMix on a configurable port.
///
/// Uses the xiu RTMP implementation for proper protocol handling including
/// full handshake, AMF command parsing, and H.264/AAC media extraction.
/// Media data flows through the StreamsHub to the MediaReceiver which
/// subscribes to the published stream, demuxes FLV, muxes to MPEG-TS,
/// and feeds the ChunkSink.
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
        chunk_sink: Arc<ChunkSink>,
        flv_chunk_sink: Arc<FlvChunkSink>,
        inpoint_state: InpointState,
        chunk_format: ChunkFormat,
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
        // processes frame data into chunks (FLV or MPEG-TS depending on config)
        let media_receiver = MediaReceiver::new(
            event_consumer,
            event_sender,
            Arc::clone(&chunk_sink),
            Arc::clone(&flv_chunk_sink),
            inpoint_state,
            chunk_format,
        );

        info!("RTMP server starting on {}", self.address);

        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::select! {
            // Run the StreamsHub event loop
            _ = hub.run() => {
                info!("StreamsHub stopped");
            }
            // Run the xiu RTMP server
            result = rtmp_server.run() => {
                match result {
                    Ok(()) => info!("RTMP server stopped"),
                    Err(e) => error!("RTMP server error: {e}"),
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
        chunk_sink.flush().await;
        flv_chunk_sink.flush().await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server_binds_and_shuts_down() {
        let server = RtmpServer::new("127.0.0.1", 0);
        let shutdown = server.shutdown_handle();
        let sink = Arc::new(ChunkSink::new_null());
        let flv_sink = Arc::new(FlvChunkSink::new_null());
        let inpoint_state = InpointState::new();

        let handle = tokio::spawn(async move {
            server
                .run(sink, flv_sink, inpoint_state, ChunkFormat::Flv)
                .await
        });

        // Give it a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = shutdown.send(());

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
