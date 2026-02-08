use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast};
use tracing::{error, info, warn};

use crate::chunker::ChunkSink;
use crate::session::RtmpSession;

/// Maximum concurrent RTMP sessions (typically only 1 encoder in church setups).
const MAX_RTMP_SESSIONS: usize = 4;

/// RTMP server that accepts connections from OBS/vMix on a configurable port.
///
/// This is a pure-Rust RTMP server. It accepts TCP connections, performs the
/// RTMP handshake, and receives published audio/video data which is forwarded
/// to the chunker for MPEG-TS muxing.
pub struct RtmpServer {
    addr: SocketAddr,
    shutdown_tx: broadcast::Sender<()>,
}

impl RtmpServer {
    pub fn new(bind: &str, port: u16) -> Self {
        let addr: SocketAddr = format!("{bind}:{port}")
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], port)));
        let (shutdown_tx, _) = broadcast::channel(1);
        Self { addr, shutdown_tx }
    }

    /// Returns a shutdown handle that can be used to stop the server.
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Run the RTMP server, accepting connections until shutdown.
    pub async fn run(self, chunk_sink: Arc<ChunkSink>) -> Result<(), crate::InpointError> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("RTMP server listening on {}", self.addr);

        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let session_sem = Arc::new(Semaphore::new(MAX_RTMP_SESSIONS));

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer)) => {
                            let permit = match session_sem.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    warn!("RTMP connection from {peer} rejected: max sessions ({MAX_RTMP_SESSIONS}) reached");
                                    drop(stream);
                                    continue;
                                }
                            };
                            info!("RTMP connection from {peer}");
                            let sink = Arc::clone(&chunk_sink);
                            let mut peer_shutdown = self.shutdown_tx.subscribe();
                            tokio::spawn(async move {
                                let _permit = permit;
                                let mut session = RtmpSession::new(stream, sink);
                                tokio::select! {
                                    result = session.run() => {
                                        match result {
                                            Ok(()) => info!("RTMP session from {peer} ended cleanly"),
                                            Err(e) => warn!("RTMP session from {peer} error: {e}"),
                                        }
                                    }
                                    _ = peer_shutdown.recv() => {
                                        info!("RTMP session from {peer} shutting down");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept RTMP connection: {e}");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("RTMP server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn server_binds_and_shuts_down() {
        let server = RtmpServer::new("127.0.0.1", 0); // random port
        let shutdown = server.shutdown_handle();
        let sink = Arc::new(ChunkSink::new_null());

        let handle = tokio::spawn(async move { server.run(sink).await });

        // Give it a moment to bind
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = shutdown.send(());

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
