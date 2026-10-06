//! The RTMP ingest subsystem as the orchestrator runs it (#368).
//!
//! `InpointService::start` launches the inpoint supervision loop
//! (`run_inpoint_loop`: bind probe, restart, crash backoff, heartbeat) and
//! decides which runtime hosts the RTMP server that loop supervises.

use std::sync::Arc;

use rs_core::models::{InpointState, WsEvent};
use rs_inpoint::flv_chunker::FlvChunkSink;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{JoinError, JoinHandle};

/// Everything the inpoint supervision loop needs.
pub(crate) struct InpointParams {
    pub(crate) bind: String,
    pub(crate) port: u16,
    pub(crate) flv_chunk_sink: Arc<FlvChunkSink>,
    pub(crate) inpoint_state: InpointState,
    pub(crate) ws_tx: broadcast::Sender<WsEvent>,
    pub(crate) restart_rx: mpsc::Receiver<()>,
    pub(crate) shutdown_rx: broadcast::Receiver<()>,
}

/// The running inpoint: its supervision loop.
pub(crate) struct InpointService {
    supervisor: Option<JoinHandle<()>>,
}

impl InpointService {
    /// Start the supervision loop on the current runtime. Call it inside a
    /// tokio runtime.
    pub(crate) fn start(p: InpointParams) -> std::io::Result<Self> {
        let server_runtime = Handle::current();
        let supervisor = tokio::spawn(crate::orchestrator::run_inpoint_loop(
            p.bind,
            p.port,
            p.flv_chunk_sink,
            p.inpoint_state,
            p.ws_tx,
            p.restart_rx,
            p.shutdown_rx,
            server_runtime,
        ));
        Ok(Self {
            supervisor: Some(supervisor),
        })
    }

    /// Wait for the supervision loop to end. Send the shutdown signal first.
    pub(crate) async fn stop(&mut self) -> Result<(), JoinError> {
        match self.supervisor.take() {
            Some(task) => task.await,
            None => Ok(()),
        }
    }
}
