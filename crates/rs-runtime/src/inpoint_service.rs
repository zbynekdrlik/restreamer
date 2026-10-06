//! The RTMP ingest subsystem as the orchestrator runs it (#368).
//!
//! `InpointService::start` launches the inpoint supervision loop
//! (`run_inpoint_loop`: bind probe, restart, crash backoff, heartbeat) on the
//! caller's (main) runtime, and the RTMP server that loop supervises on the
//! dedicated [`IngestRuntime`]. So no other subsystem (DB, HTTP, delivery,
//! notifications, a blocking slip anywhere) can stop the OBS socket read.

use std::sync::Arc;

use rs_core::models::{InpointState, WsEvent};
use rs_inpoint::flv_chunker::FlvChunkSink;
use tokio::runtime::Handle;
use tokio::sync::{broadcast, mpsc};
use tokio::task::{JoinError, JoinHandle};

use crate::ingest_runtime::IngestRuntime;

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

/// The running inpoint: its supervision loop and its ingest runtime.
pub(crate) struct InpointService {
    supervisor: Option<JoinHandle<()>>,
    ingest: Option<IngestRuntime>,
}

impl InpointService {
    /// Start the ingest runtime, then the supervision loop on the current
    /// runtime. Call it inside a tokio runtime.
    pub(crate) fn start(p: InpointParams) -> std::io::Result<Self> {
        let ingest = IngestRuntime::start()?;
        let supervisor = tokio::spawn(crate::orchestrator::run_inpoint_loop(
            p.bind,
            p.port,
            p.flv_chunk_sink,
            p.inpoint_state,
            p.ws_tx,
            p.restart_rx,
            p.shutdown_rx,
            ingest.handle().clone(),
        ));
        Ok(Self {
            supervisor: Some(supervisor),
            ingest: Some(ingest),
        })
    }

    /// The ingest runtime, for the stall detector's probe. `None` once
    /// stopped.
    pub(crate) fn ingest_handle(&self) -> Option<&Handle> {
        self.ingest.as_ref().map(IngestRuntime::handle)
    }

    /// Is the ingest runtime thread alive?
    #[cfg(test)]
    pub(crate) fn is_ingest_running(&self) -> bool {
        self.ingest.as_ref().is_some_and(IngestRuntime::is_running)
    }

    /// Wait for the supervision loop to end (send the shutdown signal
    /// first; the loop stops the RTMP server and flushes the chunker), then
    /// shut the ingest runtime down and wait for its thread.
    pub(crate) async fn stop(&mut self) -> Result<(), JoinError> {
        let supervised = match self.supervisor.take() {
            Some(task) => task.await,
            None => Ok(()),
        };
        if let Some(mut ingest) = self.ingest.take() {
            // Joining the thread blocks: never on an async worker.
            if let Err(e) = tokio::task::spawn_blocking(move || ingest.shutdown()).await {
                log::error!("ingest runtime shutdown task failed: {e}");
            }
        }
        supervised
    }
}

#[cfg(test)]
#[path = "inpoint_service_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "inpoint_service_lifecycle_tests.rs"]
mod lifecycle_tests;
