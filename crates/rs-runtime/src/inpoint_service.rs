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

use crate::ingest_runtime::{INGEST_SHUTDOWN_TIMEOUT, IngestRuntime};

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
    /// The chunker, whose background writes run on the ingest runtime and
    /// must finish before it stops. Released by `stop`, so the chunk
    /// forwarder's channel can close.
    sink: Option<Arc<FlvChunkSink>>,
}

impl InpointService {
    /// Start the ingest runtime, then the supervision loop on the current
    /// runtime. Call it inside a tokio runtime.
    pub(crate) fn start(p: InpointParams) -> Self {
        Self::start_on(p, IngestRuntime::start())
    }

    /// [`InpointService::start`] with the ingest runtime given. If it could
    /// not be started (no thread could be created), the RTMP server runs on
    /// the current runtime as before #368: degraded and logged, but the app
    /// keeps serving (#106), never a dead service.
    fn start_on(p: InpointParams, ingest: std::io::Result<IngestRuntime>) -> Self {
        let ingest = match ingest {
            Ok(ingest) => Some(ingest),
            Err(e) => {
                log::error!(
                    "ingest runtime NOT started ({e}); the RTMP inpoint runs on the app \
                     runtime, unprotected from its stalls (#368)"
                );
                None
            }
        };
        let server_runtime = ingest
            .as_ref()
            .map_or_else(Handle::current, |ingest| ingest.handle().clone());
        let sink = Arc::clone(&p.flv_chunk_sink);
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
        Self {
            supervisor: Some(supervisor),
            ingest,
            sink: Some(sink),
        }
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
    /// first; the loop stops the RTMP server and flushes the chunker), let
    /// the chunk writes still running finish, then shut the ingest runtime
    /// down and wait for its thread.
    pub(crate) async fn stop(&mut self) -> Result<(), JoinError> {
        let supervised = match self.supervisor.take() {
            Some(task) => task.await,
            None => Ok(()),
        };
        // Shutting the runtime down cancels its tasks: a chunk still being
        // written would never be reported, so never reach the DB/uploader.
        if let Some(sink) = self.sink.take() {
            let pending = sink.wait_for_writes(INGEST_SHUTDOWN_TIMEOUT).await;
            log::log!(
                drain_log_level(pending),
                "inpoint stop: chunk writes still running after the drain: {pending} \
                 (cancelled with the ingest runtime: no DB row, no upload)"
            );
        }
        if let Some(mut ingest) = self.ingest.take() {
            // Joining the thread blocks: never on an async worker.
            match tokio::task::spawn_blocking(move || ingest.shutdown()).await {
                Ok(joined) => log::info!("ingest runtime shut down (thread joined: {joined})"),
                Err(e) => log::error!("ingest runtime shutdown task failed: {e}"),
            }
        }
        supervised
    }
}

/// A chunk write still running after the drain is about to be cancelled,
/// and its chunk lost to the uploader: that is a warning, none is info.
fn drain_log_level(pending: u32) -> log::Level {
    if pending == 0 {
        log::Level::Info
    } else {
        log::Level::Warn
    }
}

#[cfg(test)]
#[path = "inpoint_service_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "inpoint_service_lifecycle_tests.rs"]
mod lifecycle_tests;
