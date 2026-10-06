//! The dedicated ingest runtime (#368).
//!
//! OBS drops frames when its RTMP send stalls for more than ~700 ms. The
//! inpoint reads that socket inside xiu session tasks, so whatever runtime
//! polls those tasks decides whether OBS drops frames. On the app-wide runtime
//! any blocking call, held lock or starved worker anywhere in restreamer
//! stopped the socket read (Sunday 2026-10-04: two ~5-7 s stalls, 416 dropped
//! frames).
//!
//! The inpoint (xiu server + streams hub + `MediaReceiver` + `FlvChunkSink`)
//! therefore runs on its own `current_thread` runtime, driven by ONE
//! dedicated OS thread, `restreamer-ingest`, raised to
//! `THREAD_PRIORITY_HIGHEST` on Windows (`ingest_priority`). Nothing else
//! runs there. Chunks and events cross to the main runtime through the
//! existing tokio channels, which work across runtimes. The chunker's disk
//! writes (`tokio::fs`) use this runtime's own blocking pool.

use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::oneshot;

use crate::ingest_priority::{
    PriorityOs, SystemPriorityOs, ThreadPriorityReport, raise_ingest_thread,
};

/// Name of the thread that drives the ingest runtime.
pub const INGEST_THREAD_NAME: &str = "restreamer-ingest";
/// Name of the ingest runtime's blocking-pool threads (chunk file writes).
pub const INGEST_BLOCKING_THREAD_NAME: &str = "restreamer-ingest-io";
/// How long a shutdown waits for blocking tasks still running (a chunk file
/// write) before it lets them go.
pub const INGEST_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// The ingest runtime and the thread that drives it.
///
/// Dropping it stops the runtime without waiting for the thread;
/// [`IngestRuntime::shutdown`] stops it and waits.
#[derive(Debug)]
pub struct IngestRuntime {
    handle: Handle,
    thread_priority: ThreadPriorityReport,
    stop_tx: Option<oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl IngestRuntime {
    /// Start the runtime with the real OS priorities. Returns once the
    /// runtime accepts tasks.
    pub fn start() -> std::io::Result<Self> {
        Self::start_with(SystemPriorityOs)
    }

    /// [`IngestRuntime::start`] with the OS priority calls given.
    pub fn start_with<O: PriorityOs + Send + 'static>(os: O) -> std::io::Result<Self> {
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let thread = std::thread::Builder::new()
            .name(INGEST_THREAD_NAME.into())
            .spawn(move || {
                let thread_priority = raise_ingest_thread(&os);
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .thread_name(INGEST_BLOCKING_THREAD_NAME)
                    .build();
                let runtime = match runtime {
                    Ok(runtime) => runtime,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };
                if ready_tx
                    .send(Ok((runtime.handle().clone(), thread_priority)))
                    .is_err()
                {
                    return;
                }
                // Drives every ingest task until stopped (or the
                // IngestRuntime is dropped, which drops `stop_tx`).
                runtime.block_on(async {
                    let _ = stop_rx.await;
                });
                runtime.shutdown_timeout(INGEST_SHUTDOWN_TIMEOUT);
                log::info!("ingest runtime stopped (thread {INGEST_THREAD_NAME})");
            })?;
        let (handle, thread_priority) = match ready_rx.recv() {
            Ok(Ok(ready)) => ready,
            Ok(Err(e)) => {
                let _ = thread.join();
                return Err(e);
            }
            Err(_) => {
                let _ = thread.join();
                return Err(std::io::Error::other(
                    "the ingest runtime thread exited before its runtime was built",
                ));
            }
        };
        log::log!(
            thread_priority.level(),
            "ingest runtime started on its own thread {INGEST_THREAD_NAME}: {}",
            thread_priority.summary()
        );
        Ok(Self {
            handle,
            thread_priority,
            stop_tx: Some(stop_tx),
            thread: Some(thread),
        })
    }

    /// Spawns onto the ingest runtime.
    pub fn handle(&self) -> &Handle {
        &self.handle
    }

    /// What the thread priority raise did.
    pub fn thread_priority(&self) -> &ThreadPriorityReport {
        &self.thread_priority
    }

    /// Is the runtime thread still alive?
    pub fn is_running(&self) -> bool {
        self.thread.as_ref().is_some_and(|t| !t.is_finished())
    }

    /// Stop the runtime and wait for its thread to exit. Tasks still running
    /// are dropped; blocking tasks get up to [`INGEST_SHUTDOWN_TIMEOUT`].
    /// Blocking: from async code call it through `spawn_blocking`.
    pub fn shutdown(&mut self) {
        self.stop_tx.take();
        if let Some(thread) = self.thread.take() {
            if thread.join().is_err() {
                log::error!("ingest runtime thread {INGEST_THREAD_NAME} panicked");
            }
        }
    }
}

#[cfg(test)]
#[path = "ingest_runtime_tests.rs"]
mod tests;
