//! `EndpointHandle` — the owning handle for a spawned per-endpoint delivery
//! task. Extracted from `endpoint_task.rs` to keep that file under the
//! 1000-line file-size gate (CI `file-size` job). Included via `#[path]` as
//! `mod endpoint_handle` inside `endpoint_task.rs`, so `super::` reaches the
//! sibling `endpoint_loop` fn and the `FfmpegProcessFactory` re-export.
//! `EndpointHandle` is re-exported at the `endpoint_task` level so the
//! existing `crate::endpoint_task::EndpointHandle` (and `crate::EndpointHandle`
//! via main.rs) import paths keep resolving unchanged. Pure move — no logic
//! change.

use std::sync::Arc;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use super::{
    BufferState, EndpointStats, FfmpegProcessFactory, Stats, endpoint_loop, initial_delivery_mode,
    initial_endpoint_stats,
};
use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;

pub struct EndpointHandle {
    task: JoinHandle<()>,
    stop_tx: watch::Sender<bool>,
    stats: Stats,
    start_chunk_id: i64,
    cfg: crate::api::EndpointConfig,
}

impl EndpointHandle {
    /// Spawn an endpoint task backed by the shared per-event `DiskCache`.
    /// `disk_cache` is required: if construction failed at /api/init, the
    /// orchestrator already received a 500 and never reaches here.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        ep_cfg: EndpointConfig,
        start_chunk_id: i64,
        delivery_delay_ms: u64,
        rescue_video_url: Option<String>,
        audit_ring: Option<Arc<AuditRing>>,
        disk_cache: Arc<crate::disk_cache::DiskCache>,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        let initial_mode = initial_delivery_mode(
            rescue_video_url.is_some(),
            ep_cfg.is_fast,
            delivery_delay_ms,
        );
        let stats: Stats = Arc::new(Mutex::new(initial_endpoint_stats(
            start_chunk_id,
            initial_mode,
        )));
        let buffer_state = Arc::new(BufferState::new());
        // Fast endpoints skip the delay entirely.
        let effective_delay = if ep_cfg.is_fast { 0 } else { delivery_delay_ms };
        let window = disk_cache.window_chunks;
        let fetcher = crate::disk_cache_fetcher::DiskCacheFetcher::new(
            disk_cache,
            ep_cfg.alias.clone(),
            start_chunk_id,
            window,
            60,
            audit_ring.clone(),
        );
        tracing::info!(alias = %ep_cfg.alias, window, "DiskCacheFetcher wired");
        // Clone for the spawned task so the original survives for the
        // EndpointHandle's `cfg` field. `cfg` powers the `config()` accessor
        // used by api::update_start_handler when it tears down and respawns
        // this endpoint with a new start_chunk_id (#189).
        let cfg = ep_cfg.clone();
        let task = tokio::spawn(endpoint_loop(
            fetcher,
            FfmpegProcessFactory,
            ep_cfg,
            start_chunk_id,
            effective_delay,
            stop_rx,
            stats.clone(),
            rescue_video_url,
            buffer_state,
            audit_ring,
        ));
        Self {
            task,
            stop_tx,
            stats,
            start_chunk_id,
            cfg,
        }
    }

    pub fn start_chunk_id(&self) -> i64 {
        self.start_chunk_id
    }

    pub fn config(&self) -> &crate::api::EndpointConfig {
        &self.cfg
    }

    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    pub async fn stats(&self) -> EndpointStats {
        self.stats.lock().await.clone()
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.task).await;
    }

    /// Test-only stub: creates a no-op EndpointHandle with the given start_chunk_id.
    /// Used by api_update_start_tests to seed AppState without a real DiskCache.
    #[cfg(test)]
    pub fn stub_for_test(start_chunk_id: i64) -> Self {
        let (stop_tx, _stop_rx) = watch::channel(false);
        let task = tokio::spawn(async {});
        let stats = Arc::new(Mutex::new(crate::endpoint_stats::initial_endpoint_stats(
            start_chunk_id,
            "normal".to_string(),
        )));
        let cfg = crate::api::EndpointConfig {
            alias: "stub".to_string(),
            service_type: "TEST_FILE".to_string(),
            stream_key: String::new(),
            is_fast: false,
            chunk_format: "flv".to_string(),
            start_chunk_id: None,
            pusher: Default::default(),
        };
        Self {
            task,
            stop_tx,
            stats,
            start_chunk_id,
            cfg,
        }
    }
}
