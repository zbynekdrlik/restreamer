//! Per-event local-disk chunk cache for rs-delivery (issue #174).
//!
//! Decouples upstream S3 ingress from the RTMP push hot path. See
//! `docs/superpowers/specs/2026-05-05-rs-delivery-disk-cache-design.md`
//! for the full architectural rationale.
//!
//! Component map:
//! - `ChunkRegistry`: in-memory availability state with tokio::Notify wake.
//! - `DownloadService`: bandwidth-managed S3 fetcher; deduplicates in-flight requests.
//! - `EndpointReader`: replaces the consumer_task hot loop; reads disk -> RTMP.
//! - `EvictionTask`: deletes files outside any endpoint window.
//! - `EndpointPositionRegistry`: tracks per-endpoint chunk_id for eviction.
//!
//! `DiskCache` is the public facade. One instance per event.
//!
//! Tasks 13+ wire this module into `init_endpoints` and `EndpointHandle::spawn`.
//! Until that integration lands, the components are unused outside their tests
//! -- the allow(dead_code, unused_imports) below silences the lints across the
//! whole module while preserving the exact API surface for the integration PR.
#![allow(dead_code, unused_imports)]

pub mod download_service;
mod endpoint_reader;
mod eviction;
mod position_registry;
mod registry;

pub use download_service::{DownloadService, S3Backend};
pub use endpoint_reader::EndpointReader;
pub use eviction::EvictionTask;
pub use position_registry::{EndpointPositionRegistry, EndpointWindow};
pub use registry::{ChunkAvailability, ChunkRegistry};

use std::path::PathBuf;
use std::sync::Arc;

/// Configuration for a `DiskCache` instance. One per event.
#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    /// Root directory for cache files. Per-event subdirectory created automatically.
    pub cache_dir: PathBuf,
    /// Cache window per endpoint, in chunks (typically `cache_delay_secs / chunk_dur_secs`).
    pub window_chunks: i64,
    /// Maximum total S3 ingress in megabits per second across the whole event.
    pub s3_ingress_cap_mbit: u64,
    /// Eviction sweep interval.
    pub eviction_interval_secs: u64,
    /// `wait_for_chunk` timeout -- surfaces real S3 outages.
    pub read_stall_timeout_secs: u64,
    /// Bounded download-request queue size.
    pub download_queue_capacity: usize,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            cache_dir: PathBuf::from("/var/cache/rs-delivery"),
            window_chunks: 60,
            s3_ingress_cap_mbit: 200,
            eviction_interval_secs: 5,
            read_stall_timeout_secs: 60,
            download_queue_capacity: 200,
        }
    }
}

/// Per-event facade over the cache subsystem.
pub struct DiskCache {
    pub registry: Arc<ChunkRegistry>,
    pub download_service: Arc<DownloadService>,
    pub position_registry: Arc<EndpointPositionRegistry>,
    eviction_handle: tokio::task::JoinHandle<()>,
    pub cache_dir: PathBuf,
    pub event_id: String,
    pub window_chunks: i64,
}

impl DiskCache {
    /// Construct a new DiskCache for one event. Creates the per-event
    /// cache directory, builds the chunk registry, the position
    /// registry, the bandwidth-managed download service, and spawns
    /// the eviction sweep task.
    pub async fn new(
        cfg: DiskCacheConfig,
        backend: Arc<dyn download_service::S3Backend>,
        event_id: String,
    ) -> std::io::Result<Self> {
        let event_dir = cfg.cache_dir.join(&event_id);
        tokio::fs::create_dir_all(&event_dir).await?;
        let registry = ChunkRegistry::new();
        let position_registry = EndpointPositionRegistry::new();
        let download_service = DownloadService::new(
            backend,
            Arc::clone(&registry),
            cfg.cache_dir.clone(),
            event_id.clone(),
            cfg.s3_ingress_cap_mbit,
            8,
        );
        let eviction_handle = EvictionTask::spawn(
            event_dir.clone(),
            Arc::clone(&position_registry),
            Arc::clone(&registry),
            std::time::Duration::from_secs(cfg.eviction_interval_secs),
        );
        Ok(Self {
            registry,
            download_service,
            position_registry,
            eviction_handle,
            cache_dir: cfg.cache_dir,
            event_id,
            window_chunks: cfg.window_chunks,
        })
    }

    /// Per-event cache directory: `{cache_dir}/{event_id}/`.
    pub fn event_dir(&self) -> PathBuf {
        self.cache_dir.join(&self.event_id)
    }

    /// Abort the eviction task and release cache handles. Call when the
    /// event ends. Does not delete cached files (the next DiskCache::new
    /// for the same event will reuse them).
    pub async fn shutdown(self) {
        self.eviction_handle.abort();
    }
}
