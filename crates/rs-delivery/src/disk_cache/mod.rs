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

mod download_service;
mod endpoint_reader;
mod eviction;
mod position_registry;
mod registry;

pub use download_service::DownloadService;
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
}

impl DiskCache {
    /// Construct a new DiskCache for one event. Spawns EvictionTask.
    /// Returns `Err` if cache_dir cannot be created.
    pub async fn new(_cfg: DiskCacheConfig) -> std::io::Result<Self> {
        unimplemented!("scaffold; implemented in Task 13")
    }

    /// Create an `EndpointReader` for one endpoint, registered with this cache.
    /// Caller must spawn the returned reader on a tokio task.
    pub fn endpoint_reader(&self, _alias: &str, _start_chunk_id: i64) -> EndpointReader {
        unimplemented!("scaffold; implemented in Task 12")
    }

    /// Abort the eviction task and release cache handles. Call when the
    /// event ends. Does not delete cached files (the next DiskCache::new
    /// for the same event will reuse them).
    pub async fn shutdown(self) {
        self.eviction_handle.abort();
    }
}
