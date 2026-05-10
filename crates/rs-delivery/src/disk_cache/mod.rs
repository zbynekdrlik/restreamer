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
pub mod prefetch_queue;
pub mod prefetch_reader;
mod registry;

#[cfg(test)]
mod prefetch_queue_tests;
#[cfg(test)]
mod prefetch_reader_tests;

pub use download_service::{DownloadService, FetchedChunk, S3Backend};
pub use endpoint_reader::EndpointReader;
pub use eviction::EvictionTask;
pub use position_registry::{EndpointPositionRegistry, EndpointWindow};
pub use prefetch_queue::PrefetchQueue;
pub use prefetch_reader::PrefetchReader;
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
    /// Per-endpoint PrefetchQueue handles, keyed by alias. Lifetimes
    /// match the endpoint's run; close()-d on endpoint stop.
    endpoint_queues: tokio::sync::Mutex<
        std::collections::HashMap<String, Arc<prefetch_queue::PrefetchQueue<Arc<Vec<u8>>>>>,
    >,
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
            endpoint_queues: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Per-event cache directory: `{cache_dir}/{event_id}/`.
    pub fn event_dir(&self) -> PathBuf {
        self.cache_dir.join(&self.event_id)
    }

    /// Snapshot of the cumulative S3 fetch profile for diag dumps.
    pub fn s3_fetch_profile_snapshot(&self) -> crate::s3_fetch_profile::S3FetchProfileSnapshot {
        self.download_service.profile_snapshot()
    }

    /// Abort the eviction task and release cache handles. Call when the
    /// event ends. Does not delete cached files (the next DiskCache::new
    /// for the same event will reuse them).
    pub async fn shutdown(self) {
        self.eviction_handle.abort();
    }

    /// Build (and remember) a PrefetchQueue + spawn its PrefetchReader for
    /// the given endpoint alias. Idempotent: returns the existing queue if
    /// the alias is already registered. `prefetch_k` is the prefetch depth
    /// (0 = synchronous rendezvous, >=1 = buffered).
    ///
    /// Spawns the PrefetchReader in the background. Caller's responsibility
    /// to invoke `close_endpoint_queue(alias)` on endpoint shutdown so the
    /// reader task exits cleanly.
    pub async fn ensure_endpoint_queue(
        &self,
        alias: &str,
        start_chunk_id: i64,
        prefetch_k: usize,
        audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    ) -> Arc<prefetch_queue::PrefetchQueue<Arc<Vec<u8>>>> {
        use std::sync::atomic::AtomicI64;
        let mut g = self.endpoint_queues.lock().await;
        if let Some(q) = g.get(alias) {
            return Arc::clone(q);
        }
        let queue = prefetch_queue::PrefetchQueue::new(prefetch_k);
        let next_id = Arc::new(AtomicI64::new(start_chunk_id));
        let q_run = Arc::clone(&queue);
        let dl = Arc::clone(&self.download_service);
        tokio::spawn(async move {
            prefetch_reader::PrefetchReader::run(q_run, dl, next_id, audit_ring).await;
        });
        g.insert(alias.to_string(), Arc::clone(&queue));
        queue
    }

    /// Close the per-endpoint queue (signals the PrefetchReader task to
    /// exit by causing its push_back to return Err). Idempotent.
    pub async fn close_endpoint_queue(&self, alias: &str) {
        let mut g = self.endpoint_queues.lock().await;
        if let Some(q) = g.remove(alias) {
            q.close();
        }
    }
}
