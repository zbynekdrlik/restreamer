//! PrefetchReader — drives PrefetchQueue. Retries forever on fetch
//! failure per spec §3.4 + user rule (never give up). One audit row
//! per minute per error class while retry is active (handled by
//! DownloadService.fetch_with_retry's internal S3FetchAuditLimiter).

use super::download_service::DownloadService;
use super::prefetch_queue::PrefetchQueue;
use super::registry::ChunkAvailability;
use crate::audit_ring::AuditRing;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

pub struct PrefetchReader;

impl PrefetchReader {
    /// Drive the prefetch loop. Returns when the queue is closed
    /// (endpoint shutdown). Never returns due to fetch errors — even
    /// across persistent S3 outages, the loop keeps trying.
    pub async fn run(
        queue: Arc<PrefetchQueue<Arc<Vec<u8>>>>,
        download: Arc<DownloadService>,
        next_chunk_id: Arc<AtomicI64>,
        _audit_ring: Option<Arc<AuditRing>>,
    ) {
        loop {
            if queue.is_closed() {
                return;
            }
            let id = next_chunk_id.fetch_add(1, Ordering::AcqRel);
            let Some(bytes) = Self::fetch_until_available(&download, &queue, id).await else {
                // Queue closed mid-fetch (between retries). Exit cleanly.
                return;
            };
            let arc_bytes = Arc::new(bytes);
            if queue.push_back(arc_bytes).await.is_err() {
                // Queue closed at push time. Exit cleanly.
                return;
            }
        }
    }

    /// Inner loop: keep calling request_chunk until the registry
    /// reports Available. NotFound triggers a backoff and re-attempt.
    /// Returns None if the queue is closed during a retry — letting
    /// the caller exit instead of pushing into a dead queue.
    async fn fetch_until_available(
        download: &Arc<DownloadService>,
        queue: &Arc<PrefetchQueue<Arc<Vec<u8>>>>,
        chunk_id: i64,
    ) -> Option<Vec<u8>> {
        let mut backoff_secs: u64 = 1;
        loop {
            if queue.is_closed() {
                return None;
            }
            download.request_chunk(chunk_id).await;
            if queue.is_closed() {
                return None;
            }
            match Self::try_read_from_disk(download, chunk_id).await {
                Some(bytes) => return Some(bytes),
                None => {
                    if queue.is_closed() {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
                    backoff_secs = (backoff_secs * 2).min(60);
                }
            }
        }
    }

    /// Read the chunk's cached file from disk. Returns None if the
    /// registry reports NotFound or Evicted (so the outer loop can
    /// retry without panicking).
    async fn try_read_from_disk(download: &Arc<DownloadService>, chunk_id: i64) -> Option<Vec<u8>> {
        let registry = download.registry_for_test();
        let state = registry
            .wait_for_chunk_with_timeout(chunk_id, Duration::from_secs(60))
            .await
            .ok()?;
        if !matches!(state, ChunkAvailability::Available { .. }) {
            return None;
        }
        let path = download.chunk_path(chunk_id);
        tokio::fs::read(&path).await.ok()
    }
}
