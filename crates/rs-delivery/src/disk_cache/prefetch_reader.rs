//! PrefetchReader — background task feeding PrefetchQueue from
//! DownloadService. Retries forever on fetch failure (#184).
//! Implementation in Task 16.

#![allow(dead_code)]

use super::download_service::DownloadService;
use super::prefetch_queue::PrefetchQueue;
use crate::audit_ring::AuditRing;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;

pub struct PrefetchReader;

impl PrefetchReader {
    /// Drive the prefetch loop. Returns when the queue is closed.
    pub async fn run(
        _queue: Arc<PrefetchQueue<Arc<Vec<u8>>>>,
        _download: Arc<DownloadService>,
        _next_chunk_id: Arc<AtomicI64>,
        _audit_ring: Option<Arc<AuditRing>>,
    ) {
        unimplemented!("Task 16")
    }
}
