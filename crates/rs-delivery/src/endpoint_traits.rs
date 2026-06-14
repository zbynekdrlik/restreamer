//! Core delivery-pipeline trait + value definitions, extracted from
//! `endpoint_task.rs` to keep that file under the 1000-line file-size gate
//! (CI `file-size` job). Included via `#[path]` as `mod endpoint_traits`
//! inside `endpoint_task.rs`; every item is re-exported at the
//! `endpoint_task` level so existing `crate::endpoint_task::{ChunkFetcher,
//! OutputProcess, OutputProcessFactory, PrefetchedChunk}` import paths (used
//! across `producer_lag`, `disk_cache_fetcher`, `rescue`, the tests, and the
//! `endpoint_ffmpeg_impl` trait impls) keep resolving unchanged. Pure move —
//! no logic change.

use std::sync::Arc;

use async_trait::async_trait;
use rs_ffmpeg::ServiceType;

/// A chunk that has been fetched from S3 and is ready for the consumer.
pub(crate) struct PrefetchedChunk {
    pub(crate) chunk_id: i64,
    pub(crate) data: Vec<u8>,
    pub(crate) duration_ms: i64,
}

/// Trait for fetching chunks (S3 or mock).
pub trait ChunkFetcher: Send + Sync {
    fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<(Vec<u8>, i64)>, String>> + Send;

    fn chunk_duration_ms(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<i64>, String>> + Send;
}

// Blanket impl so `endpoint_loop` can wrap the incoming fetcher in an `Arc`
// once and hand a fresh clone to each producer (re)spawn (C3 / #237). The
// trait's methods all take `&self`, so delegating through the `Arc` is a
// zero-cost forward. Without this the producer-respawn would need `F: Clone`
// (which `DiskCacheFetcher` cannot derive — it owns an `AtomicBool` +
// `RateLimiter`); `Arc<F>` sidesteps that with no behaviour change.
impl<T: ChunkFetcher> ChunkFetcher for Arc<T> {
    fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<(Vec<u8>, i64)>, String>> + Send {
        (**self).fetch_chunk_with_meta(chunk_id)
    }

    fn chunk_duration_ms(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<i64>, String>> + Send {
        (**self).chunk_duration_ms(chunk_id)
    }
}

/// Trait for output process (ffmpeg or mock).
/// Uses async_trait for object safety (Box<dyn OutputProcess>).
#[async_trait]
pub trait OutputProcess: Send {
    fn is_alive(&mut self) -> bool;
    async fn write(&mut self, data: &[u8]) -> Result<(), String>;
    async fn kill(&mut self);
    fn last_stderr_line(&self) -> Option<String>;
}

/// Factory for spawning output processes.
pub trait OutputProcessFactory: Send + Sync {
    fn spawn(
        &self,
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String>;
}
