//! `endpoint_task` integration-ish tests.
//!
//! #212 removed the ffmpeg-subprocess output path and its `OutputProcessFactory`
//! injection point, so the consumer now always uses a concrete
//! `rs_rtmp_push::RtmpPusher`. Tests that asserted on delivered chunk counts /
//! mock-process writes / ffmpeg restart-backoff were dropped (they exercised
//! the removed path and can no longer inject a mock output). Producer-side
//! behaviour is covered by `fast_self_healing_tests` (drives `producer_task`
//! directly), rescue activation by `rescue_endpoint_loop_tests` /
//! `disk_cache_stall_tests`, and the rust pusher by `endpoint_task_rust_push_tests`
//! / `endpoint_task_dead_target_tests`. Re-adding end-to-end delivery coverage
//! via a `Pushable` injection point is tracked as a #212 follow-up.
//!
//! The shared `MockFetcher` / `TimedMockFetcher` / `test_ep_cfg` fixtures are
//! kept here (`pub(crate)`) because `fast_self_healing_tests` reuses them.

use super::super::*;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::Mutex;
use tokio::sync::Mutex as TokioMutex;

pub(crate) struct MockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    duration_ms_per_chunk: i64,
}

impl MockFetcher {
    pub(crate) fn new(chunks: Vec<(i64, Vec<u8>)>) -> Self {
        Self {
            chunks: Arc::new(TokioMutex::new(chunks.into_iter().collect())),
            duration_ms_per_chunk: 20,
        }
    }
}

impl ChunkFetcher for MockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        let map = self.chunks.lock().await;
        Ok(map
            .get(&chunk_id)
            .map(|data| (data.clone(), self.duration_ms_per_chunk)))
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        let map = self.chunks.lock().await;
        if map.contains_key(&chunk_id) {
            Ok(Some(self.duration_ms_per_chunk))
        } else {
            Ok(None)
        }
    }
}

// `pub(crate)` so the sibling `fast_self_healing_tests` module can build the
// same default endpoint config for the moved tests.
pub(crate) fn test_ep_cfg() -> EndpointConfig {
    EndpointConfig {
        alias: "test-ep".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
    }
}

#[tokio::test]
async fn test_stops_on_signal() {
    tokio::time::pause();
    let fetcher = MockFetcher::new(vec![]);

    let (stop_tx, stop_rx) = watch::channel(false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

    let stats_clone = stats.clone();
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            test_ep_cfg(),
            1,
            0,
            stop_rx,
            stats_clone,
            None,
            Arc::new(BufferState::new()),
            None,
        )
        .await;
    });

    tokio::time::advance(std::time::Duration::from_millis(500)).await;
    tokio::task::yield_now().await;
    let _ = stop_tx.send(true);

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
    assert!(result.is_ok(), "Task should have stopped cleanly");
}

#[tokio::test]
async fn test_stats_struct_serializes() {
    let stats = EndpointStats {
        bytes_processed_total: 1000,
        duration_processed_ms: 20000,
        current_chunk_id: 42,
        chunks_processed: 10,
        ffmpeg_restart_count: 2,
        consecutive_chunk_misses: 5,
        last_error: Some("test error".to_string()),
        stall_reason: Some("chunk_gap".to_string()),
        ffmpeg_last_stderr: Some("connection refused".to_string()),
        delivery_mode: "normal".to_string(),
        ..EndpointStats::default()
    };
    let json = serde_json::to_string(&stats).unwrap();
    assert!(json.contains("\"stall_reason\":\"chunk_gap\""));
    assert!(json.contains("\"ffmpeg_restart_count\":2"));
}

// TimedMockFetcher: chunks available at configured rate.
// 2000ms chunk duration matches buffer-fill/chunk-gap tests.
// `pub(crate)` so the sibling `fast_self_healing_tests` module can reuse it
// for the moved fast-delay / chunk-gap tests.
pub(crate) struct TimedMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    available_up_to: Arc<AtomicI64>,
    duration_ms_per_chunk: i64,
    // Injected per-fetch latency to simulate S3 GET / HEAD slowness. Default
    // ZERO keeps existing tests byte-for-byte identical.
    fetch_latency: std::time::Duration,
    // Highest chunk_id ever requested via fetch_chunk_with_meta. Lets a test
    // observe the producer's READ position without a real consumer. Starts at
    // i64::MIN; updated monotonically. ZERO impact when unread.
    max_fetched_id: Arc<AtomicI64>,
    // Independent ceiling for HEAD probes (chunk_duration_ms / the lag-probe
    // ladder). `None` (default) → HEAD uses `available_up_to`, identical to
    // before. `Some(h)` lets the ladder discover the live edge at `h` while
    // GET stalls at `available_up_to` — modelling "HEAD sees the edge, GET
    // trails behind it" so a test can pin the producer's read position to
    // the lag-probe's jump target without it catching up.
    head_available_up_to: Option<Arc<AtomicI64>>,
}

impl TimedMockFetcher {
    pub(crate) fn new(chunks: Vec<(i64, Vec<u8>)>, initially_available: i64) -> Self {
        Self {
            chunks: Arc::new(TokioMutex::new(chunks.into_iter().collect())),
            available_up_to: Arc::new(AtomicI64::new(initially_available)),
            duration_ms_per_chunk: 2000,
            fetch_latency: std::time::Duration::ZERO,
            max_fetched_id: Arc::new(AtomicI64::new(i64::MIN)),
            head_available_up_to: None,
        }
    }

    /// Let HEAD probes (the lag-probe ladder) reach `head_edge` while GET
    /// fetches still stall at `available_up_to`. Models the producer
    /// discovering the live edge via HEAD but reading behind it. ZERO impact
    /// on callers that don't set it (HEAD falls back to `available_up_to`).
    pub(crate) fn with_head_edge(mut self, head_edge: i64) -> Self {
        self.head_available_up_to = Some(Arc::new(AtomicI64::new(head_edge)));
        self
    }

    pub(crate) fn with_latency(mut self, d: std::time::Duration) -> Self {
        self.fetch_latency = d;
        self
    }

    /// Override the per-chunk media duration the fetcher reports (default
    /// 2000ms). Lets a test pin `typical_chunk_dur_ms` so chunk-count maths
    /// against the adaptive read-delay are deterministic. ZERO impact on
    /// callers that don't use it.
    pub(crate) fn with_chunk_duration(mut self, ms: i64) -> Self {
        self.duration_ms_per_chunk = ms;
        self
    }

    pub(crate) fn available_up_to(&self) -> Arc<AtomicI64> {
        self.available_up_to.clone()
    }

    pub(crate) fn max_fetched_id(&self) -> Arc<AtomicI64> {
        self.max_fetched_id.clone()
    }
}

impl ChunkFetcher for TimedMockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        if !self.fetch_latency.is_zero() {
            tokio::time::sleep(self.fetch_latency).await;
        }
        self.max_fetched_id.fetch_max(chunk_id, Ordering::Relaxed);
        let available = self.available_up_to.load(Ordering::Relaxed);
        if chunk_id > available {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        Ok(map
            .get(&chunk_id)
            .map(|data| (data.clone(), self.duration_ms_per_chunk)))
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if !self.fetch_latency.is_zero() {
            tokio::time::sleep(self.fetch_latency).await;
        }
        // HEAD ceiling: the dedicated head edge when set, else GET's edge.
        let available = match &self.head_available_up_to {
            Some(h) => h.load(Ordering::Relaxed),
            None => self.available_up_to.load(Ordering::Relaxed),
        };
        if chunk_id > available {
            return Ok(None);
        }
        let map = self.chunks.lock().await;
        if map.contains_key(&chunk_id) {
            Ok(Some(self.duration_ms_per_chunk))
        } else {
            Ok(None)
        }
    }
}
