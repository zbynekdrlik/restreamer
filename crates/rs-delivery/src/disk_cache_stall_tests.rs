//! #284 / #280 — producer stall through the REAL disk-cache fetch path.
//!
//! The Group B rescue tests (`rescue_behavioral_tests.rs`) drive
//! `endpoint_loop` with a mock `ChunkFetcher` that RETURNS `Err`/`Ok(None)`
//! directly — so they could never catch the #284 wedge, which lives BELOW
//! that abstraction: `DiskCacheFetcher::fetch_chunk_with_meta` awaited
//! `DownloadService::request_chunk()` with no bound, and `fetch_with_retry`
//! retries transient S3 errors forever with the registry slot stuck
//! `InFlight`. On a persistently-erroring S3 the producer parked inside a
//! single fetch, `producer_active` never flipped, the consumer's rescue gate
//! (`!producer_active`) stayed shut, and every endpoint went dark with no
//! audit row — precisely the #280 operator incident (chunks ran out, no
//! rescue, no preview).
//!
//! These tests run the REAL `DiskCacheFetcher` + `DownloadService` +
//! `ChunkRegistry` over mock S3 *backends* (mocking the external network
//! service only, per test-strictness):
//!
//! - RED on pre-fix code: `erroring_s3_fetch_surfaces_err_within_stall_budget`,
//!   `erroring_s3_flips_producer_active_within_bounded_window`,
//!   `erroring_s3_activates_rescue_through_real_disk_cache_path` all hang past
//!   any bounded budget (the wedge) and fail.
//! - `genuine_exhaustion_404_activates_rescue_through_real_disk_cache_path`
//!   locks the already-working clean-404 drain (the operator's "source died,
//!   uploads stopped" shape) against regression via the same real path.
//!
//! All waits use tokio virtual time — no real sleeps.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::time::Duration;

use tokio::sync::{Mutex, mpsc, watch};

use rs_core::audit::Action;
use rs_core::models::PusherKind;

use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::disk_cache::{DiskCache, DiskCacheConfig, FetchedChunk, S3Backend};
use crate::disk_cache_fetcher::DiskCacheFetcher;
use crate::endpoint_producer::producer_task;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::{ChunkFetcher, endpoint_loop};

use super::tests::MockProcessFactory;

/// Short stall budget so the bounded-wait assertions run in tight virtual
/// time. Production uses 60s (`EndpointHandle::spawn`); the invariants under
/// test are budget-relative, not tied to the absolute value.
const STALL_TIMEOUT_SECS: u64 = 5;

// ---------------------------------------------------------------------------
// Mock S3 *backends* (the external network boundary — everything above them
// is the real production code path).
// ---------------------------------------------------------------------------

/// S3 backend that errors on EVERY request — models a persistently-erroring
/// S3 (5xx storm, connection resets, DNS failure). This is the #284 wedge
/// shape: `fetch_with_retry` used to loop on it forever.
#[derive(Default)]
struct ErroringBackend {
    fetches: AtomicU32,
}

#[async_trait::async_trait]
impl S3Backend for ErroringBackend {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        self.fetches.fetch_add(1, AtomicOrdering::SeqCst);
        Err(format!("forced persistent S3 error on chunk {chunk_id}"))
    }
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        // The skip-ahead / lag probe cannot escape the outage either.
        Err(format!(
            "forced persistent S3 HEAD error on chunk {chunk_id}"
        ))
    }
}

/// Serves chunks `[1, available_up_to]`, then errors forever — "stream ran,
/// then S3 broke" (the FB-soak wedge signature).
struct ServeThenErrorBackend {
    available_up_to: i64,
}

#[async_trait::async_trait]
impl S3Backend for ServeThenErrorBackend {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        if chunk_id <= self.available_up_to {
            return Ok(Some(FetchedChunk {
                data: vec![0u8; 64],
                duration_ms: 1000,
                host_emit_ts: None,
                s3_upload_complete_ts: None,
            }));
        }
        Err(format!("forced persistent S3 error on chunk {chunk_id}"))
    }
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if chunk_id <= self.available_up_to {
            return Ok(Some(1000));
        }
        Err(format!(
            "forced persistent S3 HEAD error on chunk {chunk_id}"
        ))
    }
}

/// Serves chunks `[1, available_up_to]`, then clean 404s — genuine chunk
/// exhaustion (source dead, uploads stopped, S3 healthy): the #280 operator
/// scenario.
struct ExhaustingBackend {
    available_up_to: i64,
}

#[async_trait::async_trait]
impl S3Backend for ExhaustingBackend {
    async fn fetch(&self, chunk_id: i64) -> Result<Option<FetchedChunk>, String> {
        if chunk_id <= self.available_up_to {
            return Ok(Some(FetchedChunk {
                data: vec![0u8; 64],
                duration_ms: 1000,
                host_emit_ts: None,
                s3_upload_complete_ts: None,
            }));
        }
        Ok(None)
    }
    async fn head_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        if chunk_id <= self.available_up_to {
            return Ok(Some(1000));
        }
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Shared harness
// ---------------------------------------------------------------------------

/// REAL `DiskCache` (registry + download service + eviction) over the given
/// backend, plus the REAL `DiskCacheFetcher` — the exact production wiring
/// from `EndpointHandle::spawn`, with a tight stall budget.
async fn real_fetcher(
    backend: Arc<dyn S3Backend>,
    tmp: &tempfile::TempDir,
    alias: &str,
) -> DiskCacheFetcher {
    let cfg = DiskCacheConfig {
        cache_dir: tmp.path().to_path_buf(),
        window_chunks: 4,
        s3_ingress_cap_mbit: 10_000,  // never bandwidth-limited in tests
        eviction_interval_secs: 3600, // keep the sweeper quiet under virtual time
        read_stall_timeout_secs: STALL_TIMEOUT_SECS,
        download_queue_capacity: 50,
    };
    let cache = Arc::new(
        DiskCache::new(cfg, backend, "stall-evt".to_string(), None)
            .await
            .expect("DiskCache::new"),
    );
    DiskCacheFetcher::new(cache, alias.to_string(), 1, 4, STALL_TIMEOUT_SECS, None)
}

fn ep_cfg(alias: &str) -> EndpointConfig {
    EndpointConfig {
        alias: alias.to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher: PusherKind::Ffmpeg,
    }
}

fn rescue_activated(ring: &Arc<AuditRing>) -> bool {
    let (rows, _) = ring.since(0);
    rows.iter().any(|r| r.action == Action::RescueActivated)
}

/// Drive `endpoint_loop` over the REAL disk-cache path until rescue activates
/// or a generous virtual-time budget elapses.
async fn run_real_cache_until_rescue(
    backend: Arc<dyn S3Backend>,
    alias: &str,
) -> (Arc<AuditRing>, Stats) {
    tokio::time::pause();
    let tmp = tempfile::tempdir().expect("tempdir");
    let fetcher = real_fetcher(backend, &tmp, alias).await;

    let ring = AuditRing::new(500);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let buffer_state = Arc::new(BufferState::new());
    let (stop_tx, stop_rx) = watch::channel(false);

    let stats_clone = stats.clone();
    let ring_clone = ring.clone();
    let cfg = ep_cfg(alias);
    let handle = tokio::spawn(async move {
        endpoint_loop(
            fetcher,
            MockProcessFactory::new(),
            cfg,
            1,
            0, // no warmup — straight to the pipeline
            stop_rx,
            stats_clone,
            None,
            buffer_state,
            Some(ring_clone),
        )
        .await;
    });

    // Budget: serve the available chunks, hit the outage, let the bounded
    // fetch surface it (attempt cap + stall timeout), let the producer flip
    // after >=3 failures, let the 8s rescue threshold elapse. 600 virtual
    // seconds is an order of magnitude above the whole chain.
    for _ in 0..6000 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        if rescue_activated(&ring) {
            break;
        }
    }

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
    // `tmp` dropped here — after the endpoint task has been joined/aborted.
    (ring, stats)
}

// ---------------------------------------------------------------------------
// RED tests (#284): pre-fix, every one of these hangs past its bounded budget.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn erroring_s3_fetch_surfaces_err_within_stall_budget() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let backend = Arc::new(ErroringBackend::default());
    let fetcher = real_fetcher(backend.clone(), &tmp, "stall-fetch").await;

    // 4x the stall budget: far above the bound the fetcher must honor, far
    // below "forever". Pre-fix, request_chunk().await never returns (the
    // download task retries the erroring backend forever with the registry
    // slot stuck InFlight), so this outer timeout fires.
    let budget = Duration::from_secs(STALL_TIMEOUT_SECS * 4);
    match tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(1)).await {
        Ok(Err(e)) => {
            assert!(
                !e.is_empty(),
                "stall error must carry a diagnosable message"
            );
        }
        Ok(Ok(got)) => panic!("erroring backend can never yield a chunk, got {got:?}"),
        Err(_) => panic!(
            "#284 REGRESSION: fetch_chunk_with_meta stalled past {}s on a \
             persistently-erroring S3. The producer parks inside \
             request_chunk().await, producer_active never flips, and rescue \
             never fires — every endpoint goes dark (#280).",
            budget.as_secs()
        ),
    }
    assert!(
        backend.fetches.load(AtomicOrdering::SeqCst) > 0,
        "the real DownloadService must have actually hit the backend"
    );
}

#[tokio::test(start_paused = true)]
async fn erroring_s3_flips_producer_active_within_bounded_window() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let backend = Arc::new(ErroringBackend::default());
    let fetcher = real_fetcher(backend, &tmp, "stall-producer").await;

    let buffer_state = Arc::new(BufferState::new());
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    // Keep the receiver alive: a dropped rx would stop the producer early.
    let (tx, _rx) = mpsc::channel::<crate::endpoint_task::PrefetchedChunk>(4);
    let (stop_tx, stop_rx) = watch::channel(false);

    let bs = buffer_state.clone();
    let task = tokio::spawn(producer_task(
        fetcher,
        tx,
        1,
        0,
        false,
        stop_rx,
        stats,
        "stall-producer".to_string(),
        bs,
        None,
    ));

    // 120 virtual seconds — an order of magnitude above the bounded flip
    // budget (3 bounded fetch failures + producer backoffs). Pre-fix the
    // producer never returns from its FIRST fetch, so the flag never flips.
    let mut flipped = false;
    for _ in 0..240 {
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        if !buffer_state.producer_active.load(AtomicOrdering::Relaxed) {
            flipped = true;
            break;
        }
    }
    assert!(
        flipped,
        "#284 REGRESSION: producer_active never flipped to false within 120s \
         of a persistently-erroring S3 — the rescue gate (!producer_active) \
         stays shut and every endpoint goes dark (#280)"
    );

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
}

#[tokio::test]
async fn erroring_s3_activates_rescue_through_real_disk_cache_path() {
    // End-to-end #280 lock, error-shaped outage: 3 chunks flow, then S3
    // errors persistently. Rescue MUST activate through the REAL
    // DiskCacheFetcher path (the mock-fetcher Group B equivalent passed
    // while prod was broken — the wedge is below the mock).
    let (ring, stats) = run_real_cache_until_rescue(
        Arc::new(ServeThenErrorBackend { available_up_to: 3 }),
        "real-cache-err",
    )
    .await;

    assert!(
        rescue_activated(&ring),
        "#284/#280 REGRESSION: RescueActivated never emitted on an \
         error-shaped S3 outage through the real disk-cache path"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "rescue",
        "delivery_mode must read 'rescue' during the outage, got {:?}",
        s.delivery_mode
    );
}

// ---------------------------------------------------------------------------
// Regression lock: the clean-404 exhaustion drain (works pre-fix) must keep
// working through the same real path after the fix.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn genuine_exhaustion_404_activates_rescue_through_real_disk_cache_path() {
    // The #280 operator scenario shape: source dies, uploads stop, S3 is
    // healthy and clean-404s past the last chunk. The VPS must exhaust and
    // rescue — through the REAL DiskCacheFetcher + DownloadService path.
    let (ring, stats) = run_real_cache_until_rescue(
        Arc::new(ExhaustingBackend { available_up_to: 3 }),
        "real-cache-404",
    )
    .await;

    assert!(
        rescue_activated(&ring),
        "genuine chunk exhaustion (clean 404 past the last chunk) must \
         activate rescue through the real disk-cache path"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "rescue",
        "delivery_mode must read 'rescue' after exhaustion, got {:?}",
        s.delivery_mode
    );
}
