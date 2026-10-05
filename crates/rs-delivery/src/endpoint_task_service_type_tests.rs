//! #192 ROZHODNUTE item 1 (issuecomment-5994508052): an endpoint whose
//! service type is UNKNOWN must refuse to start, loudly.
//!
//! Before this, `parse().unwrap_or(ServiceType::TestFile)` routed an unknown
//! type into the TEST_FILE loopback discard sink (`test_file_sink`, #192),
//! so the endpoint pushed its warmup rescue clip into a black hole while
//! looking alive, and then its consumer exited with only a log line: no
//! `last_error` / `stall_reason` in the VPS status, no audit row for the host.
//!
//! These tests drive the REAL `endpoint_loop` with service type "BOGUS" on
//! both start paths (warmup, and the no-delay fast path) and assert the
//! refusal: nothing fetched, nothing spawned, the reason in the endpoint
//! stats, and the start-failure audit row.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use rs_core::audit::{Action, Severity};
use rs_core::models::PusherKind;
use rs_ffmpeg::ServiceType;
use tokio::sync::{Mutex, watch};

use crate::api::EndpointConfig;
use crate::audit_ring::{AuditRing, RingRow};
use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::{OutputProcess, OutputProcessFactory, endpoint_loop};

/// Counts every S3 access. Serves chunks so a start that DOES proceed has
/// everything it needs (and the count shows it).
struct CountingFetcher {
    calls: Arc<AtomicU32>,
}

impl crate::endpoint_task::ChunkFetcher for CountingFetcher {
    async fn fetch_chunk_with_meta(
        &self,
        _chunk_id: i64,
    ) -> Result<Option<(Vec<u8>, i64)>, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some((vec![0u8; 64], 2_000)))
    }

    async fn chunk_duration_ms(&self, _chunk_id: i64) -> Result<Option<i64>, String> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(2_000))
    }
}

/// Counts every ffmpeg spawn attempt and refuses it.
struct CountingFactory {
    spawns: Arc<AtomicU32>,
}

impl OutputProcessFactory for CountingFactory {
    fn spawn(
        &self,
        _service_type: ServiceType,
        _stream_key: &str,
        _alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String> {
        self.spawns.fetch_add(1, Ordering::Relaxed);
        Err("counting factory never spawns".to_string())
    }
}

fn bogus_cfg(alias: &str, is_fast: bool, pusher: PusherKind) -> EndpointConfig {
    EndpointConfig {
        alias: alias.to_string(),
        service_type: "BOGUS".to_string(),
        stream_key: "bogus-key".to_string(),
        is_fast,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher,
    }
}

struct Outcome {
    stats: EndpointStats,
    rows: Vec<RingRow>,
    fetches: u32,
    spawns: u32,
}

/// Run `endpoint_loop` to completion; an endpoint that refuses to start
/// returns at once, so a 10 s budget is only a hang guard.
async fn run(cfg: EndpointConfig, delivery_delay_ms: u64) -> Outcome {
    let ring = AuditRing::new(100);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let calls = Arc::new(AtomicU32::new(0));
    let spawns = Arc::new(AtomicU32::new(0));
    let (_stop_tx, stop_rx) = watch::channel(false);

    let finished = tokio::time::timeout(
        Duration::from_secs(10),
        endpoint_loop(
            CountingFetcher {
                calls: calls.clone(),
            },
            CountingFactory {
                spawns: spawns.clone(),
            },
            cfg,
            1,
            delivery_delay_ms,
            stop_rx,
            stats.clone(),
            None,
            Arc::new(BufferState::new()),
            Some(ring.clone()),
        ),
    )
    .await;
    assert!(
        finished.is_ok(),
        "an endpoint with an unknown service type must refuse to start, not keep running"
    );

    let stats = stats.lock().await.clone();
    let (rows, _) = ring.since(0);
    Outcome {
        stats,
        rows,
        fetches: calls.load(Ordering::Relaxed),
        spawns: spawns.load(Ordering::Relaxed),
    }
}

fn assert_refused_loudly(o: &Outcome, alias: &str) {
    assert_eq!(
        o.fetches, 0,
        "a refused endpoint must not touch S3 (no warmup probe, no producer)"
    );
    assert_eq!(o.spawns, 0, "a refused endpoint must not spawn ffmpeg");
    let last_error = o.stats.last_error.as_deref().unwrap_or("");
    assert!(
        last_error.contains("BOGUS"),
        "VPS status must name the unknown service type, got last_error={:?}",
        o.stats.last_error
    );
    assert_eq!(
        o.stats.stall_reason.as_deref(),
        Some("unknown_service_type"),
        "VPS status must carry the refusal reason"
    );
    let refusals: Vec<&RingRow> = o
        .rows
        .iter()
        .filter(|r| {
            r.action == Action::EndpointFfmpegRestartFailed
                && r.detail.get("phase").and_then(|v| v.as_str()) == Some("service_type")
        })
        .collect();
    assert_eq!(
        refusals.len(),
        1,
        "exactly one start-failure audit row for the refusal, got {:?}",
        o.rows
    );
    let row = refusals[0];
    assert_eq!(row.severity, Severity::Error);
    assert_eq!(row.endpoint.as_deref(), Some(alias));
    assert_eq!(
        row.detail.get("service_type").and_then(|v| v.as_str()),
        Some("BOGUS")
    );
    assert!(
        row.detail
            .get("error")
            .and_then(|v| v.as_str())
            .is_some_and(|e| e.contains("BOGUS")),
        "the audit row carries the parse error: {}",
        row.detail
    );
}

/// The warmup path: a non-fast endpoint with a delivery delay used to push
/// its rescue clip into the TEST_FILE sink before the consumer noticed.
#[tokio::test]
async fn unknown_service_type_refuses_to_start_on_the_warmup_path() {
    let o = run(bogus_cfg("bogus-warmup", false, PusherKind::Rust), 2_000).await;
    assert_refused_loudly(&o, "bogus-warmup");
}

/// The no-delay path (fast endpoint, ffmpeg pusher): the consumer used to
/// exit with only a log line while the producer was already fetching.
#[tokio::test]
async fn unknown_service_type_refuses_to_start_on_the_fast_path() {
    let o = run(bogus_cfg("bogus-fast", true, PusherKind::Ffmpeg), 0).await;
    assert_refused_loudly(&o, "bogus-fast");
}

/// A KNOWN service type still starts: the refusal is only for unknown ones.
#[tokio::test]
async fn a_known_service_type_still_starts() {
    let ring = AuditRing::new(100);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let calls = Arc::new(AtomicU32::new(0));
    let (stop_tx, stop_rx) = watch::channel(false);
    let cfg = EndpointConfig {
        service_type: "TEST_FILE".to_string(),
        ..bogus_cfg("known", true, PusherKind::Ffmpeg)
    };
    let task = tokio::spawn(endpoint_loop(
        CountingFetcher {
            calls: calls.clone(),
        },
        CountingFactory {
            spawns: Arc::new(AtomicU32::new(0)),
        },
        cfg,
        1,
        0,
        stop_rx,
        stats.clone(),
        None,
        Arc::new(BufferState::new()),
        Some(ring.clone()),
    ));
    // The producer starts fetching as soon as the endpoint starts.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while calls.load(Ordering::Relaxed) == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(10), task).await;
    assert!(
        calls.load(Ordering::Relaxed) > 0,
        "a known service type must start its pipeline"
    );
    assert_ne!(
        stats.lock().await.stall_reason.as_deref(),
        Some("unknown_service_type")
    );
    let (rows, _) = ring.since(0);
    assert!(
        !rows
            .iter()
            .any(|r| r.detail.get("phase").and_then(|v| v.as_str()) == Some("service_type")),
        "no refusal row for a known service type"
    );
}
