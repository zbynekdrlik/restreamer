//! #408 — the end of the source through the real keepalive bridge and the
//! real `endpoint_loop`.
//!
//! A deliberate OBS stop is announced as the last chunk of the source. An
//! endpoint plays out to it and stops sending: no keepalive freeze, no
//! rescue clip, no `RescueActivated` (the outage alert). A chunk after the
//! end resumes it. Without an announcement, or before the endpoint reached
//! the end, a stalled source still escalates to rescue exactly as before.
//!
//! Virtual time only (`tokio::time::pause` / `start_paused`).

use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use rs_core::audit::Action;
use rs_core::models::PusherKind;
use rs_rtmp_push::PushError;
use tokio::sync::{Mutex, mpsc, watch};

use super::super::consumer_helpers::Pushable;
use super::super::{KeepaliveOutcome, keepalive_until_chunk};
use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::{PrefetchedChunk, endpoint_loop};

use super::tests::MockProcessFactory;

/// Records every push; never fails.
struct Recording {
    pushes: Arc<StdMutex<usize>>,
}

impl Pushable for Recording {
    async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
        *self.pushes.lock().unwrap() += 1;
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok(())
    }
    async fn close(&mut self) {}
    fn reconnect_count(&self) -> u32 {
        0
    }
    fn av_skew_ms(&self) -> i64 {
        0
    }
    async fn finish(&mut self) {
        self.close().await
    }
    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

fn actions(ring: &Arc<AuditRing>) -> Vec<Action> {
    ring.since(0).0.into_iter().map(|r| r.action).collect()
}

/// Runs the bridge for a fast endpoint whose producer is stalled and whose
/// last delivered chunk is 3. `announce` is applied after `after`.
async fn bridge(
    announce: Option<i64>,
    after: Duration,
) -> (KeepaliveOutcome, Arc<AuditRing>, usize) {
    let pushes = Arc::new(StdMutex::new(0));
    let mut pusher = Recording {
        pushes: Arc::clone(&pushes),
    };
    let (_tx, mut rx) = mpsc::channel::<PrefetchedChunk>(4);
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let ring = AuditRing::new(100);
    let audit = Some(Arc::clone(&ring));
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let bs = Arc::new(BufferState::new());
    bs.producer_active.store(false, Ordering::Relaxed);
    let last: Option<Arc<Vec<u8>>> = Some(Arc::new(vec![0u8; 64]));
    let bs_task = Arc::clone(&bs);
    let task = tokio::spawn(async move {
        keepalive_until_chunk(
            &mut pusher,
            &mut rx,
            &last,
            "ep",
            &audit,
            &mut stop_rx,
            &stats,
            &bs_task,
            3,
            Duration::from_secs(8),
        )
        .await
    });
    tokio::time::sleep(after).await;
    if let Some(end) = announce {
        assert!(bs.source_end.announce(end));
    }
    let outcome = tokio::time::timeout(Duration::from_secs(60), task)
        .await
        .expect("the bridge must end")
        .unwrap();
    let n = *pushes.lock().unwrap();
    (outcome, ring, n)
}

#[tokio::test(start_paused = true)]
async fn an_end_announced_during_the_gap_ends_the_bridge_before_rescue() {
    let (outcome, ring, _) = bridge(Some(3), Duration::from_secs(3)).await;
    assert!(
        matches!(outcome, KeepaliveOutcome::SourceEnded(3)),
        "expected SourceEnded(3), the bridge must never escalate past an announced end"
    );
    assert_eq!(
        actions(&ring),
        vec![Action::FastKeepaliveStarted, Action::FastKeepaliveEnded],
        "the keepalive episode is closed; no rescue"
    );
}

#[tokio::test(start_paused = true)]
async fn a_reached_end_skips_the_bridge_entirely() {
    let pushes = Arc::new(StdMutex::new(0));
    let mut pusher = Recording {
        pushes: Arc::clone(&pushes),
    };
    let (_tx, mut rx) = mpsc::channel::<PrefetchedChunk>(4);
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let ring = AuditRing::new(100);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let bs = Arc::new(BufferState::new());
    assert!(bs.source_end.announce(3));
    // Bounded: a bridge that missed the end would freeze-push forever (the
    // producer is active, so it never escalates either).
    let outcome = tokio::time::timeout(
        Duration::from_secs(60),
        keepalive_until_chunk(
            &mut pusher,
            &mut rx,
            &Some(Arc::new(vec![0u8; 64])),
            "ep",
            &Some(Arc::clone(&ring)),
            &mut stop_rx,
            &stats,
            &bs,
            4,
            Duration::from_secs(8),
        ),
    )
    .await
    .expect("a reached end must end the bridge at once");
    assert!(matches!(outcome, KeepaliveOutcome::SourceEnded(3)));
    assert_eq!(*pushes.lock().unwrap(), 0, "no freeze frame after the end");
    assert!(actions(&ring).is_empty(), "no keepalive episode at all");
    assert_eq!(stats.lock().await.delivery_mode, "normal", "untouched");
}

#[tokio::test(start_paused = true)]
async fn an_end_the_endpoint_has_not_reached_still_escalates_a_stall() {
    // Chunks 4 and 5 are announced but never arrive: that is an outage.
    let (outcome, ring, pushes) = bridge(Some(5), Duration::from_secs(1)).await;
    assert!(matches!(outcome, KeepaliveOutcome::EscalateToRescue));
    assert!(pushes > 0, "freeze frames bridged the gap");
    assert_eq!(
        actions(&ring),
        vec![Action::FastKeepaliveStarted, Action::FastKeepaliveEnded]
    );
}

#[tokio::test(start_paused = true)]
async fn without_an_end_a_stall_escalates_as_before() {
    let (outcome, _, _) = bridge(None, Duration::from_secs(1)).await;
    assert!(matches!(outcome, KeepaliveOutcome::EscalateToRescue));
}

// ---------------------------------------------------------------------------
// The real endpoint_loop.
// ---------------------------------------------------------------------------

/// Serves chunks `1..=available` except `hole` (a chunk that was never
/// uploaded), 404 past it; `available` can grow (OBS publishes again).
struct GrowingFetcher {
    available: Arc<AtomicI64>,
    hole: Option<i64>,
}

impl GrowingFetcher {
    fn has(&self, chunk_id: i64) -> bool {
        chunk_id <= self.available.load(Ordering::Relaxed) && self.hole != Some(chunk_id)
    }
}

impl crate::endpoint_task::ChunkFetcher for GrowingFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        Ok(self.has(chunk_id).then(|| (vec![0u8; 64], 2000)))
    }
    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        Ok(self.has(chunk_id).then_some(2000))
    }
}

fn rust_endpoint(alias: &str, is_fast: bool) -> EndpointConfig {
    EndpointConfig {
        alias: alias.to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher: PusherKind::Rust,
        youtube_backup_ingest: false,
    }
}

/// Advance virtual time in 100 ms steps until `done` or ~1000 s.
async fn until(mut done: impl FnMut() -> bool) -> bool {
    for _ in 0..10_000 {
        if done() {
            return true;
        }
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    done()
}

async fn ended_then_resumed(is_fast: bool, hole: Option<i64>) {
    tokio::time::pause();
    let ring = AuditRing::new(2000);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let bs = Arc::new(BufferState::new());
    // OBS was stopped on purpose after chunk 3; the host announced it.
    assert!(bs.source_end.announce(3));
    let available = Arc::new(AtomicI64::new(3));
    let (stop_tx, stop_rx) = watch::channel(false);
    let handle = tokio::spawn(endpoint_loop(
        GrowingFetcher {
            available: Arc::clone(&available),
            hole,
        },
        MockProcessFactory::new(),
        rust_endpoint(if is_fast { "fast" } else { "buffered" }, is_fast),
        1,
        0,
        stop_rx,
        stats.clone(),
        None,
        Arc::clone(&bs),
        Some(Arc::clone(&ring)),
    ));

    assert!(
        until(|| actions(&ring).contains(&Action::EndpointSourceEnded)).await,
        "the endpoint must end after playing out chunk 3: {:?}",
        actions(&ring)
    );
    assert_eq!(stats.lock().await.delivery_mode, "ended");

    // Long after the end: still ended, never rescue, never a chunk_gap stall
    // (the producer passed 40 misses here). The refused TEST_FILE pushes
    // leave their own stall reason; only the producer's gap verdict counts.
    for _ in 0..2000 {
        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
    }
    {
        let s = stats.lock().await;
        assert_eq!(s.delivery_mode, "ended");
        assert_ne!(
            s.stall_reason.as_deref(),
            Some("chunk_gap"),
            "the end of the source is no gap"
        );
    }
    assert!(!actions(&ring).contains(&Action::RescueActivated));

    // OBS publishes again: the next chunk (4, or 5 past a hole) resumes it.
    available.store(6, Ordering::Relaxed);
    assert!(
        until(|| actions(&ring).contains(&Action::EndpointSourceResumed)).await,
        "the endpoint must resume on chunk 4: {:?}",
        actions(&ring)
    );
    assert_eq!(bs.source_end.ended_at(), None, "the end is cleared");
    assert_ne!(stats.lock().await.delivery_mode, "ended");
    assert!(!actions(&ring).contains(&Action::RescueActivated));

    let _ = stop_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
}

#[tokio::test]
async fn a_buffered_endpoint_plays_out_ends_and_resumes_without_rescue() {
    ended_then_resumed(false, None).await;
}

#[tokio::test]
async fn a_fast_endpoint_plays_out_ends_and_resumes_without_rescue() {
    ended_then_resumed(true, None).await;
}

/// The chunk after the end never reached S3 (an abandoned upload): the
/// producer's probe finds the chunks after it, so the endpoints still come
/// back when OBS publishes again (review of #408).
#[tokio::test]
async fn a_lost_chunk_after_the_end_never_strands_the_endpoints() {
    ended_then_resumed(false, Some(4)).await;
}
