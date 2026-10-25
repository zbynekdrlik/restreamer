//! #424: the #296 slow refill must measure the cushion the viewers are
//! actually behind (the consumer's last pushed chunk) and must slow the
//! pusher's pacing, not sleep between chunks.
//!
//! In production both went wrong at once: the producer measured from its own
//! read pointer, ~11 chunks ahead in the prefetch channel, so every buffered
//! endpoint reported a false ~23 s deficit for the whole event; and the
//! consumer's sleep was caught up by the pusher's wall-clock pacing, so the
//! false deficit never slowed anything.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rs_core::audit::Action;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::Instant;

use super::super::consumer_helpers::{Pushable, maybe_refill_throttle};
use super::tests::TimedMockFetcher;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_producer::producer_task;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::{PREFETCH_BUFFER_SIZE, PrefetchedChunk};
use rs_rtmp_push::PushError;

/// Records every `slow_pacing` call; pushes nothing.
#[derive(Default)]
struct PacingRecorder {
    slowed: Vec<Duration>,
}

impl Pushable for PacingRecorder {
    async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
        Ok(())
    }
    async fn close(&mut self) {}
    fn reconnect_count(&self) -> u32 {
        0
    }
    fn av_skew_ms(&self) -> i64 {
        0
    }
    fn slow_pacing(&mut self, by: Duration) {
        self.slowed.push(by);
    }
    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

fn state_with_deficit(deficit_secs: u64) -> Arc<BufferState> {
    let state = Arc::new(BufferState::new());
    state
        .refill_deficit_secs
        .store(deficit_secs, Ordering::Relaxed);
    state
}

fn new_stats() -> Stats {
    Arc::new(Mutex::new(EndpointStats::default()))
}

#[tokio::test(start_paused = true)]
async fn refill_slows_the_pusher_instead_of_sleeping() {
    let state = state_with_deficit(23);
    let stats = new_stats();
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let mut pusher = PacingRecorder::default();

    let started = Instant::now();
    maybe_refill_throttle(
        false,
        &state,
        2_000,
        &stats,
        Some(&mut pusher),
        &mut stop_rx,
    )
    .await;

    assert_eq!(
        started.elapsed(),
        Duration::ZERO,
        "a sleep here is caught up by the pusher's pacing"
    );
    let expected = Duration::from_millis(crate::refill::refill_throttle_ms(23, 2_000));
    assert!(expected > Duration::ZERO);
    assert_eq!(pusher.slowed, vec![expected]);
    assert_eq!(stats.lock().await.delivery_mode, "refilling");
}

/// The legacy ffmpeg path has no rust pusher: it keeps the sleep.
#[tokio::test(start_paused = true)]
async fn refill_without_a_rust_pusher_sleeps() {
    let state = state_with_deficit(23);
    let stats = new_stats();
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    let started = Instant::now();
    maybe_refill_throttle(
        false,
        &state,
        2_000,
        &stats,
        None::<&mut PacingRecorder>,
        &mut stop_rx,
    )
    .await;

    let expected = Duration::from_millis(crate::refill::refill_throttle_ms(23, 2_000));
    assert_eq!(started.elapsed(), expected);
}

#[tokio::test(start_paused = true)]
async fn no_deficit_no_slowdown() {
    let state = state_with_deficit(0);
    let stats = new_stats();
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let mut pusher = PacingRecorder::default();

    maybe_refill_throttle(
        false,
        &state,
        2_000,
        &stats,
        Some(&mut pusher),
        &mut stop_rx,
    )
    .await;

    assert!(pusher.slowed.is_empty());
    assert_ne!(stats.lock().await.delivery_mode, "refilling");
}

#[tokio::test(start_paused = true)]
async fn a_fast_endpoint_never_refills() {
    let state = state_with_deficit(23);
    let stats = new_stats();
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let mut pusher = PacingRecorder::default();

    maybe_refill_throttle(true, &state, 2_000, &stats, Some(&mut pusher), &mut stop_rx).await;

    assert!(pusher.slowed.is_empty());
    assert_ne!(stats.lock().await.delivery_mode, "refilling");
}

/// A live source one chunk per second, a 40 s cushion exactly at target, and
/// a consumer pushing one chunk per second. The producer reads ~11 chunks
/// ahead into the prefetch channel; that is not a drained cushion, so no
/// refill may start.
#[tokio::test(start_paused = true)]
async fn the_prefetch_queue_is_not_a_refill_deficit() {
    const TARGET_CHUNKS: i64 = 40;
    const START: i64 = 1;
    let chunks: Vec<(i64, Vec<u8>)> = (1..=400).map(|i| (i, vec![0u8; 1])).collect();
    let fetcher = TimedMockFetcher::new(chunks, START + TARGET_CHUNKS).with_chunk_duration(1000);
    let live_edge = fetcher.available_up_to();
    let max_fetched = fetcher.max_fetched_id();

    let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);
    let (stop_tx, stop_rx) = watch::channel(false);
    let stats = new_stats();
    stats.lock().await.current_chunk_id = START;
    let buffer_state = Arc::new(BufferState::new());
    let ring = AuditRing::new(500);

    let producer = tokio::spawn(producer_task(
        fetcher,
        tx,
        START,
        (TARGET_CHUNKS as u64) * 1000,
        false,
        stop_rx,
        stats.clone(),
        "buffered-ep".to_string(),
        buffer_state.clone(),
        Some(ring.clone()),
    ));

    // The source adds a chunk every second and the consumer pushes one.
    let consumer_stats = stats.clone();
    let consumer = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            live_edge.fetch_add(1, Ordering::Relaxed);
            let Some(chunk) = rx.recv().await else { break };
            consumer_stats.lock().await.current_chunk_id = chunk.chunk_id;
        }
    });

    tokio::time::sleep(Duration::from_secs(150)).await;
    let _ = stop_tx.send(true);
    consumer.abort();
    let _ = producer.await;

    let read = max_fetched.load(Ordering::Relaxed);
    assert!(
        read >= 4 * crate::producer_lag::LAG_PROBE_INTERVAL_ITERS as i64,
        "the producer must have read enough chunks to run the refill probe a \
         few times (read up to {read})"
    );
    let delivered = stats.lock().await.current_chunk_id;
    assert!(
        read > delivered + 5,
        "the producer must be ahead of the consumer (read {read}, pushed {delivered})"
    );
    let (rows, _) = ring.since(0);
    let started: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::BufferRefillStarted)
        .map(|r| r.detail.clone())
        .collect();
    assert!(
        started.is_empty(),
        "the cushion was at target, but a refill started: {started:?}"
    );
    assert_eq!(buffer_state.refill_deficit_secs.load(Ordering::Relaxed), 0);
}
