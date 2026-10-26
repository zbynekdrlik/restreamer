//! #424: the cushion deficit is measured at the consumer's last pushed chunk,
//! and it never changes the speed of the outgoing stream.
//!
//! The #296 slow refill (deliver at 0.98x until the cushion is back) never
//! acted in production: the pusher's wall-clock pacing caught up its sleep.
//! #424 made it act, and YouTube flagged every refill window as
//! `videoIngestionStarved` (health bad, PR #425 run 40302038359). Owner
//! decision 2026-10-26: no slowdown or speed-up of the outgoing stream, ever.
//! The deficit stays as information only.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use rs_core::audit::Action;
use rs_core::models::PusherKind;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::time::Instant;

use super::tests::TimedMockFetcher;
use crate::audit_ring::AuditRing;
use crate::buffer_state::BufferState;
use crate::endpoint_producer::producer_task;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::endpoint_task::{
    OutputProcess, OutputProcessFactory, PREFETCH_BUFFER_SIZE, PrefetchedChunk,
};
use rs_ffmpeg::ServiceType;

fn new_stats() -> Stats {
    Arc::new(Mutex::new(EndpointStats::default()))
}

/// Accepts every write and counts it.
struct CountingProcess {
    writes: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait::async_trait]
impl OutputProcess for CountingProcess {
    fn is_alive(&mut self) -> bool {
        true
    }
    async fn write(&mut self, _data: &[u8]) -> Result<(), String> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    async fn kill(&mut self) {}
    fn last_stderr_line(&self) -> Option<String> {
        None
    }
}

struct CountingFactory {
    writes: Arc<std::sync::atomic::AtomicUsize>,
}

impl OutputProcessFactory for CountingFactory {
    fn spawn(&self, _: ServiceType, _: &str, _: &str) -> Result<Box<dyn OutputProcess>, String> {
        Ok(Box::new(CountingProcess {
            writes: self.writes.clone(),
        }))
    }
}

/// What a consumer run showed: how long `CHUNKS` chunks took to write, and
/// whether the dashboard ever read "refilling".
struct ConsumerRun {
    elapsed: Duration,
    showed_refilling: bool,
}

const CHUNKS: usize = 10;

/// Feeds `CHUNKS` buffered 2 s chunks to a consumer on the ffmpeg path, with
/// `deficit_secs` already flagged on the shared state, and times the writes.
async fn run_consumer(deficit_secs: u64) -> ConsumerRun {
    let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let factory = CountingFactory {
        writes: writes.clone(),
    };
    let ep_cfg = crate::api::EndpointConfig {
        alias: "buffered-ep".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "k".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher: PusherKind::Ffmpeg,
        youtube_backup_ingest: false,
    };
    let buffer_state = Arc::new(BufferState::new());
    buffer_state
        .refill_deficit_secs
        .store(deficit_secs, Ordering::Relaxed);
    let stats = new_stats();
    let (tx, rx) = mpsc::channel::<PrefetchedChunk>(CHUNKS);
    let (stop_tx, stop_rx) = watch::channel(false);

    let started = Instant::now();
    let consumer = tokio::spawn(super::super::consumer_task(
        rx,
        factory,
        ep_cfg,
        ServiceType::TestFile,
        0,
        stop_rx,
        stats.clone(),
        None,
        buffer_state,
        None,
    ));
    for chunk_id in 1..=CHUNKS as i64 {
        tx.send(PrefetchedChunk {
            chunk_id,
            data: vec![chunk_id as u8; 64],
            duration_ms: 2_000,
        })
        .await
        .expect("consumer alive");
    }

    let mut showed_refilling = false;
    // A hang bound only: the writes need no wall time at all.
    for _ in 0..100_000 {
        showed_refilling |= stats.lock().await.delivery_mode == "refilling";
        if writes.load(Ordering::Relaxed) >= CHUNKS {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let elapsed = started.elapsed();
    assert!(
        writes.load(Ordering::Relaxed) >= CHUNKS,
        "the consumer wrote only {} of {CHUNKS} chunks",
        writes.load(Ordering::Relaxed)
    );
    let _ = stop_tx.send(true);
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(30), consumer).await;
    ConsumerRun {
        elapsed,
        showed_refilling,
    }
}

/// A cushion below target (23 s, as after an outage) must not change how fast
/// the chunks go out: YouTube reads a slower ingest as starved video.
#[tokio::test(start_paused = true)]
async fn a_cushion_deficit_never_slows_the_push() {
    let at_target = run_consumer(0).await;
    let drained = run_consumer(23).await;
    assert!(
        drained.elapsed <= at_target.elapsed + Duration::from_millis(5),
        "a 23 s deficit slowed the push: {:?} vs {:?} at target",
        drained.elapsed,
        at_target.elapsed
    );
}

/// The dashboard must not claim a refill that does not happen.
#[tokio::test(start_paused = true)]
async fn a_cushion_deficit_shows_no_refilling_mode() {
    let drained = run_consumer(23).await;
    assert!(!drained.showed_refilling);
}

/// What a refill probe saw: the producer's furthest read, the consumer's last
/// pushed chunk, the `buffer_refill_started` details, and the deficit left on
/// the shared state.
struct RefillRun {
    read: i64,
    delivered: i64,
    started: Vec<serde_json::Value>,
    deficit_secs: u64,
}

/// A live source adding one chunk per second, a consumer pushing one chunk per
/// second, a 40 s target, and a cushion of `cushion` chunks between the live
/// edge and the consumer, for 150 s.
async fn run_buffered_producer(cushion: i64) -> RefillRun {
    const TARGET_CHUNKS: i64 = 40;
    const START: i64 = 1;
    let chunks: Vec<(i64, Vec<u8>)> = (1..=400).map(|i| (i, vec![0u8; 1])).collect();
    let fetcher = TimedMockFetcher::new(chunks, START + cushion).with_chunk_duration(1000);
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
    let delivered = stats.lock().await.current_chunk_id;
    let (rows, _) = ring.since(0);
    let started = rows
        .iter()
        .filter(|r| r.action == Action::BufferRefillStarted)
        .map(|r| r.detail.clone())
        .collect();
    RefillRun {
        read,
        delivered,
        started,
        deficit_secs: buffer_state.refill_deficit_secs.load(Ordering::Relaxed),
    }
}

impl RefillRun {
    /// The probe ran a few times, with the producer ahead of the consumer.
    fn assert_probed_ahead_of_the_consumer(&self) {
        let (read, delivered) = (self.read, self.delivered);
        assert!(
            read >= 4 * crate::producer_lag::LAG_PROBE_INTERVAL_ITERS as i64,
            "the producer must have read enough chunks to run the refill probe \
             a few times (read up to {read})"
        );
        assert!(
            read > delivered + 5,
            "the producer must be ahead of the consumer (read {read}, pushed {delivered})"
        );
    }
}

/// The cushion is exactly at target. The producer reads ~11 chunks ahead into
/// the prefetch channel; that is not a drained cushion, so no refill starts.
#[tokio::test(start_paused = true)]
async fn the_prefetch_queue_is_not_a_refill_deficit() {
    let run = run_buffered_producer(40).await;
    run.assert_probed_ahead_of_the_consumer();
    assert!(
        run.started.is_empty(),
        "the cushion was at target, but a refill started: {:?}",
        run.started
    );
    assert_eq!(run.deficit_secs, 0);
}

/// A cushion 20 chunks short of target is a real deficit of 20 s, measured at
/// the consumer (the producer's read pointer would say ~31 s).
#[tokio::test(start_paused = true)]
async fn a_drained_cushion_is_measured_at_the_consumer() {
    let run = run_buffered_producer(20).await;
    run.assert_probed_ahead_of_the_consumer();
    let deficits: Vec<u64> = run
        .started
        .iter()
        .filter_map(|d| d["deficit_secs"].as_u64())
        .collect();
    // ±1 for the second the consumer is in the middle of; the producer's read
    // pointer, ~11 chunks further on, would say ~31 s.
    assert!(
        deficits.len() == 1 && (19..=21).contains(&deficits[0]),
        "one refill of ~20 s expected: {:?}",
        run.started
    );
    assert!(
        (19..=21).contains(&run.deficit_secs),
        "{}",
        run.deficit_secs
    );
}
