//! C4 (#239) — REAL runtime behavioral rescue tests: Group A.
//!
//! Replaces the R1/R2/R3 source-grep tests (`read_to_string` + `.contains`)
//! that asserted the SHAPE of the source instead of its BEHAVIOUR. Those
//! tests passed while #251 shipped a broken rescue path — exactly the failure
//! mode the operator hit (CI green, rescue dead). These tests instead RUN the
//! trigger and assert observable behaviour.
//!
//! Group A (this file) drives the shared `rust_rescue_push_with_pusher` loop
//! directly with a recording `Pushable`, proving the rescue clip bytes
//! actually flow on the wire — the assertion the source-grep R1 could never
//! make. `rust_rescue_push_with_pusher` is the single loop every endpoint
//! type (fast + non-fast, after #251) funnels through, so proving the bytes
//! here proves them for all of them. Standing up an in-process RTMP server
//! to observe bytes end-to-end would be >300 LoC of protocol plumbing — the
//! same scope-creep R1/R2/R3 punted to source greps; the injectable
//! `Pushable` seam (#239) is the architecture-clean alternative.
//!
//! Group B (end-to-end drain activates rescue via the real `endpoint_loop`)
//! and Group C (producer-respawn arm) live in
//! `rescue_endpoint_loop_tests.rs` — split out to stay under the project's
//! 1000-line-per-file cap.
//!
//! All time-based waits are fast-forwarded with `tokio::time` virtual time —
//! no real sleeps.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use rs_rtmp_push::PushError;

use crate::buffer_state::BufferState;
use crate::endpoint_stats::{EndpointStats, Stats};
use crate::pushable::Pushable;
use crate::rescue_default::DEFAULT_RESCUE_FLV;

/// Advance virtual time in `count` steps of `step`, yielding between each so
/// spawned tasks make progress (their internal sleeps fire deterministically).
async fn advance_in_steps(step: Duration, count: u32) {
    for _ in 0..count {
        tokio::time::advance(step).await;
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// Group A — the shared rescue push loop actually pushes the rescue clip.
//
// This is the assertion the source-grep R1 could never make: that real rescue
// bytes flow on the wire. `rust_rescue_push_with_pusher` is the single loop
// every endpoint type (fast + non-fast, after #251) funnels through, so
// proving the bytes here proves them for all of them. A recording `Pushable`
// captures every payload; the production path constructs the concrete
// `RtmpPusher` in the (untested-here) thin wrapper. Standing up an in-process
// RTMP server to observe bytes end-to-end would be >300 LoC of protocol
// plumbing — the same scope-creep R1/R2/R3 punted to source greps; the
// injectable `Pushable` seam (#239) is the architecture-clean alternative.
// ---------------------------------------------------------------------------

/// A recording `Pushable`: captures every pushed payload's exact bytes and
/// never errors, so any rescue-clip push is observable. Models ~1x self-pacing
/// (200ms virtual sleep) so it advances virtual time like the real pusher.
struct RecordingPusher {
    pushes: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Pushable for RecordingPusher {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.pushes.lock().await.push(data.to_vec());
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
}

#[tokio::test(start_paused = true)]
async fn rescue_push_actually_pushes_rescue_clip_bytes() {
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    // Outage: producer stalled, so the refill-exit (120s active) never fires.
    let buffer_state = Arc::new(BufferState::new());
    buffer_state.producer_active.store(false, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());
    let stats_task = stats.clone();
    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-bytes-test",
            crate::rescue_segments::RescueClipSource::Fixed(flv),
            bs_task,
            stats_task,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Let the loop push several rescue blobs (each ~200ms paced).
    advance_in_steps(Duration::from_millis(200), 20).await;

    {
        let recorded = pushes.lock().await;
        assert!(
            !recorded.is_empty(),
            "rescue loop must push the rescue clip during an outage; pushed nothing"
        );
        assert!(
            recorded.iter().all(|p| p.as_slice() == DEFAULT_RESCUE_FLV),
            "every rescue push must be the DEFAULT_RESCUE_FLV blob (len {}); got lengths {:?}",
            DEFAULT_RESCUE_FLV.len(),
            recorded.iter().map(|p| p.len()).collect::<Vec<_>>()
        );
    }
    {
        let s = stats.lock().await;
        assert_eq!(
            s.delivery_mode, "rescue",
            "delivery_mode must read 'rescue' while the producer is stalled, got {:?}",
            s.delivery_mode
        );
        assert!(
            s.last_push_ok_unix_ms.is_some(),
            "rescue pushes must stamp last_push_ok_unix_ms — the #238 crash \
             gate asserts 'rescue is LIVE' via last_push_ok_age_ms"
        );
    }

    let _ = stop_tx.send(true);
    advance_in_steps(Duration::from_millis(50), 4).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}

#[tokio::test(start_paused = true)]
async fn rescue_push_errors_do_not_stamp_last_push_ok() {
    // #284 telemetry honesty: a FAILING rescue push must NOT advance
    // last_push_ok_unix_ms. The #238 crash-exhaustion gate reads that field
    // as proof the rescue clip is actually FLOWING; stamping it on errors
    // would mask a dark endpoint as "rescue live".
    struct ErroringPusher;
    impl Pushable for ErroringPusher {
        async fn push_flv_bytes(&mut self, _data: &[u8]) -> Result<(), PushError> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Err(PushError::Timeout)
        }
        async fn close(&mut self) {}
        fn reconnect_count(&self) -> u32 {
            0
        }
        fn av_skew_ms(&self) -> i64 {
            0
        }
    }

    let buffer_state = Arc::new(BufferState::new());
    buffer_state.producer_active.store(false, Ordering::Relaxed);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());

    let stats_task = stats.clone();
    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            ErroringPusher,
            "rescue-err-test",
            crate::rescue_segments::RescueClipSource::Fixed(flv),
            bs_task,
            stats_task,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Several push+backoff cycles (200ms push + 500ms ERROR_BACKOFF).
    advance_in_steps(Duration::from_millis(200), 30).await;

    {
        let s = stats.lock().await;
        assert!(
            s.last_push_ok_unix_ms.is_none(),
            "failed rescue pushes must NOT stamp last_push_ok_unix_ms, got {:?}",
            s.last_push_ok_unix_ms
        );
        assert_eq!(s.delivery_mode, "rescue");
    }

    let _ = stop_tx.send(true);
    advance_in_steps(Duration::from_millis(100), 10).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}

#[tokio::test(start_paused = true)]
async fn rescue_push_resumes_normal_when_producer_recovers() {
    // Refill recovery: once the producer is active AND queuing genuinely fresh
    // chunks for RESCUE_REFILL_TARGET_SECS continuous wall-seconds, the loop
    // exits with `false` (not stop) — the recovery path that lets normal
    // delivery resume.
    //
    // #289: "recovery" now requires BOTH producer_active AND fresh chunks
    // actually flowing (highest_sent_chunk_id advancing). Modelling recovery as
    // producer_active alone (the pre-#289 assumption this test encoded) is
    // exactly the bug — a bare flag flap must NOT resume normal delivery. So
    // this test now drives a background "producer" that queues a fresh chunk
    // every 500ms of virtual time, proving the cache is genuinely refilling.
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    let buffer_state = Arc::new(BufferState::new());
    // Producer ACTIVE from the start (source already back) ...
    buffer_state.producer_active.store(true, Ordering::Relaxed);
    // ... and starting from a concrete live-edge high-water mark.
    buffer_state
        .highest_sent_chunk_id
        .store(0, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());

    // Background "producer": queues a fresh chunk every 500ms of virtual time,
    // so highest_sent_chunk_id keeps advancing — the cache is genuinely
    // refilling with new live content, not a producer_active flag flap.
    let bs_producer = buffer_state.clone();
    let producer = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            bs_producer
                .highest_sent_chunk_id
                .fetch_add(1, Ordering::Relaxed);
        }
    });

    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-recover-test",
            crate::rescue_segments::RescueClipSource::Fixed(flv),
            bs_task,
            stats,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Advance well past RESCUE_REFILL_TARGET_SECS (120s) of continuous active
    // WITH fresh chunks flowing.
    advance_in_steps(Duration::from_millis(500), 320).await; // ~160s

    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect(
            "rescue loop must exit once the producer has been active + queuing fresh chunks long enough",
        )
        .expect("rescue task panicked");
    producer.abort();
    assert!(
        !result,
        "rescue loop must return false (refilled, resume normal), not true (stop)"
    );
}

#[tokio::test(start_paused = true)]
async fn outage_rescue_persists_when_producer_active_flaps_without_fresh_chunks() {
    // #289 regression (RED before the fix). Under a sustained / trickle outage
    // the #237 producer-respawn churn holds `producer_active` = true for well
    // past RESCUE_REFILL_TARGET_SECS, but NO genuinely fresh chunks are queued:
    // the respawn resumes PAST the live edge, finds nothing, and
    // `highest_sent_chunk_id` (a `fetch_max`, capped at the pre-outage edge)
    // stays frozen. Pre-#289 the exit keyed on `producer_active` ALONE, so the
    // loop exited (returned false = "resume normal") and dropped rescue onto a
    // dark/stuttering stream. Post-#289 the loop must STAY in rescue: it must
    // NOT exit, and `delivery_mode` must stay banner-worthy the whole time.
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    let buffer_state = Arc::new(BufferState::new());
    // Producer flag stuck TRUE the whole outage (respawn churn) ...
    buffer_state.producer_active.store(true, Ordering::Relaxed);
    // ... but the high-water mark of SENT chunks never advances (no fresh
    // content past the pre-outage live edge). Freeze it at a realistic edge.
    buffer_state
        .highest_sent_chunk_id
        .store(500, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());

    let bs_task = buffer_state.clone();
    let stats_task = stats.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-flap-test",
            crate::rescue_segments::RescueClipSource::Fixed(flv),
            bs_task,
            stats_task,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Advance ~200s — well past RESCUE_REFILL_TARGET_SECS (120s). Pre-fix the
    // loop would already have exited on producer_active alone by ~120s.
    advance_in_steps(Duration::from_millis(500), 400).await;

    // The discriminating assertion: the loop must STILL be running (has NOT
    // exited to resume normal delivery). RED pre-fix (task finished ~120s in),
    // GREEN post-fix (fresh_chunks == 0 keeps rescue latched).
    assert!(
        !task.is_finished(),
        "#289: rescue must NOT exit on producer_active alone — it exited without \
         any genuinely fresh chunks (highest_sent frozen), which drops rescue onto \
         a dark/stuttering stream"
    );

    // And the dashboard must still show a banner-worthy rescue mode.
    {
        let s = stats.lock().await;
        assert!(
            s.delivery_mode == "rescue" || s.delivery_mode == "recovering",
            "#289: delivery_mode must stay banner-worthy during the outage, got {:?}",
            s.delivery_mode
        );
    }

    // Cleanup: stop the loop and let the spawned task unwind.
    let _ = stop_tx.send(true);
    advance_in_steps(Duration::from_millis(50), 10).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}

#[tokio::test(start_paused = true)]
async fn rescue_exits_after_recovery_even_when_channel_backpressure_plateaus_sent() {
    // Regression for the 2026-07-15 overnight E2E failure ("RescueRecovered
    // not recorded - never returned to live"). During GENUINE recovery the
    // producer refills the prefetch channel, but the consumer is still busy
    // pushing the rescue clip and consumes nothing — so after
    // PREFETCH_BUFFER_SIZE (10) sends the channel is full, the producer
    // blocks, and `highest_sent_chunk_id` PLATEAUS. The review-driven
    // recency gate (RESCUE_STALE_GRACE_SECS) treated that plateau as "stale
    // advance" and refused to ever exit rescue — a deadlock strictly worse
    // than the bug it hardened against. The exit discriminator must accept
    // a plateaued-but-substantial refill (fresh_chunks >= half the channel)
    // as genuine recovery.
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    let buffer_state = Arc::new(BufferState::new());
    // Producer active (source is back) ...
    buffer_state.producer_active.store(true, Ordering::Relaxed);
    buffer_state
        .highest_sent_chunk_id
        .store(500, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);
    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());

    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-plateau-test",
            crate::rescue_segments::RescueClipSource::Fixed(flv),
            bs_task,
            stats,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Genuine recovery burst: the producer refills the prefetch channel —
    // 10 fresh chunks (PREFETCH_BUFFER_SIZE) over ~5s — then BLOCKS on the
    // full channel and `highest_sent_chunk_id` plateaus for the rest of
    // the window (the consumer is pushing rescue, not consuming).
    for _ in 0..10 {
        advance_in_steps(Duration::from_millis(500), 1).await;
        buffer_state
            .highest_sent_chunk_id
            .fetch_add(1, Ordering::Relaxed);
    }

    // Advance well past RESCUE_REFILL_TARGET_SECS (120s) with the plateau
    // in place. The loop MUST exit (return false = resume normal delivery):
    // producer continuously active + a substantial refill queued.
    advance_in_steps(Duration::from_millis(500), 320).await; // ~160s

    let result = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect(
            "rescue must exit after a genuine recovery even though channel \
             backpressure plateaued highest_sent_chunk_id (the 2026-07-15 \
             E2E deadlock: RescueRecovered never recorded)",
        )
        .expect("rescue task panicked");
    assert!(
        !result,
        "rescue loop must return false (refilled, resume normal), not true (stop)"
    );
}

#[tokio::test(start_paused = true)]
async fn outage_rescue_persists_when_only_one_stray_chunk_lands_early() {
    // Review finding on #289 (v0.29.1 batch, RED before the recency fix).
    // `fresh_chunks` was a cumulative delta since the active window STARTED,
    // so a single stray/early chunk (e.g. a stale-tail re-fetch landing one
    // queued-but-not-yet-delivered chunk before finding nothing further)
    // satisfied `fresh_chunks > 0` for the REST OF THE WINDOW even though no
    // more content ever arrived. Pre-fix this let rescue exit at the 120s
    // mark onto a stream that had actually been dark for ~119s of that
    // window — reproducing the exact failure #289 fixed, just requiring one
    // coincidental extra chunk instead of zero.
    let pushes = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let pusher = RecordingPusher {
        pushes: pushes.clone(),
    };

    let buffer_state = Arc::new(BufferState::new());
    // Producer flag stuck TRUE the whole outage (respawn churn) ...
    buffer_state.producer_active.store(true, Ordering::Relaxed);
    buffer_state
        .highest_sent_chunk_id
        .store(500, Ordering::Relaxed);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let flv = Arc::new(DEFAULT_RESCUE_FLV.to_vec());

    let bs_task = buffer_state.clone();
    let task = tokio::spawn(async move {
        crate::rust_rescue_push::rust_rescue_push_with_pusher(
            pusher,
            "rescue-stray-chunk-test",
            crate::rescue_segments::RescueClipSource::Fixed(flv),
            bs_task,
            stats,
            &mut stop_rx,
            crate::rust_rescue_push::RescuePushMode::Outage,
        )
        .await
    });

    // Let the loop observe the frozen baseline for a couple of ticks, then
    // land ONE stray chunk early in the window (a stale-tail re-fetch
    // finding one queued chunk before the respawn resumes past the live
    // edge and finds nothing further) -- then never touch it again.
    advance_in_steps(Duration::from_millis(500), 6).await; // ~3s in
    buffer_state
        .highest_sent_chunk_id
        .fetch_add(1, Ordering::Relaxed);

    // Advance ~200s total -- well past RESCUE_REFILL_TARGET_SECS (120s) AND
    // past RESCUE_STALE_GRACE_SECS (15s) since the one stray chunk. Pre-fix
    // the loop would have exited at ~120s (fresh_chunks stuck at 1 > 0 the
    // whole time); post-fix the stray advance ages out after 15s and the
    // loop must stay latched in rescue.
    advance_in_steps(Duration::from_millis(500), 394).await; // ~197s more

    assert!(
        !task.is_finished(),
        "#289: rescue must NOT exit on a single stale early chunk advance -- \
         the stream went dark again for the rest of the window, so exiting \
         resumes normal delivery onto silence"
    );

    // Cleanup: stop the loop and let the spawned task unwind.
    let _ = stop_tx.send(true);
    advance_in_steps(Duration::from_millis(50), 10).await;
    let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
}
