use super::*;

#[test]
fn format_countdown_warmup() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        95,
    );
    assert_eq!(text, "Vysielanie sa spustí o ~1m 35s");
}

#[test]
fn format_countdown_buffer_empty() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        30,
    );
    assert_eq!(text, "Obnovujeme o ~30s");
}

#[test]
fn format_countdown_warmup_seconds_only() {
    // eta < 60 → seconds-only form, no minutes segment.
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        45,
    );
    assert_eq!(text, "Vysielanie sa spustí o ~45s");
}

#[test]
fn format_countdown_buffer_empty_minutes() {
    // eta >= 60 → minutes + seconds form.
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        150,
    );
    assert_eq!(text, "Obnovujeme o ~2m 30s");
}

#[test]
fn format_countdown_zero() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        0,
    );
    assert_eq!(text, "Vysielanie sa spustí o chvíľu");
}

#[test]
fn format_countdown_buffer_empty_zero() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        0,
    );
    assert_eq!(text, "Obnovujeme o chvíľu");
}

#[test]
fn format_countdown_normal_mode_empty() {
    let text = format_countdown_text(&DeliveryMode::Normal, 120);
    assert_eq!(text, "");
}

// #259: the temp-file countdown plumbing (countdown_file_path /
// write_countdown_file / cleanup_countdown_file) was removed — nothing ever
// read the file on the pure-Rust path. The viewer countdown is now genuinely
// rendered via the pre-rendered segment set; its selection is unit-tested in
// `rescue_segments::tests`.

// ------------------------------------------------------------------
// Integration tests for run_warmup_loop
//
// These tests catch the original bug where warmup mode only updated stats
// without actually spawning rescue ffmpeg. They use a mock fetcher that
// returns chunks on demand, and assert on the side effects run_warmup_loop
// produces: stats changes, countdown file contents, and transition to
// "normal" when the buffer fills.
// ------------------------------------------------------------------

use crate::api::EndpointConfig;
use crate::endpoint_task::{ChunkFetcher, EndpointStats, Stats};
use rs_core::models::PusherKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use tokio::sync::{Mutex, watch};

/// Mock fetcher for warmup tests. `chunk_duration_ms(id)` returns
/// `Ok(Some(chunk_duration_ms))` if `id` is in the inclusive range
/// `[available_start, available_end]`, else `Ok(None)`.
///
/// `probe_count` records every call to `chunk_duration_ms` so tests can
/// assert on algorithmic complexity (e.g. "exponential probe finishes in
/// O(log n) calls, not O(n)").
///
/// Two construction patterns:
/// - `new(N, dur)` — chunks `..=N` available (the common "S3 has up to chunk N" pattern)
/// - `with_range(s, e, dur)` — chunks pruned outside `[s, e]` (the "start_chunk_id below live edge" pattern from #146)
struct WarmupMockFetcher {
    available_start: AtomicI64,
    available_end: AtomicI64,
    chunk_duration_ms: i64,
    probe_count: AtomicU64,
}

impl WarmupMockFetcher {
    fn new(available_up_to: i64, chunk_duration_ms: i64) -> Self {
        Self {
            available_start: AtomicI64::new(i64::MIN),
            available_end: AtomicI64::new(available_up_to),
            chunk_duration_ms,
            probe_count: AtomicU64::new(0),
        }
    }

    /// Chunks outside `[start, end]` (inclusive) return `Ok(None)`. Models
    /// the production scenario where `start_chunk_id` points at a pruned
    /// chunk but newer chunks exist (#146).
    fn with_range(start: i64, end: i64, chunk_duration_ms: i64) -> Self {
        Self {
            available_start: AtomicI64::new(start),
            available_end: AtomicI64::new(end),
            chunk_duration_ms,
            probe_count: AtomicU64::new(0),
        }
    }

    fn probe_count(&self) -> u64 {
        self.probe_count.load(Ordering::Relaxed)
    }
}

impl ChunkFetcher for WarmupMockFetcher {
    async fn fetch_chunk_with_meta(
        &self,
        _chunk_id: i64,
    ) -> Result<Option<(Vec<u8>, i64)>, String> {
        unreachable!("warmup loop only calls chunk_duration_ms")
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        self.probe_count.fetch_add(1, Ordering::Relaxed);
        let start = self.available_start.load(Ordering::Relaxed);
        let end = self.available_end.load(Ordering::Relaxed);
        if chunk_id >= start && chunk_id <= end {
            Ok(Some(self.chunk_duration_ms))
        } else {
            Ok(None)
        }
    }
}

fn test_endpoint_config(alias: &str, is_fast: bool) -> EndpointConfig {
    EndpointConfig {
        alias: alias.to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "test-key".to_string(),
        is_fast,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
        pusher: PusherKind::Ffmpeg,
    }
}

/// A temp directory fixture so countdown-file tests don't pollute /tmp
/// or race each other. Override via countdown_file_path is not needed —
/// we rely on unique aliases so the file paths don't collide.
fn unique_alias(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos}")
}

// Test that verifies warmup exits as soon as buffer fills — regardless
// of wall-clock time. We previously had a wall-clock minimum which
// caused rescue video to keep playing 120s AFTER cache was ready, which
// delayed real content from reaching viewers. The correct behavior is:
// rescue plays until buffer is ready, no longer.

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn warmup_exits_as_soon_as_buffer_fills() {
    // 100 chunks of 2000ms each = 200_000ms available. Target 1000ms.
    // Should hit target and exit after probing just one chunk.
    let alias = unique_alias("fast-exit");
    let fetcher = WarmupMockFetcher::new(100, 2000);
    let ep_cfg = test_endpoint_config(&alias, false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    let target_ms = 1000u64; // 1s target

    let stopped = run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        0,
        target_ms,
        Some("file:///tmp/nonexistent-rescue.mp4"),
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    assert!(!stopped, "should not be stopped");

    // After warmup: normal mode, eta cleared, buffer met immediately
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "normal",
        "should transition to normal after buffer fills"
    );
    assert_eq!(s.rescue_eta_secs, None);
}

#[tokio::test]
async fn warmup_without_rescue_url_skips_ffmpeg_but_waits_for_fill() {
    // No rescue URL configured → no ffmpeg spawn, no stats changes, just
    // a straightforward buffer-fill wait.
    let alias = unique_alias("no-rescue");
    let fetcher = WarmupMockFetcher::new(100, 50); // 100 chunks of 50ms = 5000ms available
    let ep_cfg = test_endpoint_config(&alias, false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    let stopped = run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        0,
        2000, // target 2000ms
        None,
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    assert!(!stopped, "should not be stopped");
    let s = stats.lock().await;
    // No rescue URL → delivery_mode stays at default (we don't touch it)
    assert_eq!(s.delivery_mode, "normal");
    assert_eq!(s.rescue_eta_secs, None);
}

#[tokio::test]
async fn warmup_with_rescue_url_updates_mode_to_warmup() {
    // This test catches the bug where warmup only updated stats without
    // countdown file or ffmpeg. We verify: stats becomes warmup, countdown
    // file gets written.
    //
    // Fetcher has only 1 chunk available (50ms), target is 10_000ms, so
    // fill never completes — warmup stays active. Probe observes the
    // "warmup" state, then sends stop signal to terminate.
    let alias = unique_alias("warmup-mode");
    let fetcher = WarmupMockFetcher::new(0, 50); // only chunk 0 available
    let ep_cfg = test_endpoint_config(&alias, false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    // Capture mode transitions by polling stats in parallel.
    // Also send stop signal once we see warmup OR after 1s timeout.
    let stats_probe = stats.clone();
    let probe = tokio::spawn(async move {
        let mut saw_warmup = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            let s = stats_probe.lock().await;
            if s.delivery_mode == "warmup" {
                saw_warmup = true;
                break;
            }
        }
        // Stop the warmup loop regardless
        let _ = stop_tx.send(true);
        saw_warmup
    });

    let _ = run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        0,
        10_000, // unreachable target — warmup stays active until stop signal
        Some("file:///tmp/nonexistent-rescue.mp4"),
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    let saw_warmup = probe.await.unwrap();
    assert!(
        saw_warmup,
        "stats.delivery_mode should have been 'warmup' at some point during fill"
    );
}

// #259: `warmup_writes_countdown_file_with_warmup_text` was REMOVED — the
// temp-file countdown surface no longer exists. The warmup viewer clip is the
// "Vysielanie sa o chvíľu spustí…" segment (`rescue_segments::SEG_WARMUP`) and
// warmup selection is unit-tested in `rescue_segments::tests`; warmup stats
// (`rescue_eta_secs`) are still asserted by the mode tests above/below.

#[tokio::test]
async fn warmup_fast_endpoint_skips_rescue_ffmpeg() {
    // Fast endpoints should not spawn rescue ffmpeg even when rescue_video_url
    // is set (they run near-live, rescue adds unacceptable latency).
    let alias = unique_alias("fast");
    let fetcher = WarmupMockFetcher::new(100, 50);
    let ep_cfg = test_endpoint_config(&alias, true); // is_fast = true
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    let _ = run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        0,
        500,
        Some("file:///tmp/nonexistent.mp4"),
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    // Fast endpoint: stats.delivery_mode must never flip to "warmup" (fast
    // endpoints skip rescue entirely per the low-latency design).
    let s = stats.lock().await;
    assert_ne!(
        s.delivery_mode, "warmup",
        "fast endpoint should not enter warmup rescue"
    );
}

#[tokio::test]
async fn warmup_stop_signal_cleans_up_and_returns_true() {
    let alias = unique_alias("stop-signal");
    // No chunks available — loop will hang waiting
    let fetcher = WarmupMockFetcher::new(-1, 50);
    let ep_cfg = test_endpoint_config(&alias, false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    // Send stop signal after 100ms
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let _ = stop_tx.send(true);
    });

    let stopped = run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        0,
        10_000, // large target, will not fill
        Some("file:///tmp/nonexistent.mp4"),
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    assert!(stopped, "should return true when stop signal received");
}

/// Hardens warmup against the "start_chunk_id points at a pruned chunk"
/// failure mode (#146). Pre-fix the Ok(None) branch slept 2s without
/// incrementing probe_id, so a missing chunk hung the warmup loop
/// forever and silently. Post-fix: after CONSECUTIVE_NONE_THRESHOLD
/// consecutive Ok(None)s on the same chunk, log one WARN and probe
/// forward exponentially to find the live edge.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn warmup_skips_forward_when_chunk_missing_for_n_seconds() {
    // chunks 1..=4 missing (pruned). chunks 5+ available, 50ms each.
    // Target 1000ms — should reach within ~20 chunks past chunk 5.
    let alias = unique_alias("skip-stuck");
    let fetcher = WarmupMockFetcher::with_range(5, i64::MAX, 50);
    let ep_cfg = test_endpoint_config(&alias, false);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    // Start at chunk 1 (the "pruned" range).
    let stopped = crate::rescue::run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        1,
        1000,
        None, // no rescue video — keeps test simple
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    assert!(
        !stopped,
        "warmup must complete, not get stuck or be stopped"
    );
    let s = stats.lock().await;
    assert_eq!(
        s.delivery_mode, "normal",
        "warmup should hand off to normal"
    );
}

/// Validates the exponential-probe path of the warmup hardening (#146 review
/// follow-up). Pre-exponential the recovery was `+= 1` per 60s of consecutive
/// Nones, which on a 500-chunk pruned gap would take ~8 hours. Exponential
/// probe finds the live edge in ~10 fetches.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn warmup_exponential_probe_clears_large_pruned_gap() {
    // 500 pruned chunks, then chunks 501+ available.
    let alias = unique_alias("skip-large-gap");
    let fetcher = WarmupMockFetcher::with_range(501, i64::MAX, 50);
    let ep_cfg = test_endpoint_config(&alias, false);

    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (_stop_tx, mut stop_rx) = watch::channel(false);

    let stopped = crate::rescue::run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        1,
        1000,
        None,
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    assert!(
        !stopped,
        "warmup must complete via exponential probe on a 500-chunk gap"
    );

    // Algorithmic-complexity assertion: the probe count must be O(log n),
    // not O(n). For a 500-chunk gap starting from probe_id=1:
    //   * 30 stuck-detection probes on chunk 1 (CONSECUTIVE_NONE_THRESHOLD)
    //   * ~10 exponential-jump probes (jump 1, 2, 4, ..., 512 finds chunk 513)
    //   * ~target_delay_ms / chunk_dur successful probes filling the buffer
    //     (1000 / 50 = 20 chunks)
    // Total upper bound ~80. Linear `+= 1` would have been 500 × 30 = 15 000.
    // Cap at 200 leaves ample headroom for any reasonable refactor while
    // catching a regression to linear-advance behaviour.
    let probes = fetcher.probe_count();
    assert!(
        probes < 200,
        "exponential probe must be O(log n); got {probes} probes for a 500-chunk gap"
    );

    let s = stats.lock().await;
    assert_eq!(s.delivery_mode, "normal");
}
