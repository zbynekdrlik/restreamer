use super::*;

#[test]
fn format_countdown_warmup() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        95,
    );
    assert_eq!(text, "Stream starting ~ 1m 35s");
}

#[test]
fn format_countdown_buffer_empty() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        30,
    );
    assert_eq!(text, "Stream recovering ~ 30s");
}

#[test]
fn format_countdown_zero() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        0,
    );
    assert_eq!(text, "Stream starting soon");
}

#[test]
fn format_countdown_normal_mode_empty() {
    let text = format_countdown_text(&DeliveryMode::Normal, 120);
    assert_eq!(text, "");
}

#[test]
fn countdown_file_path_sanitizes() {
    // Path is platform-dependent (temp_dir), so assert only the suffix
    let path = countdown_file_path("FB/Test Stream");
    assert!(
        path.ends_with("rescue_FB_Test_Stream.txt"),
        "path should end with sanitized alias, got: {path}"
    );
}

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

    // Countdown file should be cleaned up after stop
    assert!(
        !std::path::Path::new(&countdown_file_path(&alias)).exists(),
        "countdown file should be cleaned up after stop"
    );
}

#[tokio::test]
async fn warmup_writes_countdown_file_with_warmup_text() {
    // TDD: this catches that the countdown file is actually written with
    // Warmup text (not BufferEmpty, not empty). Earlier implementation only
    // updated stats — no file was ever written during warmup.
    //
    // Approach: set up fetcher with limited chunks so the fill never
    // completes, then send a stop signal after 1s. During that second,
    // the initial seed write (+ per-chunk updates on the 1 chunk available)
    // should have produced a "Stream starting" file.
    let alias = unique_alias("countdown");
    let fetcher = WarmupMockFetcher::new(0, 100); // only 1 chunk available, 100ms
    let ep_cfg = test_endpoint_config(&alias, false);
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let (stop_tx, mut stop_rx) = watch::channel(false);

    // Poll the countdown file for warmup text, AND send a stop signal
    // after 1s so the main warmup loop terminates (otherwise it hangs
    // forever waiting for the target buffer duration).
    let alias_probe = alias.clone();
    let probe = tokio::spawn(async move {
        let mut saw_warmup_text = false;
        for _ in 0..100 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            if let Ok(contents) = std::fs::read_to_string(countdown_file_path(&alias_probe)) {
                if contents.starts_with("Stream starting") {
                    saw_warmup_text = true;
                    break;
                }
            }
        }
        // Stop the warmup loop regardless so the test doesn't hang.
        let _ = stop_tx.send(true);
        saw_warmup_text
    });

    let _ = run_warmup_loop(
        &fetcher,
        &alias,
        &ep_cfg,
        0,
        10_000, // large target we'll never reach
        Some("file:///tmp/nonexistent.mp4"),
        &stats,
        &mut stop_rx,
        None,
    )
    .await;

    let saw_warmup_text = probe.await.unwrap();
    // The rescue ffmpeg will fail to spawn on a file:// URL that doesn't
    // exist, but the seed countdown write happens BEFORE the spawn, so
    // the file should still have been created with the warmup text.
    assert!(
        saw_warmup_text,
        "countdown file should contain 'Stream starting' text during warmup"
    );
}

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

    // Fast endpoint: no countdown file should be created
    assert!(
        !std::path::Path::new(&countdown_file_path(&alias)).exists(),
        "fast endpoint should not create countdown file"
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
    // Countdown file should be cleaned up
    assert!(
        !std::path::Path::new(&countdown_file_path(&alias)).exists(),
        "countdown file should be cleaned up on stop"
    );
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

// ------------------------------------------------------------------
// R1 RED — `rescue_activates_when_url_null_and_cache_drains`
//
// Reproduces the 2026-05-30 stream.lan crash production incident:
// stream.lan went down, the cache on the Hetzner VPS drained, and every
// endpoint went dark because all 5 production templates had
// `rescue_video_url = NULL`. The consumer cache-drain branch at
// `endpoint_task.rs:663` is gated by
// `if let Some(ref rescue_url) = rescue_video_url { ... rescue ... }` —
// so when the URL is None (the default), the rescue block is skipped
// entirely and the endpoint goes silent.
//
// After Task 6 (the GREEN commit) the cache-drain branch will:
//   * Drop the outer `if let Some(ref rescue_url) = rescue_video_url`
//     guard, so rescue fires whenever the buffer is empty AND the
//     producer is stalled — regardless of URL config.
//   * Call `crate::rescue::run_rescue_loop(...)` with
//     `rescue_video_url.as_deref()` (Option<&str>) rather than `&str`.
//   * Let `resolve_rescue_bytes(None, ...)` (added in Task 4) substitute
//     the embedded `DEFAULT_RESCUE_FLV` blob when no URL is configured.
//
// APPROACH (Option C — source-structure assertion):
//
// The runtime path (consumer_task → cache-drain branch → run_rescue_loop)
// requires the full producer/consumer machinery: a real S3Fetcher trait
// impl, a real RtmpPusher, a Tauri-scope BufferState + audit_ring, and a
// minimum 60s sleep for RESCUE_STALL_THRESHOLD_SECS to elapse. Building
// a TestHarness that exercises this end-to-end would require >300 LoC of
// mock plumbing (mock producer, mock pusher, mock S3, mock audit) which
// is scope creep for a single regression test AND duplicates the
// integration coverage that Task 6's signature change will give us for
// free at compile time.
//
// Instead, this test asserts the structural invariant that the bug
// reduces to: the cache-drain branch in `endpoint_task.rs` must NOT
// gate the rescue invocation on `rescue_video_url` being Some. Today
// the guard is present (test FAILS). After Task 6 removes it, the test
// PASSES. This is brittle to refactor but precise to the bug — Task 6
// is explicitly the commit that removes the guard, so the signal is
// 1:1 with the fix.
//
// Spec: docs/superpowers/specs/2026-05-31-always-on-rust-rescue-design.md
// Plan: docs/superpowers/plans/2026-05-31-always-on-rust-rescue.md (Task 5/6)
#[test]
fn rescue_activates_when_url_null_and_cache_drains() {
    // Read the source file at test time. CARGO_MANIFEST_DIR points at
    // crates/rs-delivery so the source path is stable across local + CI.
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let source_path = std::path::Path::new(manifest_dir).join("src/endpoint_task.rs");
    let source = std::fs::read_to_string(&source_path).unwrap_or_else(|e| {
        panic!(
            "R1: failed to read {} — test cannot verify cache-drain branch structure: {e}",
            source_path.display()
        )
    });

    // Locate the cache-drain branch by its distinguishing marker — the
    // RESCUE_STALL_THRESHOLD_SECS sleep arm of the tokio::select! macro.
    // This is the ONE place in the consumer that handles "no chunks for
    // N seconds → maybe rescue".
    let marker = "tokio::time::sleep(std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS))";
    let marker_pos = source.find(marker).unwrap_or_else(|| {
        panic!(
            "R1: cache-drain branch marker not found in endpoint_task.rs. \
             The branch was probably renamed or refactored — update the R1 \
             test marker to match. Marker searched: {marker:?}"
        )
    });

    // Inspect the next ~80 lines after the marker — this is the full
    // body of the cache-drain branch up to and including the
    // `run_rescue_loop` call.
    let after = &source[marker_pos..];
    let branch_body: String = after.lines().take(80).collect::<Vec<_>>().join("\n");

    // The BUG: the rescue invocation is gated by `if let Some(ref
    // rescue_url) = rescue_video_url` so when URL is None the block is
    // skipped and the endpoint goes dark.
    //
    // The FIX (Task 6): remove this guard so rescue fires on
    // buffer-empty + producer-stalled regardless of URL config. The
    // embedded DEFAULT_RESCUE_FLV (via resolve_rescue_bytes(None, ...))
    // covers the no-URL case.
    let buggy_guard = "if let Some(ref rescue_url) = rescue_video_url";
    assert!(
        !branch_body.contains(buggy_guard),
        "R1 RED: cache-drain branch in endpoint_task.rs still gates rescue on \
         `if let Some(ref rescue_url) = rescue_video_url`. This is the \
         2026-05-30 production incident: when rescue_video_url=None (the \
         default across all 5 templates), the cache-drain branch is skipped \
         entirely and the endpoint goes dark. Task 6 must remove this guard \
         and call run_rescue_loop with Option<&str>, letting \
         resolve_rescue_bytes(None, ...) substitute the embedded \
         DEFAULT_RESCUE_FLV. See \
         docs/superpowers/specs/2026-05-31-always-on-rust-rescue-design.md. \
         Branch body inspected:\n---\n{branch_body}\n---"
    );

    // Additional invariant: the rescue invocation must pass the URL as
    // Option<&str> (via `rescue_video_url.as_deref()`) — proof that the
    // wiring threads through to resolve_rescue_bytes correctly. Today
    // the call passes `rescue_url` (a &str via the buggy guard's
    // binding), tomorrow it passes `rescue_video_url.as_deref()`.
    let expected_call_form = "rescue_video_url.as_deref()";
    assert!(
        branch_body.contains(expected_call_form),
        "R1 RED: cache-drain branch does not pass `rescue_video_url.as_deref()` \
         to run_rescue_loop. Task 6 must change the call to thread the Option \
         through so resolve_rescue_bytes can substitute DEFAULT_RESCUE_FLV \
         when URL is None. Branch body inspected:\n---\n{branch_body}\n---"
    );
}

// ------------------------------------------------------------------
// R3 RED (Task 7) — `warmup_always_pushes_default_rescue_when_no_url`
//
// Warmup gap analogue of the R1 cache-drain bug. Today
// `rescue.rs:run_warmup_loop` only spawns rescue when the operator
// configured a custom URL:
//
//   if let Some(rescue_url) = rescue_video_url {
//       if !ep_cfg.is_fast {
//           ... tokio::process::Command::new("ffmpeg") ...
//       }
//   }
//
// All 5 production templates have `rescue_video_url = NULL`, so
// non-fast endpoints currently show ~120s of blank screen during the
// initial cache fill instead of the embedded DEFAULT_RESCUE_FLV.
//
// After Task 7 (the R3 GREEN commit) the warmup branch must:
//   * Drop the outer `if let Some(rescue_url) = rescue_video_url`
//     guard so non-fast warmup ALWAYS pushes rescue (fast endpoints
//     still skip — low-latency trade-off per design).
//   * Replace the external `tokio::process::Command::new("ffmpeg")`
//     spawn with `crate::rust_rescue_push::rust_rescue_push` — pure
//     rust, zero ffmpeg on the VPS at runtime.
//   * Let `resolve_rescue_bytes(None, ...)` substitute
//     DEFAULT_RESCUE_FLV when no URL is configured (same pattern as
//     the cache-drain branch fix from Task 6).
//
// APPROACH (Option C — source-structure assertion, same as R1):
//
// The runtime path requires a real fetcher + audit_ring + a spawned
// rust_rescue_push that lazy-connects to an RTMP endpoint and would
// either hang or hit connect errors in tests. A behavioural test
// with a capturing pusher would need >300 LoC of mock plumbing
// (mock fetcher already exists, but a mock pusher to inspect "did
// rescue bytes flow during warmup" does not). The structural test
// asserts the two invariants the bug reduces to and stays 1:1 with
// the Task 7 fix.
//
// Spec: docs/superpowers/specs/2026-05-31-always-on-rust-rescue-design.md
// Plan: docs/superpowers/plans/2026-05-31-always-on-rust-rescue.md (Task 7)
#[test]
fn warmup_always_pushes_default_rescue_when_no_url() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let source_path = std::path::Path::new(manifest_dir).join("src/rescue.rs");
    let source = std::fs::read_to_string(&source_path).unwrap_or_else(|e| {
        panic!(
            "R3: failed to read {} — test cannot verify warmup branch structure: {e}",
            source_path.display()
        )
    });

    // Locate run_warmup_loop and grab its body up to the next `pub `
    // declaration (or EOF). This is the entire warmup function body
    // we need to inspect for the buggy guard + ffmpeg spawn.
    let warmup_start = source
        .find("pub async fn run_warmup_loop")
        .expect("R3: locate run_warmup_loop fn in rescue.rs");
    let after_start = &source[warmup_start..];
    // Skip past the signature line so "pub" doesn't match itself.
    let body_offset = after_start
        .find('{')
        .expect("R3: locate opening brace of run_warmup_loop");
    let body_search = &after_start[body_offset..];
    let next_pub = body_search.find("\npub ").unwrap_or(body_search.len());
    let body = &body_search[..next_pub];

    // BUG 1: outer guard skips rescue entirely when URL is None.
    assert!(
        !body.contains("if let Some(rescue_url) = rescue_video_url"),
        "R3 RED: warmup branch in rescue.rs still gates rescue on \
         `if let Some(rescue_url) = rescue_video_url`. When the operator \
         has not configured a custom URL (the default state for all 5 \
         production templates), non-fast endpoints show a blank screen \
         during the initial cache fill (~120s) instead of the embedded \
         DEFAULT_RESCUE_FLV. Task 7 must drop this guard and use \
         resolve_rescue_bytes + rust_rescue_push, mirroring the \
         cache-drain branch fix from Task 6. See \
         docs/superpowers/specs/2026-05-31-always-on-rust-rescue-design.md.\n\n\
         Warmup body inspected (first 600 chars):\n---\n{}\n---",
        &body[..body.len().min(600)]
    );

    // BUG 2: external ffmpeg process spawn must be gone.
    // Task 7 replaces it with rust_rescue_push so the VPS does not
    // depend on a system ffmpeg for rescue at runtime.
    assert!(
        !body.contains("tokio::process::Command::new(\"ffmpeg\")"),
        "R3 RED: warmup branch still spawns external ffmpeg via \
         `tokio::process::Command::new(\"ffmpeg\")`. Task 7 must use \
         `crate::rust_rescue_push::rust_rescue_push` (pure-rust pusher) \
         instead so the VPS does not depend on system ffmpeg for rescue \
         at runtime — matches the cache-drain branch path."
    );

    // Positive assertion: the fix must call rust_rescue_push from
    // within warmup (proves the new pusher is wired up, not just that
    // the old ffmpeg spawn was deleted).
    assert!(
        body.contains("rust_rescue_push"),
        "R3 RED: warmup branch does not call `rust_rescue_push` yet. \
         Task 7's GREEN commit must spawn the pure-rust pusher in a \
         background tokio::task during warmup for non-fast endpoints \
         and abort the handle when the warmup probe loop exits."
    );
}
