use super::*;

#[test]
fn build_rescue_ffmpeg_args_rtmp_endpoint() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/rescue.mp4",
        "rtmps://live-api-s.facebook.com:443/rtmp/key123",
        "flv",
        "FB-Test",
    );
    assert!(args.contains(&"-stream_loop".to_string()));
    assert!(args.contains(&"-1".to_string()));
    assert!(args.contains(&"-re".to_string()));
    let vf_idx = args.iter().position(|a| a == "-vf").unwrap();
    let vf_val = &args[vf_idx + 1];
    assert!(vf_val.contains("drawtext="));
    assert!(vf_val.contains("reload=1"));
    // Countdown file path is platform-dependent (std::env::temp_dir), so
    // match only the suffix that identifies the alias.
    assert!(
        vf_val.contains("rescue_FB-Test.txt"),
        "vf should reference rescue_FB-Test.txt, got: {vf_val}"
    );
    assert!(args.last().unwrap().contains("facebook.com"));
}

#[test]
fn build_rescue_ffmpeg_args_hls_endpoint() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/rescue.mp4",
        "https://a.upload.youtube.com/http_upload_hls?cid=key123&copy=0&file=out1248.ts",
        "hls",
        "YT-Test",
    );
    assert!(args.iter().any(|a| a == "hls"));
    assert!(args.iter().any(|a| a == "PUT"));
}

/// Rescue ffmpeg output MUST be stream-format-compatible with what OBS
/// sends, otherwise switching between real content and rescue content
/// confuses YouTube's ingestion. These tests pin the normalization so
/// a regression that drops any of these flags is caught immediately.
#[test]
fn build_rescue_ffmpeg_args_normalizes_to_1080p30() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/src.mp4",
        "rtmp://a.rtmp.youtube.com/live2/key",
        "flv",
        "YT",
    );

    let vf_idx = args.iter().position(|a| a == "-vf").unwrap();
    let vf = &args[vf_idx + 1];
    assert!(
        vf.contains("scale=1920:1080"),
        "-vf must scale to 1920x1080, got: {vf}"
    );
    assert!(vf.contains("fps=30"), "-vf must enforce 30fps, got: {vf}");
    assert!(
        vf.contains("format=yuv420p"),
        "-vf must enforce yuv420p, got: {vf}"
    );
    assert!(
        vf.contains("pad="),
        "-vf must letterbox with pad, got: {vf}"
    );
}

#[test]
fn build_rescue_ffmpeg_args_uses_libx264_main_profile() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/src.mp4",
        "rtmp://a.rtmp.youtube.com/live2/key",
        "flv",
        "YT",
    );

    // Find -c:v libx264 pair
    let cv_idx = args
        .iter()
        .position(|a| a == "-c:v")
        .expect("-c:v must be present");
    assert_eq!(args[cv_idx + 1], "libx264");

    let profile_idx = args
        .iter()
        .position(|a| a == "-profile:v")
        .expect("-profile:v must be present");
    assert_eq!(args[profile_idx + 1], "main");

    let pix_idx = args
        .iter()
        .position(|a| a == "-pix_fmt")
        .expect("-pix_fmt must be present");
    assert_eq!(args[pix_idx + 1], "yuv420p");
}

#[test]
fn build_rescue_ffmpeg_args_keyframe_every_2s() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/src.mp4",
        "rtmp://a.rtmp.youtube.com/live2/key",
        "flv",
        "YT",
    );

    // -g 60 at -r 30 = 2s GOP, matches OBS / YouTube low-latency expectation
    let g_idx = args.iter().position(|a| a == "-g").expect("-g must be set");
    assert_eq!(args[g_idx + 1], "60");

    let r_idx = args.iter().position(|a| a == "-r").expect("-r must be set");
    assert_eq!(args[r_idx + 1], "30");
}

#[test]
fn build_rescue_ffmpeg_args_aac_audio_48k_stereo() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/src.mp4",
        "rtmp://a.rtmp.youtube.com/live2/key",
        "flv",
        "YT",
    );

    let ca_idx = args
        .iter()
        .position(|a| a == "-c:a")
        .expect("-c:a must be set");
    assert_eq!(args[ca_idx + 1], "aac");

    let ar_idx = args
        .iter()
        .position(|a| a == "-ar")
        .expect("-ar must be set");
    assert_eq!(args[ar_idx + 1], "48000");

    let ac_idx = args
        .iter()
        .position(|a| a == "-ac")
        .expect("-ac must be set");
    assert_eq!(args[ac_idx + 1], "2");
}

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

#[test]
fn endpoint_url_youtube_hls() {
    let url = endpoint_url_for_service(rs_ffmpeg::ServiceType::YtHls, "test-key");
    assert!(url.contains("a.upload.youtube.com"));
    assert!(url.contains("test-key"));
}

#[test]
fn endpoint_url_facebook() {
    let url = endpoint_url_for_service(rs_ffmpeg::ServiceType::Facebook, "fb-key");
    assert!(url.contains("facebook.com"));
    assert!(url.contains("fb-key"));
}

#[test]
fn output_format_yt_hls_is_hls() {
    assert_eq!(
        output_format_for_service(rs_ffmpeg::ServiceType::YtHls),
        "hls"
    );
}

#[test]
fn output_format_facebook_is_flv() {
    assert_eq!(
        output_format_for_service(rs_ffmpeg::ServiceType::Facebook),
        "flv"
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
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::{Mutex, watch};

struct WarmupMockFetcher {
    available_up_to: AtomicI64,
    chunk_duration_ms: i64,
}

impl WarmupMockFetcher {
    fn new(available_up_to: i64, chunk_duration_ms: i64) -> Self {
        Self {
            available_up_to: AtomicI64::new(available_up_to),
            chunk_duration_ms,
        }
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
        let up_to = self.available_up_to.load(Ordering::Relaxed);
        if chunk_id <= up_to {
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
    )
    .await;

    assert!(stopped, "should return true when stop signal received");
    // Countdown file should be cleaned up
    assert!(
        !std::path::Path::new(&countdown_file_path(&alias)).exists(),
        "countdown file should be cleaned up on stop"
    );
}
