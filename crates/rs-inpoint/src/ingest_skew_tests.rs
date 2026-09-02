//! Unit tests for the ingest A/V-skew monitor (#354). RED→GREEN core: a
//! mid-stream source desync must raise `Detected` after the debounce; a clean
//! source, a constant startup domain gap, and a one-track stream must NEVER
//! detect (no false positives at normal jitter); recovery emits `Cleared`.

use super::*;
use rs_rtmp_push::SKEW_DEBOUNCE_CHUNKS;

/// The operator alert threshold used across these tests (the production
/// default; below the pusher's 4000 ms recovery kill).
const THRESHOLD_MS: i64 = 2_000;

/// Build one chunk's per-track input-PTS sequences given each track's start ts
/// on the chunker's shared epoch. Video ~33 ms (30 fps), audio ~21 ms (AAC) —
/// same shape as `rs_rtmp_push::skew`'s own tests, so the reused tracker is
/// driven exactly as in production.
fn chunk_input_pts(video_start: u32, audio_start: u32, span_ms: u32) -> (Vec<u32>, Vec<u32>) {
    let video: Vec<u32> = (0..=span_ms / 33).map(|i| video_start + i * 33).collect();
    let audio: Vec<u32> = (0..=span_ms / 21).map(|i| audio_start + i * 21).collect();
    (video, audio)
}

/// Feed one chunk (both tracks) into the monitor and return the boundary
/// transition.
fn feed_chunk(m: &mut IngestSkewMonitor, video_start: u32, audio_start: u32) -> SkewTransition {
    let (video, audio) = chunk_input_pts(video_start, audio_start, 2_000);
    for v in &video {
        m.observe_video(*v);
    }
    for a in &audio {
        m.observe_audio(*a);
    }
    m.evaluate_chunk()
}

/// A healthy shared-epoch source (audio and video advance together from 0)
/// must NEVER detect — the monitor is silent in steady state.
#[test]
fn clean_aligned_source_never_detects() {
    let mut m = IngestSkewMonitor::new(THRESHOLD_MS);
    for chunk in 0..12u32 {
        let start = chunk * 2_000;
        let t = feed_chunk(&mut m, start, start);
        assert_eq!(
            t.event, None,
            "aligned A/V must never flip the latch (chunk {chunk})"
        );
        assert!(!m.is_active());
        assert!(m.skew_ms().abs() <= THRESHOLD_MS);
    }
}

/// A benign CONSTANT A/V domain offset present from session start (audio
/// xiu-ts vs video wall-clock have different absolute zero points) must NEVER
/// detect — the constant offset folds into the baseline; only a CHANGE trips.
/// This is the false-positive guard (acceptance: no false positives at normal
/// 0-100 ms jitter, extended here to a >> threshold CONSTANT offset).
#[test]
fn constant_startup_offset_never_detects() {
    let mut m = IngestSkewMonitor::new(THRESHOLD_MS);
    const CONST_OFFSET: u32 = 20_000; // >> threshold, but CONSTANT
    for chunk in 0..15u32 {
        let video_start = chunk * 2_000 + CONST_OFFSET;
        let audio_start = chunk * 2_000;
        let t = feed_chunk(&mut m, video_start, audio_start);
        assert_eq!(
            t.event, None,
            "a CONSTANT startup domain offset is benign and must never detect"
        );
        assert!(!m.is_active());
    }
}

/// A one-track stream (audio only, no video) must NEVER detect — the tracker
/// never captures a baseline, so the skew stays 0 and the latch never flips.
#[test]
fn audio_only_stream_never_detects() {
    let mut m = IngestSkewMonitor::new(THRESHOLD_MS);
    for chunk in 0..12u32 {
        m.observe_audio(chunk * 10_000);
        let t = m.evaluate_chunk();
        assert_eq!(t.event, None, "one-track stream has no A/V skew to detect");
        assert!(!m.is_active());
        assert_eq!(m.skew_ms(), 0);
    }
}

/// THE core RED→GREEN: the incident desync APPEARS mid-stream (audio falls far
/// behind video after a clean start) and MUST raise `Detected` after
/// `SKEW_DEBOUNCE_CHUNKS` consecutive over-threshold chunks.
///
/// RED (stub monitor): `evaluate_chunk` reports 0 skew and never flips the
/// latch — the desync is propagated silently, exactly the #354 bug.
/// GREEN: the latch flips to `Detected` and `skew_ms` records the deviation.
#[test]
fn mid_stream_desync_detects_after_debounce() {
    let mut m = IngestSkewMonitor::new(THRESHOLD_MS);
    const LAG_MS: u32 = 25_500;

    // Phase 1: a few ALIGNED chunks establish a near-zero baseline.
    for chunk in 0..3u32 {
        let start = chunk * 2_000;
        let t = feed_chunk(&mut m, start, start);
        assert_eq!(t.event, None, "aligned warm-up must not detect");
    }
    assert!(!m.is_active());

    // Phase 2: video's epoch leaps LAG_MS ahead of audio (audio falls behind).
    // The gap persists, so after SKEW_DEBOUNCE_CHUNKS over-threshold chunks the
    // latch must flip to Detected exactly once.
    let mut detected_at = None;
    for chunk in 3..(3 + SKEW_DEBOUNCE_CHUNKS + 2) {
        let video_start = chunk * 2_000 + LAG_MS;
        let audio_start = chunk * 2_000;
        let t = feed_chunk(&mut m, video_start, audio_start);
        if t.event == Some(SkewEvent::Detected) && detected_at.is_none() {
            detected_at = Some(chunk);
            assert!(
                t.skew_ms.abs() > THRESHOLD_MS,
                "the Detected transition must carry the over-threshold skew, got {}",
                t.skew_ms
            );
        }
    }
    assert!(
        detected_at.is_some(),
        "a persistent mid-stream desync MUST raise Detected after the debounce"
    );
    assert!(
        m.is_active(),
        "latch stays active while the desync persists"
    );
    // Exactly one Detected — the latch does not re-fire every chunk.
    let extra = {
        let video_start = 99 * 2_000 + LAG_MS;
        feed_chunk(&mut m, video_start, 99 * 2_000)
    };
    assert_eq!(
        extra.event, None,
        "Detected must not re-fire while already active"
    );
}

/// Recovery: once the source realigns (audio catches up to video within a
/// session), the skew drops back under threshold and the latch emits `Cleared`.
#[test]
fn recovery_clears_after_desync() {
    let mut m = IngestSkewMonitor::new(THRESHOLD_MS);
    const LAG_MS: u32 = 25_500;
    // Aligned baseline.
    feed_chunk(&mut m, 0, 0);
    // Sustained desync → Detected.
    for chunk in 1..(1 + SKEW_DEBOUNCE_CHUNKS + 1) {
        feed_chunk(&mut m, chunk * 2_000 + LAG_MS, chunk * 2_000);
    }
    assert!(m.is_active(), "must be latched active after the desync");

    // Audio catches up to video's leading position (raw offset returns to the
    // baseline) → skew under threshold → Cleared, exactly once.
    let mut cleared = false;
    for chunk in 20..24u32 {
        // Both tracks now start at the SAME (leading) position.
        let pos = chunk * 2_000 + LAG_MS;
        let t = feed_chunk(&mut m, pos, pos);
        if t.event == Some(SkewEvent::Cleared) {
            cleared = true;
        }
    }
    assert!(
        cleared,
        "recovery must emit Cleared once skew drops under threshold"
    );
    assert!(!m.is_active(), "latch clears on recovery");
}

/// `reset()` (fresh session / OBS republish re-anchor) clears the latch and
/// the skew so the banner clears and measurement restarts from a new origin.
#[test]
fn reset_clears_active_latch() {
    let mut m = IngestSkewMonitor::new(THRESHOLD_MS);
    const LAG_MS: u32 = 25_500;
    feed_chunk(&mut m, 0, 0);
    for chunk in 1..(1 + SKEW_DEBOUNCE_CHUNKS + 1) {
        feed_chunk(&mut m, chunk * 2_000 + LAG_MS, chunk * 2_000);
    }
    assert!(m.is_active());
    m.reset();
    assert!(!m.is_active(), "reset must clear the latch");
    assert_eq!(m.skew_ms(), 0, "reset must clear the measured skew");
}
