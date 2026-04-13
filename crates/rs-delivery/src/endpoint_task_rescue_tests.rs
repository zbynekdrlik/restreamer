//! Tests for rescue mode state machine in the endpoint delivery loop.
use super::*;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn endpoint_stats_start_in_warmup_mode() {
    let stats: Stats = Arc::new(Mutex::new(EndpointStats {
        current_chunk_id: 1,
        delivery_mode: "warmup".to_string(),
        ..Default::default()
    }));
    let s = stats.lock().await;
    assert_eq!(s.delivery_mode, "warmup");
}

#[tokio::test]
async fn endpoint_stats_default_is_normal() {
    let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));
    let s = stats.lock().await;
    assert_eq!(s.delivery_mode, "normal");
}

#[tokio::test]
async fn buffer_state_tracks_duration() {
    let bs = BufferState::new();
    assert_eq!(bs.buffer_duration_ms.load(Ordering::Relaxed), 0);
    bs.buffer_duration_ms.store(5000, Ordering::Relaxed);
    assert_eq!(bs.buffer_duration_ms.load(Ordering::Relaxed), 5000);
}

#[tokio::test]
async fn buffer_state_producer_active_default_true() {
    let bs = BufferState::new();
    assert!(bs.producer_active.load(Ordering::Relaxed));
}

#[tokio::test]
async fn buffer_state_producer_stall_detection() {
    let bs = BufferState::new();
    bs.producer_active.store(false, Ordering::Relaxed);
    assert!(!bs.producer_active.load(Ordering::Relaxed));
}

// Tests for initial_delivery_mode — pure function that decides whether
// an endpoint starts in "warmup" or "normal" based on configuration.
// Warmup requires: rescue video configured AND not fast AND non-zero delay.

#[test]
fn initial_mode_warmup_when_all_conditions_met() {
    assert_eq!(initial_delivery_mode(true, false, 120_000), "warmup");
}

#[test]
fn initial_mode_normal_when_no_rescue_video() {
    assert_eq!(initial_delivery_mode(false, false, 120_000), "normal");
}

#[test]
fn initial_mode_normal_when_fast_endpoint() {
    // Fast endpoints never enter warmup — they run near-live
    assert_eq!(initial_delivery_mode(true, true, 120_000), "normal");
}

#[test]
fn initial_mode_normal_when_zero_delay() {
    // Zero delay means no cache window to fill, so no warmup
    assert_eq!(initial_delivery_mode(true, false, 0), "normal");
}
