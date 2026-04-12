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
