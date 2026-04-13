//! Tests for rescue mode state machine in the endpoint delivery loop.
//!
//! Buffer state and initial_delivery_mode tests live in buffer_state.rs
//! next to the code they test.
use super::*;

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

#[test]
fn initial_endpoint_stats_sets_delivery_mode_warmup() {
    let s = initial_endpoint_stats(42, "warmup".to_string());
    assert_eq!(s.delivery_mode, "warmup");
    assert_eq!(s.current_chunk_id, 42);
}

#[test]
fn initial_endpoint_stats_sets_delivery_mode_normal() {
    let s = initial_endpoint_stats(7, "normal".to_string());
    assert_eq!(s.delivery_mode, "normal");
    assert_eq!(s.current_chunk_id, 7);
}

#[test]
fn initial_endpoint_stats_other_fields_default() {
    let s = initial_endpoint_stats(100, "warmup".to_string());
    // Starts from Default, so everything else should be zero/empty
    assert_eq!(s.chunks_processed, 0);
    assert_eq!(s.bytes_processed_total, 0);
    assert_eq!(s.ffmpeg_restart_count, 0);
    assert!(s.restart_history.is_empty());
    assert_eq!(s.rescue_eta_secs, None);
}
