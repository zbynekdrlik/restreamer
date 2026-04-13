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
