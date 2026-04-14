//! Tests for DeliveryOrchestrator.

use rs_core::config::Config;
use rs_core::db;

use crate::delivery::{DeliveryOrchestrator, compute_start_chunk_id, is_delivery_active};

#[test]
fn is_delivery_active_true_for_live_states() {
    assert!(is_delivery_active("booting"));
    assert!(is_delivery_active("initializing"));
    assert!(is_delivery_active("delivering"));
    // Back-compat for older DB rows that used the flat "running" state.
    assert!(is_delivery_active("running"));
}

#[test]
fn is_delivery_active_false_for_pre_boot_and_post_death_states() {
    assert!(!is_delivery_active("creating"));
    assert!(!is_delivery_active("stopping"));
    assert!(!is_delivery_active("deleted"));
    assert!(!is_delivery_active("failed"));
    assert!(!is_delivery_active(""));
    assert!(!is_delivery_active("unknown-future-state"));
}

#[tokio::test]
async fn orchestrator_none_without_token() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    assert!(DeliveryOrchestrator::new(pool, config).is_none());
}

#[tokio::test]
async fn orchestrator_some_with_token() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    assert!(DeliveryOrchestrator::new(pool, config).is_some());
}

#[tokio::test]
async fn stop_delivery_noop_when_no_instance() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    let orch = DeliveryOrchestrator::new(pool, config).unwrap();

    // Should not error when no instance exists
    orch.stop_delivery(999).await.unwrap();
}

#[tokio::test]
async fn get_delivery_status_no_instance() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let mut config = Config::for_testing();
    config.hetzner.api_token = "test-token".to_string();
    let orch = DeliveryOrchestrator::new(pool, config).unwrap();

    let status = orch.get_delivery_status(999).await.unwrap();
    assert!(status.instance.is_none());
    assert!(!status.server_ready);
    assert!(status.endpoints.is_empty());
}

// --- compute_start_chunk_id unit tests ---

#[test]
fn start_chunk_is_next_after_latest() {
    assert_eq!(compute_start_chunk_id(Some(50)), 51);
    assert_eq!(compute_start_chunk_id(Some(1)), 2);
    assert_eq!(compute_start_chunk_id(Some(0)), 1);
}

#[test]
fn start_chunk_is_1_when_no_chunks_yet() {
    assert_eq!(compute_start_chunk_id(None), 1);
}

/// Integration test: after seeding 50 historical chunks the fresh delivery
/// start position must be 51, NOT 1.  This pins the fix for the live-edge
/// regression where historical chunks were walked by VPS warmup.
#[tokio::test]
async fn fresh_start_chunk_id_is_past_historical_max() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let event_id = db::upsert_streaming_event(&pool, "test-event-fresh-start")
        .await
        .unwrap();

    // Seed 50 historical chunks (already on S3 before operator clicks Start)
    for i in 0..50_i64 {
        db::insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/chunk{i}.bin"),
            1024,
            &format!("md5-{i:04}"),
            0,
        )
        .await
        .unwrap();
    }

    // Query the max sequence number and compute start_chunk_id
    let max_seq = db::get_latest_sequence_number_for_event(&pool, event_id)
        .await
        .unwrap();
    assert_eq!(max_seq, Some(50), "Expected 50 historical chunks");

    let start = compute_start_chunk_id(max_seq);
    assert_eq!(
        start, 51,
        "start_chunk_id must be 51 (first NEW chunk), not 1 (historical)"
    );
}

/// When there are no chunks at all, start_chunk_id must be 1.
#[tokio::test]
async fn fresh_start_chunk_id_is_1_when_no_history() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let event_id = db::upsert_streaming_event(&pool, "test-event-no-history")
        .await
        .unwrap();

    let max_seq = db::get_latest_sequence_number_for_event(&pool, event_id)
        .await
        .unwrap();
    assert_eq!(max_seq, None, "No chunks should exist");

    let start = compute_start_chunk_id(max_seq);
    assert_eq!(
        start, 1,
        "start_chunk_id must be 1 when event has no chunks"
    );
}
