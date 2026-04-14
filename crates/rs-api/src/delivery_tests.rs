//! Tests for DeliveryOrchestrator.

use rs_core::config::Config;
use rs_core::db;

use crate::delivery::{DeliveryOrchestrator, is_delivery_active};

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
