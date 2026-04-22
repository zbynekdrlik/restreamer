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

// compute_start_chunk_id tests removed — function reverted (broke VPS creation
// when chunks are cleared on restart). Cache init fix needs proper redesign.

#[tokio::test]
async fn restart_history_loads_from_db_newest_first() {
    use crate::delivery::load_restart_history_from_db;
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let event_id = db::create_streaming_event(&pool, "rh-evt").await.unwrap();
    let inst_id = db::create_delivery_instance(
        &pool,
        1,
        "rh-inst",
        "192.0.2.1",
        "cx22",
        Some(event_id),
        "tok",
    )
    .await
    .unwrap();

    // Two rows, oldest first inserted.
    sqlx::query(
        "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs, stderr_tail)
         VALUES (?1, ?2, 'yt1', ?3, 100, 300, 'youtube_rtmp_closed', 30,
         'size= 1kB time=00:00:00 bitrate=1kbits/s\n[aost] Error submitting a packet to the muxer: Broken pipe')"
    ).bind(inst_id).bind(event_id).bind(1000_i64).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs, stderr_tail)
         VALUES (?1, ?2, 'yt1', ?3, 200, 200, 'youtube_rtmp_closed', 60, '')"
    ).bind(inst_id).bind(event_id).bind(2000_i64).execute(&pool).await.unwrap();

    let rows = load_restart_history_from_db(&pool, inst_id, "yt1", 10).await;
    assert_eq!(rows.len(), 2);
    // Newest first (timestamp_ms=2000 → chunk_id=200).
    assert_eq!(rows[0].chunk_id, 200);
    assert_eq!(rows[0].timestamp_ms, 2000);
    assert_eq!(rows[0].reason, "youtube_rtmp_closed");
    assert_eq!(rows[0].backoff_secs, 60);

    // Second-newest carries stderr_tail and a parsed last-error line.
    assert_eq!(rows[1].chunk_id, 100);
    assert!(
        rows[1]
            .stderr_tail
            .as_deref()
            .unwrap()
            .contains("Broken pipe")
    );
    let line = rows[1].stderr_last_error_line.as_deref().unwrap();
    assert!(line.contains("Broken pipe"));
    // The noisy "size=" progress line must be filtered out.
    assert!(!line.starts_with("size="));
}

#[tokio::test]
async fn restart_history_filter_by_alias_only_matches_alias_rows() {
    use crate::delivery::load_restart_history_from_db;
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    let event_id = db::create_streaming_event(&pool, "rh-evt2").await.unwrap();
    let inst_id = db::create_delivery_instance(
        &pool,
        2,
        "rh-inst2",
        "192.0.2.2",
        "cx22",
        Some(event_id),
        "tok",
    )
    .await
    .unwrap();

    for alias in ["yt1", "yt2"] {
        sqlx::query(
            "INSERT INTO delivery_restart_log (instance_id, event_id, alias, timestamp_ms, chunk_id, lifetime_secs, reason, backoff_secs, stderr_tail)
             VALUES (?1, ?2, ?3, ?4, 50, 100, 'stdin_closed', 10, NULL)"
        ).bind(inst_id).bind(event_id).bind(alias).bind(500_i64).execute(&pool).await.unwrap();
    }

    let rows = load_restart_history_from_db(&pool, inst_id, "yt1", 10).await;
    assert_eq!(rows.len(), 1);
    // The alias-specific row has no stderr, so no parsed last-error line either.
    assert!(rows[0].stderr_tail.is_none());
    assert!(rows[0].stderr_last_error_line.is_none());
}

#[test]
fn pick_last_error_line_inline_skips_progress_and_finds_broken_pipe() {
    use crate::delivery::pick_last_error_line_inline;
    let stderr = "ffmpeg version 6.1\n  built with gcc\n  configuration: --enable-gpl\nsize=  42kB time=00:00:01 bitrate=330kbits/s\n[aost] Error submitting a packet to the muxer: Broken pipe\nsize=  45kB time=00:00:02 bitrate=300kbits/s";
    let line = pick_last_error_line_inline(stderr).unwrap();
    assert!(line.contains("Broken pipe"));
    assert!(!line.starts_with("size="));
}

#[test]
fn pick_last_error_line_inline_returns_none_when_only_progress() {
    use crate::delivery::pick_last_error_line_inline;
    let stderr = "size=  42kB time=00:00:01 bitrate=330kbits/s\nframe=    2 fps=1.0 q=-1.0";
    assert!(pick_last_error_line_inline(stderr).is_none());
}
