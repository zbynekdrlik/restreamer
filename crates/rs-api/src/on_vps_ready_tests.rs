//! Tests for `compute_fast_endpoint_updates` — the pure-DB part of the
//! orchestrator's `on_vps_ready` flow. Verifies the SELECT logic that
//! decides which fast endpoints need a fresh start_chunk_id POST. The
//! full HTTP-emit + audit-row path of on_vps_ready is verified during
//! operator soak (Kiko visibly ahead of FB/YT confirms the swap landed
//! end-to-end on the live VPS).

use rs_core::db::{create_memory_pool, run_migrations, upsert_streaming_event};
use sqlx::{Row, SqlitePool};

async fn setup_pool() -> SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

async fn insert_chunk_raw(
    pool: &SqlitePool,
    event_id: i64,
    seq: i64,
    duration_ms: i64,
    sent: bool,
) {
    sqlx::query(
        "INSERT INTO chunk_records
         (streaming_event_id, chunk_file_path, data_size, sequence_number, duration_ms, sent)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
    )
    .bind(event_id)
    .bind(format!("/tmp/test-{event_id}-{seq}.bin"))
    .bind(0_i64)
    .bind(seq)
    .bind(duration_ms)
    .bind(if sent { 1_i64 } else { 0_i64 })
    .execute(pool)
    .await
    .unwrap();
}

async fn insert_endpoint(pool: &SqlitePool, event_id: i64, alias: &str, is_fast: bool) {
    let row = sqlx::query(
        "INSERT INTO endpoint_configs (alias, service_type, is_fast)
         VALUES (?1, 'TEST_FILE', ?2) RETURNING id",
    )
    .bind(alias)
    .bind(if is_fast { 1_i64 } else { 0_i64 })
    .fetch_one(pool)
    .await
    .unwrap();
    let endpoint_id: i64 = row.get("id");
    sqlx::query("INSERT INTO event_endpoints (event_id, endpoint_id) VALUES (?1, ?2)")
        .bind(event_id)
        .bind(endpoint_id)
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn compute_fast_endpoint_updates_returns_only_is_fast() {
    let pool = setup_pool().await;
    let event_id = upsert_streaming_event(&pool, "ev-only-fast").await.unwrap();
    for s in 1..=30 {
        insert_chunk_raw(&pool, event_id, s, 2000, true).await;
    }
    insert_endpoint(&pool, event_id, "kiko-only", true).await;
    insert_endpoint(&pool, event_id, "fb-only", false).await;
    insert_endpoint(&pool, event_id, "yt-only", false).await;

    let updates = crate::delivery_live_edge::compute_fast_endpoint_updates(&pool, event_id)
        .await
        .expect("compute should succeed");

    assert_eq!(updates.len(), 1, "exactly one is_fast endpoint");
    let (ep, new_start) = &updates[0];
    assert_eq!(ep.alias, "kiko-only");
    assert!(ep.is_fast);
    assert_eq!(*new_start, 31, "MAX(seq where sent=1)=30, +1 = 31");
}

#[tokio::test]
async fn compute_fast_endpoint_updates_returns_max_sent_plus_one() {
    let pool = setup_pool().await;
    let event_id = upsert_streaming_event(&pool, "ev-mixed-sent")
        .await
        .unwrap();
    // 10 sent + 3 unsent.
    for s in 1..=10 {
        insert_chunk_raw(&pool, event_id, s, 2000, true).await;
    }
    for s in 11..=13 {
        insert_chunk_raw(&pool, event_id, s, 2000, false).await;
    }
    insert_endpoint(&pool, event_id, "kiko-mixed", true).await;

    let updates = crate::delivery_live_edge::compute_fast_endpoint_updates(&pool, event_id)
        .await
        .unwrap();

    assert_eq!(updates.len(), 1);
    let (_, new_start) = &updates[0];
    assert_eq!(
        *new_start, 11,
        "unsent rows ignored; MAX(sent=1)=10, +1 = 11"
    );
}

#[tokio::test]
async fn compute_fast_endpoint_updates_returns_empty_when_no_fast() {
    let pool = setup_pool().await;
    let event_id = upsert_streaming_event(&pool, "ev-no-fast").await.unwrap();
    for s in 1..=30 {
        insert_chunk_raw(&pool, event_id, s, 2000, true).await;
    }
    insert_endpoint(&pool, event_id, "fb-no-fast", false).await;
    insert_endpoint(&pool, event_id, "yt-no-fast", false).await;

    let updates = crate::delivery_live_edge::compute_fast_endpoint_updates(&pool, event_id)
        .await
        .unwrap();
    assert!(updates.is_empty(), "no fast endpoints means empty result");
}
