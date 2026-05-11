use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

async fn insert_event(pool: &sqlx::sqlite::SqlitePool, name: &str) -> i64 {
    upsert_streaming_event(pool, name).await.unwrap()
}

/// Raw INSERT that lets the test control sequence_number AND sent directly.
/// Production code goes through insert_chunk (auto-seq, sent updated later
/// via upload::mark_chunk_uploaded). Tests for get_endpoint_lag_secs need
/// to control both fields explicitly to set up specific scenarios.
async fn insert_chunk_raw(
    pool: &sqlx::sqlite::SqlitePool,
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

#[tokio::test]
async fn endpoint_lag_zero_at_live_edge() {
    let pool = setup_db().await;
    let ev = insert_event(&pool, "ev-zero-at-edge").await;
    for s in 1..=3 {
        insert_chunk_raw(&pool, ev, s, 2000, true).await;
    }
    let lag = get_endpoint_lag_secs(&pool, ev, 3).await.unwrap();
    assert!(
        (lag - 0.0).abs() < f64::EPSILON,
        "expected 0.0 at live edge, got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_120s_when_60_chunks_behind() {
    let pool = setup_db().await;
    let ev = insert_event(&pool, "ev-120-behind").await;
    for s in 1..=60 {
        insert_chunk_raw(&pool, ev, s, 2000, true).await;
    }
    let lag = get_endpoint_lag_secs(&pool, ev, 0).await.unwrap();
    assert!(
        (lag - 120.0).abs() < f64::EPSILON,
        "expected 120.0s lag when 60 chunks behind, got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_ignores_unsent_chunks() {
    let pool = setup_db().await;
    let ev = insert_event(&pool, "ev-ignore-unsent").await;
    insert_chunk_raw(&pool, ev, 1, 2000, true).await;
    insert_chunk_raw(&pool, ev, 2, 2000, true).await;
    insert_chunk_raw(&pool, ev, 3, 2000, false).await; // not yet sent
    let lag = get_endpoint_lag_secs(&pool, ev, 1).await.unwrap();
    assert!(
        (lag - 2.0).abs() < f64::EPSILON,
        "expected 2.0s (only sent chunks count), got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_zero_when_no_chunks_sent() {
    let pool = setup_db().await;
    let ev = insert_event(&pool, "ev-no-chunks").await;
    let lag = get_endpoint_lag_secs(&pool, ev, 0).await.unwrap();
    assert!(
        (lag - 0.0).abs() < f64::EPSILON,
        "expected 0.0 with no sent chunks, got {lag}"
    );
}

#[tokio::test]
async fn endpoint_lag_excludes_rows_beyond_live_edge() {
    let pool = setup_db().await;
    let ev = insert_event(&pool, "ev-clamp-live-edge").await;
    for s in 1..=3 {
        insert_chunk_raw(&pool, ev, s, 1000, true).await;
    }
    insert_chunk_raw(&pool, ev, 4, 1000, false).await;
    // Live edge = MAX(seq where sent=1) = 3. Endpoint at 1. Lag = chunks 2,3 = 2000ms.
    let lag = get_endpoint_lag_secs(&pool, ev, 1).await.unwrap();
    assert!(
        (lag - 2.0).abs() < f64::EPSILON,
        "expected 2.0s (live edge clamped to seq 3), got {lag}"
    );
}
