use crate::db::{self, insert_chunk, set_chunk_sent, upsert_streaming_event};

#[tokio::test]
async fn reset_orphaned_in_process_clears_abandoned_claims() {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "evt").await.unwrap();

    // Simulate three rows: sent (leave alone), orphaned (in_process=1, sent=0),
    // perma-failed (in_process=1 but failed_permanently=1 — leave alone).
    let c_sent = db::insert_chunk(&pool, event_id, "/tmp/s", 100, "m", 2000)
        .await
        .unwrap();
    let c_orphan = db::insert_chunk(&pool, event_id, "/tmp/o", 100, "m", 2000)
        .await
        .unwrap();
    let c_perma = db::insert_chunk(&pool, event_id, "/tmp/p", 100, "m", 2000)
        .await
        .unwrap();

    db::record_upload_success(&pool, c_sent, 1, 50)
        .await
        .unwrap();
    sqlx::query("UPDATE chunk_records SET in_process = 1 WHERE id = ?1")
        .bind(c_orphan)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE chunk_records SET in_process = 1, upload_failed_permanently = 1 WHERE id = ?1",
    )
    .bind(c_perma)
    .execute(&pool)
    .await
    .unwrap();

    let reset = db::reset_orphaned_in_process(&pool).await.unwrap();
    assert_eq!(reset, 1, "exactly one orphaned row should be reset");

    // Verify state: c_orphan is now eligible (in_process=0), c_perma stays claimed,
    // c_sent stays sent.
    let orphan_row: (i64, i64, i64) = sqlx::query_as(
        "SELECT sent, in_process, upload_failed_permanently FROM chunk_records WHERE id = ?1",
    )
    .bind(c_orphan)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(orphan_row, (0, 0, 0));

    let perma_row: (i64, i64, i64) = sqlx::query_as(
        "SELECT sent, in_process, upload_failed_permanently FROM chunk_records WHERE id = ?1",
    )
    .bind(c_perma)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        perma_row,
        (0, 1, 1),
        "permanently-failed rows keep their claim flag"
    );
}

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn migration_v17_adds_upload_telemetry_columns() {
    let pool = setup_db().await;

    // All seven upload telemetry columns must exist after migrations run.
    sqlx::query(
        "SELECT upload_attempts, upload_first_attempt_at, upload_completed_at, \
         upload_duration_ms, upload_last_error, upload_next_retry_at, \
         upload_failed_permanently FROM chunk_records LIMIT 1",
    )
    .fetch_optional(&pool)
    .await
    .expect("upload telemetry columns must exist on chunk_records");

    // The upload-queue index must exist.
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_chunks_upload_queue'",
    )
    .fetch_optional(&pool)
    .await
    .expect("sqlite_master query must succeed");

    assert!(
        row.is_some(),
        "idx_chunks_upload_queue index must exist after V17 migration"
    );
}

#[tokio::test]
async fn chunk_record_round_trips_upload_columns() {
    let pool = setup_db().await;
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();

    let event_id = db::upsert_streaming_event(&pool, "evt-1").await.unwrap();

    let chunk_id = db::insert_chunk(&pool, event_id, "/tmp/f.bin", 100_000, "md5xxxx", 2000)
        .await
        .unwrap();

    db::record_upload_attempt(&pool, chunk_id, 1735829023000)
        .await
        .unwrap();
    db::record_upload_failure(&pool, chunk_id, "timeout", 1735829024000, 1200)
        .await
        .unwrap();

    let chunks = db::get_unsent_chunks(&pool, 10).await.unwrap();
    let c = chunks
        .iter()
        .find(|c| c.id == chunk_id)
        .expect("chunk should be queryable");
    assert_eq!(c.upload_attempts, 1);
    assert!(c.upload_first_attempt_at.is_some());
    assert_eq!(c.upload_last_error.as_deref(), Some("timeout"));
    assert_eq!(c.upload_duration_ms, Some(1200));
    assert!(c.upload_next_retry_at.is_some());
    assert!(!c.upload_failed_permanently);
}

#[tokio::test]
async fn picker_skips_chunks_before_retry_time_and_claims_atomically() {
    let pool = setup_db().await;
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "evt-1").await.unwrap();

    let c1 = db::insert_chunk(&pool, event_id, "/tmp/a", 100, "m", 2000)
        .await
        .unwrap();
    let c2 = db::insert_chunk(&pool, event_id, "/tmp/b", 100, "m2", 2000)
        .await
        .unwrap();

    // c1 has retry scheduled in the future, c2 is eligible now
    db::record_upload_failure(&pool, c1, "timeout", 9_999_999_999_999, 500)
        .await
        .unwrap();

    let now_ms = 1_735_000_000_000_i64;
    let picked = db::pick_next_uploadable_chunk(&pool, now_ms).await.unwrap();
    assert_eq!(
        picked.as_ref().map(|c| c.id),
        Some(c2),
        "should pick eligible one"
    );
    // After pick, c2 is in_process=true
    let in_proc: (i64,) = sqlx::query_as("SELECT in_process FROM chunk_records WHERE id = ?1")
        .bind(c2)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(in_proc.0, 1, "picked chunk must be marked in_process");

    // A second pick returns None (c1 still in future, c2 claimed)
    let again = db::pick_next_uploadable_chunk(&pool, now_ms).await.unwrap();
    assert!(again.is_none(), "no other chunk is eligible");

    // Advancing the clock past c1's retry time lets picker claim it
    let later = 10_000_000_000_000_i64;
    let picked2 = db::pick_next_uploadable_chunk(&pool, later).await.unwrap();
    assert_eq!(picked2.as_ref().map(|c| c.id), Some(c1));
}

#[tokio::test]
async fn picker_skips_permanently_failed() {
    let pool = setup_db().await;
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "evt-1").await.unwrap();
    let c = db::insert_chunk(&pool, event_id, "/tmp/a", 100, "m", 2000)
        .await
        .unwrap();
    db::mark_upload_permanently_failed(&pool, c).await.unwrap();

    let picked = db::pick_next_uploadable_chunk(&pool, 1_000_000_000_000)
        .await
        .unwrap();
    assert!(
        picked.is_none(),
        "permanently-failed chunks must not be picked"
    );
}

#[tokio::test]
async fn list_recent_uploads_returns_status_transitions() {
    let pool = setup_db().await;
    let event_id = db::upsert_streaming_event(&pool, "evt-a").await.unwrap();

    let c1 = db::insert_chunk(&pool, event_id, "/tmp/a", 100, "m1", 2000)
        .await
        .unwrap();
    let c2 = db::insert_chunk(&pool, event_id, "/tmp/b", 200, "m2", 2000)
        .await
        .unwrap();
    let c3 = db::insert_chunk(&pool, event_id, "/tmp/c", 300, "m3", 2000)
        .await
        .unwrap();

    db::record_upload_success(&pool, c1, 123, 150)
        .await
        .unwrap();
    db::record_upload_failure(&pool, c2, "oops", 99_999_999_999_i64, 500)
        .await
        .unwrap();
    db::mark_upload_permanently_failed(&pool, c3).await.unwrap();

    let rows = db::list_recent_uploads(&pool, 10).await.unwrap();
    let by_id: std::collections::HashMap<i64, &crate::models::UploadChunkRow> =
        rows.iter().map(|r| (r.chunk_id, r)).collect();
    assert_eq!(by_id[&c1].status, "sent");
    assert_eq!(by_id[&c2].status, "retrying");
    assert_eq!(by_id[&c3].status, "failed");
    assert_eq!(by_id[&c2].last_error.as_deref(), Some("oops"));
    assert_eq!(by_id[&c1].event_identifier, "evt-a");
}

#[tokio::test]
async fn list_recent_uploads_classifies_pending_and_in_process() {
    let pool = setup_db().await;
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "evt-pending")
        .await
        .unwrap();

    // Brand-new chunk: sent=0, in_process=0, attempts=0 → "pending"
    let c_pending = db::insert_chunk(&pool, event_id, "/tmp/p", 100, "mpend", 2000)
        .await
        .unwrap();

    // Chunk in flight: attempts=0 but in_process=1 (worker claimed it atomically)
    let c_in_flight = db::insert_chunk(&pool, event_id, "/tmp/f", 100, "mflight", 2000)
        .await
        .unwrap();
    sqlx::query("UPDATE chunk_records SET in_process = 1 WHERE id = ?1")
        .bind(c_in_flight)
        .execute(&pool)
        .await
        .unwrap();

    // Chunk with attempts=1 but in_process=0 (failed, awaiting retry)
    let c_retry = db::insert_chunk(&pool, event_id, "/tmp/r", 100, "mretry", 2000)
        .await
        .unwrap();
    db::record_upload_attempt(&pool, c_retry, 1_000_000_000)
        .await
        .unwrap();
    db::record_upload_failure(&pool, c_retry, "oops", 9_999_999_999_999, 200)
        .await
        .unwrap();

    let rows = db::list_recent_uploads(&pool, 10).await.unwrap();
    let by_id: std::collections::HashMap<i64, &crate::models::UploadChunkRow> =
        rows.iter().map(|r| (r.chunk_id, r)).collect();
    assert_eq!(
        by_id[&c_pending].status, "pending",
        "brand-new chunk (sent=0, in_process=0, attempts=0) must be 'pending'"
    );
    assert_eq!(
        by_id[&c_in_flight].status, "retrying",
        "in_process=1 alone must flip to 'retrying'"
    );
    assert_eq!(
        by_id[&c_retry].status, "retrying",
        "attempts=1 with in_process=0 must be 'retrying'"
    );
}

#[tokio::test]
async fn list_recent_uploads_attempts_zero_is_pending_not_retrying() {
    // Specifically targets the `attempts > 0` mutation: if `>` became `>=`,
    // a chunk with attempts=0 would wrongly classify as 'retrying'.
    let pool = setup_db().await;
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "evt-boundary")
        .await
        .unwrap();
    let c = db::insert_chunk(&pool, event_id, "/tmp/x", 100, "mboundary", 2000)
        .await
        .unwrap();

    let rows = db::list_recent_uploads(&pool, 10).await.unwrap();
    let row = rows.iter().find(|r| r.chunk_id == c).unwrap();
    assert_eq!(
        row.status, "pending",
        "attempts=0 must be 'pending', not 'retrying'"
    );
    assert_eq!(row.attempts, 0);
}

#[tokio::test]
async fn list_recent_uploads_attempts_gt_zero_no_last_error_is_retrying() {
    // The condition is `in_proc == 1 || attempts > 0 || last_error.is_some()`.
    // A mutant changes `>` to `<` (attempts > 0 → attempts < 0).
    // This test has attempts=1 and last_error=NULL so only `attempts > 0` can
    // fire, proving the `>` comparison is tested independently of last_error.
    let pool = setup_db().await;
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    let event_id = db::upsert_streaming_event(&pool, "evt-gt-zero")
        .await
        .unwrap();
    let c = db::insert_chunk(&pool, event_id, "/tmp/gt", 100, "mgt", 2000)
        .await
        .unwrap();
    // Directly bump attempts without setting last_error (no record_upload_failure call)
    sqlx::query("UPDATE chunk_records SET upload_attempts = 1, in_process = 0 WHERE id = ?1")
        .bind(c)
        .execute(&pool)
        .await
        .unwrap();

    let rows = db::list_recent_uploads(&pool, 10).await.unwrap();
    let row = rows.iter().find(|r| r.chunk_id == c).unwrap();
    assert_eq!(
        row.status, "retrying",
        "attempts=1 with in_process=0 and no last_error must be 'retrying'"
    );
    assert_eq!(row.attempts, 1);
    assert!(
        row.last_error.is_none(),
        "last_error must be NULL so only attempts > 0 drives the classification"
    );
}

// --- cache metric cap + compute_target_start_chunk tests ---

#[tokio::test]
async fn cache_duration_capped_at_target_during_warmup() {
    let pool = setup_db_for_start_chunk().await;
    let event_id = upsert_streaming_event(&pool, "cache-cap-test")
        .await
        .unwrap();

    // Insert 5 chunks with 1000ms each (5s total)
    let mut ids = Vec::new();
    for i in 1..=5 {
        let id = insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/cap{i}.ts"),
            1000,
            &format!("md5cap{i}"),
            1000,
        )
        .await
        .unwrap();
        ids.push(id);
    }
    for &id in &ids {
        set_chunk_sent(&pool, id).await.unwrap();
    }

    // delivered_up_to=0 (warmup), target=2.0s → capped at 2.0 (raw=5.0)
    let dur = db::get_cache_duration_secs(&pool, event_id, 0, 2.0)
        .await
        .unwrap();
    assert!((dur - 2.0).abs() < 0.001, "warmup: expected 2.0, got {dur}");

    // delivered_up_to=0 (warmup), target=10.0s → raw 5.0 < target, no cap
    let dur = db::get_cache_duration_secs(&pool, event_id, 0, 10.0)
        .await
        .unwrap();
    assert!(
        (dur - 5.0).abs() < 0.001,
        "warmup below target: expected 5.0, got {dur}"
    );

    // delivered_up_to=1 (VPS playing), target=2.0s → NO cap, raw = 4.0
    let dur = db::get_cache_duration_secs(&pool, event_id, 1, 2.0)
        .await
        .unwrap();
    assert!(
        (dur - 4.0).abs() < 0.001,
        "playing: expected 4.0 (uncapped), got {dur}"
    );
}

async fn setup_db_for_start_chunk() -> db::SqlitePool {
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    pool
}

#[tokio::test]
async fn compute_target_start_chunk_returns_first_when_content_below_target() {
    let pool = setup_db_for_start_chunk().await;
    let event_id = upsert_streaming_event(&pool, "start-chunk-below")
        .await
        .unwrap();

    // No chunks → default 1
    let start = db::compute_target_start_chunk(&pool, event_id, 120_000)
        .await
        .unwrap();
    assert_eq!(start, 1);

    // 3 chunks × 4000ms = 12s total, target 120s → returns first_seq (not enough content)
    for i in 1..=3 {
        let id = insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/b{i}.ts"),
            1000,
            &format!("b{i}"),
            4000,
        )
        .await
        .unwrap();
        set_chunk_sent(&pool, id).await.unwrap();
    }
    let start = db::compute_target_start_chunk(&pool, event_id, 120_000)
        .await
        .unwrap();
    assert_eq!(start, 1, "content < target: should return first seq");
}

#[tokio::test]
async fn compute_target_start_chunk_skips_old_when_content_exceeds_target() {
    let pool = setup_db_for_start_chunk().await;
    let event_id = upsert_streaming_event(&pool, "start-chunk-above")
        .await
        .unwrap();

    // 10 chunks × 4000ms = 40s total, target = 12s → should start at chunk 8
    // (chunks 8,9,10 = 12s)
    for i in 1..=10 {
        let id = insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/a{i}.ts"),
            1000,
            &format!("a{i}"),
            4000,
        )
        .await
        .unwrap();
        set_chunk_sent(&pool, id).await.unwrap();
    }

    let start = db::compute_target_start_chunk(&pool, event_id, 12_000)
        .await
        .unwrap();
    assert_eq!(
        start, 8,
        "40s content, 12s target: should start at seq 8 (chunks 8,9,10 = 12s)"
    );
}

#[tokio::test]
async fn compute_target_start_chunk_exact_match() {
    let pool = setup_db_for_start_chunk().await;
    let event_id = upsert_streaming_event(&pool, "start-chunk-exact")
        .await
        .unwrap();

    // 5 chunks × 4000ms = 20s, target = 20s → returns first seq (exact match)
    for i in 1..=5 {
        let id = insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/e{i}.ts"),
            1000,
            &format!("e{i}"),
            4000,
        )
        .await
        .unwrap();
        set_chunk_sent(&pool, id).await.unwrap();
    }

    let start = db::compute_target_start_chunk(&pool, event_id, 20_000)
        .await
        .unwrap();
    assert_eq!(start, 1, "exact match: should return first seq");
}
