use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn get_pending_chunk_count_for_event_excludes_permanently_failed() {
    // Sister regression to `pending_chunks_excludes_permanently_failed`:
    // dashboard's `local_buffer_chunks` (rs-api/src/lib.rs:311) reads
    // get_pending_chunk_count_for_event and was inflated by dead chunks
    // from prior runs for the same exact reason.
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-failperm-2")
        .await
        .unwrap();

    let _live = insert_chunk(&pool, event_id, "/tmp/live.bin", 100, "live", 0)
        .await
        .unwrap();
    let dead = insert_chunk(&pool, event_id, "/tmp/dead.bin", 100, "dead", 0)
        .await
        .unwrap();

    upload::mark_upload_permanently_failed(&pool, dead)
        .await
        .unwrap();

    let count = get_pending_chunk_count_for_event(&pool, event_id)
        .await
        .unwrap();
    assert_eq!(
        count, 1,
        "dead chunk must NOT count as pending; only the live one should"
    );
}

#[tokio::test]
async fn count_permanently_failed_since_only_counts_recent() {
    // Issue #168: dashboard upload-strip needs a "permanent failures in
    // the last 5 min" count to escalate from yellow (transient burst)
    // to red. The counter must use upload_first_attempt_at as its
    // timestamp anchor (the chunk's failure window starts when the
    // first attempt fired) and must EXCLUDE chunks marked permanent
    // before the cutoff so old failures don't keep the strip red
    // forever.
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-permcount")
        .await
        .unwrap();

    // Chunk A: marked permanent 1 hour ago — should NOT count.
    let old = insert_chunk(&pool, event_id, "/tmp/old.bin", 100, "old", 0)
        .await
        .unwrap();
    upload::record_upload_attempt(&pool, old, 0).await.unwrap();
    sqlx::query("UPDATE chunk_records SET upload_first_attempt_at = ?1 WHERE id = ?2")
        .bind(1_000_000_000_000_i64) // way in the past
        .bind(old)
        .execute(&pool)
        .await
        .unwrap();
    upload::mark_upload_permanently_failed(&pool, old)
        .await
        .unwrap();

    // Chunk B: marked permanent 2 min ago (recent) — SHOULD count.
    let recent = insert_chunk(&pool, event_id, "/tmp/recent.bin", 100, "recent", 0)
        .await
        .unwrap();
    let now_ms = 2_000_000_000_000_i64;
    let two_min_ago = now_ms - 2 * 60 * 1000;
    upload::record_upload_attempt(&pool, recent, two_min_ago)
        .await
        .unwrap();
    upload::mark_upload_permanently_failed(&pool, recent)
        .await
        .unwrap();

    // Chunk C: live (NOT permanent) — should NEVER count.
    let _live = insert_chunk(&pool, event_id, "/tmp/live.bin", 100, "live", 0)
        .await
        .unwrap();

    // Cutoff = now - 5 min. Recent chunk B (2 min old) is in window;
    // old chunk A (1 hour) is excluded; live chunk C is excluded.
    let since = now_ms - 5 * 60 * 1000;
    let n = upload::count_permanently_failed_since(&pool, since)
        .await
        .unwrap();
    assert_eq!(
        n, 1,
        "only the chunk marked permanent within the 5-min window should count"
    );

    // Sanity: a wide-open since should count both permanent chunks.
    let n_all = upload::count_permanently_failed_since(&pool, 0)
        .await
        .unwrap();
    assert_eq!(n_all, 2);
}

#[tokio::test]
async fn count_permanently_failed_since_zero_when_none() {
    let pool = setup_db().await;
    let n = upload::count_permanently_failed_since(&pool, 0)
        .await
        .unwrap();
    assert_eq!(n, 0);
}

#[tokio::test]
async fn pending_chunks_excludes_permanently_failed() {
    // E2E-gate regression: chunks with upload_failed_permanently=1 used to
    // count as `pending_chunks`. Two dead chunks from a prior CI run made
    // every subsequent E2E run hit "FAILED: 2 chunks still pending S3 upload".
    // pending_chunks must mirror the uploader's pick criteria.
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-failperm").await.unwrap();

    let live = insert_chunk(&pool, event_id, "/tmp/live.bin", 100, "live", 0)
        .await
        .unwrap();
    let dead = insert_chunk(&pool, event_id, "/tmp/dead.bin", 100, "dead", 0)
        .await
        .unwrap();

    upload::mark_upload_permanently_failed(&pool, dead)
        .await
        .unwrap();

    let stats = get_chunk_stats(&pool, 1000).await.unwrap();
    assert_eq!(stats.total_chunks, 2);
    assert_eq!(
        stats.pending_chunks, 1,
        "dead chunk must NOT count as pending; only the live one ({live}) should"
    );
    assert_eq!(stats.sent_chunks, 0);
    assert_eq!(stats.in_process_chunks, 0);
}
