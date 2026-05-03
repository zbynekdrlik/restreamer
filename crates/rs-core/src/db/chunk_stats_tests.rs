use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
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
