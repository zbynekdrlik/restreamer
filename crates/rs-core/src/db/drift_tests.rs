use super::*;

async fn setup_db() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn insert_chunk_with_walltime_persists_wall_clock() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-drift-1").await.unwrap();

    let id = drift::insert_chunk_with_walltime(
        &pool,
        event_id,
        "/tmp/c.bin",
        4096,
        "md5x",
        1000,
        1_700_000_000_000,
    )
    .await
    .unwrap();

    let wc: i64 =
        sqlx::query_scalar("SELECT wall_clock_written_at_ms FROM chunk_records WHERE id = ?1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(wc, 1_700_000_000_000);
}

#[tokio::test]
async fn list_chunk_producer_rate_computes_ratio() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-drift-2").await.unwrap();

    // Two chunks, each 1000ms of content, written 1010ms wall-clock apart.
    drift::insert_chunk_with_walltime(&pool, event_id, "/tmp/a", 1, "a", 1000, 1_000_000)
        .await
        .unwrap();
    drift::insert_chunk_with_walltime(&pool, event_id, "/tmp/b", 1, "b", 1000, 1_001_010)
        .await
        .unwrap();

    let series = drift::list_chunk_producer_rate(&pool, event_id, 0)
        .await
        .unwrap();
    assert_eq!(series.len(), 1);
    // 1000ms ts / 1010ms wall ≈ 0.990099 ratio (producer slightly slow)
    assert!(
        (series[0].value - 0.990).abs() < 0.001,
        "ratio {} not near 0.990",
        series[0].value
    );
}

#[tokio::test]
async fn list_chunk_producer_rate_empty_without_walltime() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-drift-3").await.unwrap();

    // Insert a chunk the old way (no wall_clock_written_at_ms).
    insert_chunk(&pool, event_id, "/tmp/old.bin", 512, "oldhash", 1000)
        .await
        .unwrap();

    let series = drift::list_chunk_producer_rate(&pool, event_id, 0)
        .await
        .unwrap();
    // Old-style chunks have NULL wall_clock_written_at_ms, so they are excluded.
    assert!(series.is_empty());
}

#[tokio::test]
async fn list_clock_skew_returns_samples() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-drift-4").await.unwrap();

    drift::insert_clock_skew_sample(&pool, event_id, 1_000, 900, 950, 901, 50, 1)
        .await
        .unwrap();
    drift::insert_clock_skew_sample(&pool, event_id, 2_000, 1900, 1960, 1901, 60, 2)
        .await
        .unwrap();

    let series = drift::list_clock_skew(&pool, event_id, 0).await.unwrap();
    assert_eq!(series.len(), 2);
    assert_eq!(series[0].t_ms, 1_000);
    assert!((series[0].value - 50.0).abs() < f64::EPSILON);
    assert_eq!(series[1].t_ms, 2_000);
    assert!((series[1].value - 60.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn list_ffmpeg_consumer_rate_computes_ratio() {
    let pool = setup_db().await;
    let event_id = upsert_streaming_event(&pool, "evt-drift-5").await.unwrap();

    // Two progress samples: media time advances 990ms while wall-clock advances 1000ms.
    drift::insert_ffmpeg_progress_sample(&pool, event_id, "YT_RTMP", 1_000, 0, 1_000_000)
        .await
        .unwrap();
    drift::insert_ffmpeg_progress_sample(&pool, event_id, "YT_RTMP", 2_000, 990, 1_001_000)
        .await
        .unwrap();

    let series = drift::list_ffmpeg_consumer_rate(&pool, event_id, "YT_RTMP", 0)
        .await
        .unwrap();
    assert_eq!(series.len(), 1);
    // 990ms media / 1000ms wall = 0.990 ratio (consumer slightly slow)
    assert!(
        (series[0].value - 0.990).abs() < 0.001,
        "ratio {} not near 0.990",
        series[0].value
    );
}
