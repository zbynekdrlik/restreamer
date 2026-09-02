//! Unit tests for the endpoint-config read/write path in `db/v2.rs`.
//!
//! #212 removed the `pusher` selector and the ffmpeg-subprocess push path;
//! `rs_rtmp_push` is the only backend now. The `pusher` TEXT column is left in
//! place on the table (inert, DEFAULT 'ffmpeg') for back-compat with existing
//! DBs, but no code reads or writes it. These tests prove:
//! - a legacy row still carrying `pusher='ffmpeg'` loads into a valid
//!   `EndpointConfig` (the column is ignored, not an error) — the removal-path
//!   regression proof;
//! - `list_endpoint_configs` returns all rows / an empty Vec correctly.

use super::*;

async fn setup() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

/// #212 regression proof: a legacy row whose `pusher` column still literally
/// holds `'ffmpeg'` (an un-migrated stale DB) must still load into a valid
/// `EndpointConfig`. After the removal the column is simply not read, so the
/// value is inert and never re-selects the deleted ffmpeg push path.
#[tokio::test]
async fn legacy_ffmpeg_pusher_column_is_ignored_on_read() {
    let pool = setup().await;
    let id = create_endpoint_config(&pool, "legacy-ffmpeg", "YT_RTMP", "key1", false)
        .await
        .unwrap();
    // Simulate a legacy/un-migrated row that still carries the old value.
    sqlx::query("UPDATE endpoint_configs SET pusher = 'ffmpeg' WHERE id = ?1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    // Must load fine — the `pusher` column is no longer selected or parsed.
    let one = get_endpoint_config(&pool, id).await.unwrap();
    assert!(one.is_some(), "legacy 'ffmpeg' row must still load");
    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].alias, "legacy-ffmpeg");
}

/// `list_endpoint_configs` on an empty table must return an empty Vec, not an
/// error. Kills the "replace -> Ok(vec![])" mutant indirectly: when rows exist
/// (covered by the test below) the function must NOT return empty.
#[tokio::test]
async fn list_endpoint_configs_empty_table_returns_empty_vec() {
    let pool = setup().await;
    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert!(
        configs.is_empty(),
        "empty table must return empty vec, not an error"
    );
}

/// Verify that multiple inserted endpoints are all returned by
/// `list_endpoint_configs`, in id order. Exercises the full row-mapping path
/// and makes the "replace -> Ok(vec![])" mutant fail (non-zero length).
#[tokio::test]
async fn list_endpoint_configs_returns_all_rows() {
    let pool = setup().await;
    create_endpoint_config(&pool, "ep-a", "YT_RTMP", "a-key", false)
        .await
        .unwrap();
    create_endpoint_config(&pool, "ep-b", "FB", "b-key", false)
        .await
        .unwrap();

    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert_eq!(
        configs.len(),
        2,
        "list_endpoint_configs must return all rows; \
         kills 'replace -> Ok(vec![])' mutant"
    );
    assert_eq!(configs[0].alias, "ep-a");
    assert_eq!(configs[1].alias, "ep-b");
}
