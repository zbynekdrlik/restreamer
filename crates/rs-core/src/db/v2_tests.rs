//! Unit tests for `parse_pusher_kind` in `db/v2.rs`.
//! Kills the surviving mutants:
//! - "delete match arm 'rust'" -> Rust variant would map to default (Ffmpeg)
//! - "replace -> Default::default()" -> all variants would return Ffmpeg
//!
//! `parse_pusher_kind` is private, so we test it indirectly via
//! `list_endpoint_configs` which calls it on every row.

use super::*;
use crate::models::PusherKind;

async fn setup() -> sqlx::sqlite::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

/// Inserting an endpoint with pusher="rust" and reading it back must yield
/// `PusherKind::Rust`, not `PusherKind::Ffmpeg`.
/// Kills the "delete match arm 'rust'" mutant.
#[tokio::test]
async fn parse_pusher_kind_rust_roundtrip() {
    let pool = setup().await;
    let id = create_endpoint_config(&pool, "yt-rust", "YT_RTMP", "key1", false)
        .await
        .unwrap();
    // Set pusher = "rust" directly — create_endpoint_config defaults to "ffmpeg".
    sqlx::query("UPDATE endpoint_configs SET pusher = 'rust' WHERE id = ?1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(
        configs[0].pusher,
        PusherKind::Rust,
        "pusher='rust' in DB must deserialize to PusherKind::Rust; \
         kills 'delete match arm rust' and 'replace -> Default::default()' mutants"
    );
}

/// Inserting an endpoint with pusher="ffmpeg" must yield `PusherKind::Ffmpeg`.
#[tokio::test]
async fn parse_pusher_kind_ffmpeg_roundtrip() {
    let pool = setup().await;
    let id = create_endpoint_config(&pool, "fb-ffmpeg", "Facebook", "key2", false)
        .await
        .unwrap();
    sqlx::query("UPDATE endpoint_configs SET pusher = 'ffmpeg' WHERE id = ?1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(configs[0].pusher, PusherKind::Ffmpeg);
}

/// An unknown pusher value must fall back to `PusherKind::Ffmpeg` (the default).
#[tokio::test]
async fn parse_pusher_kind_unknown_defaults_to_ffmpeg() {
    let pool = setup().await;
    let id = create_endpoint_config(&pool, "vimeo-unk", "VIMEO", "key3", false)
        .await
        .unwrap();
    sqlx::query("UPDATE endpoint_configs SET pusher = 'unknown_backend' WHERE id = ?1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(
        configs[0].pusher,
        PusherKind::Ffmpeg,
        "unknown pusher string must fall back to PusherKind::Ffmpeg (the wildcard arm)"
    );
}

/// `list_endpoint_configs` on an empty table must return an empty Vec, not an
/// error. Kills the "replace -> Ok(vec![])" mutant indirectly: when rows exist
/// (covered by the tests above) the function must NOT return empty.
#[tokio::test]
async fn list_endpoint_configs_empty_table_returns_empty_vec() {
    let pool = setup().await;
    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert!(
        configs.is_empty(),
        "empty table must return empty vec, not an error"
    );
}

/// Verify that two endpoints inserted with different pusher kinds are both
/// returned by `list_endpoint_configs` with the correct kinds, in id order.
/// This exercises the full row-mapping path and makes the "replace ->
/// Ok(vec![])" mutant fail because the returned vec has a non-zero length.
#[tokio::test]
async fn list_endpoint_configs_returns_all_rows_with_correct_pusher() {
    let pool = setup().await;
    let id1 = create_endpoint_config(&pool, "ep-rust", "YT_RTMP", "r-key", false)
        .await
        .unwrap();
    let id2 = create_endpoint_config(&pool, "ep-ffmpeg", "Facebook", "f-key", false)
        .await
        .unwrap();

    sqlx::query("UPDATE endpoint_configs SET pusher = 'rust' WHERE id = ?1")
        .bind(id1)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE endpoint_configs SET pusher = 'ffmpeg' WHERE id = ?1")
        .bind(id2)
        .execute(&pool)
        .await
        .unwrap();

    let configs = list_endpoint_configs(&pool).await.unwrap();
    assert_eq!(
        configs.len(),
        2,
        "list_endpoint_configs must return all rows; \
         kills 'replace -> Ok(vec![])' mutant"
    );
    assert_eq!(configs[0].pusher, PusherKind::Rust);
    assert_eq!(configs[1].pusher, PusherKind::Ffmpeg);
}
