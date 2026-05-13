//! Tests for `crate::db::youtube_oauth` multi-account ops.

use crate::db::youtube_oauth as yo;
use crate::db::{create_memory_pool, run_migrations};

async fn fresh_pool() -> sqlx::SqlitePool {
    let pool = create_memory_pool().await.unwrap();
    run_migrations(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn upsert_by_label_inserts_then_updates_same_row() {
    let pool = fresh_pool().await;
    let id1 = yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "a1",
        "r1",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "scope1",
        Some("2026-05-12T00:00:00Z"),
    )
    .await
    .unwrap();
    let id2 = yo::upsert_oauth_by_label(
        &pool,
        "bb",
        "a2",
        "r2",
        "https://oauth2.googleapis.com/token",
        "cid",
        "csec",
        "scope2",
        Some("2027-01-01T00:00:00Z"),
    )
    .await
    .unwrap();
    assert_eq!(
        id1, id2,
        "upsert by label must update same row, not duplicate"
    );

    let row = yo::get_oauth_by_label(&pool, "bb").await.unwrap().unwrap();
    assert_eq!(row.access_token, "a2");
    assert_eq!(row.scopes, "scope2");
}

#[tokio::test]
async fn get_by_label_returns_none_for_unknown_label() {
    let pool = fresh_pool().await;
    assert!(
        yo::get_oauth_by_label(&pool, "nope")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn get_by_id_resolves_correct_label() {
    let pool = fresh_pool().await;
    let id = yo::upsert_oauth_by_label(&pool, "bb", "a", "r", "u", "c", "s", "sc", None)
        .await
        .unwrap();
    let row = yo::get_oauth_by_id(&pool, id).await.unwrap().unwrap();
    assert_eq!(row.id, id);
    assert_eq!(row.access_token, "a");
}

#[tokio::test]
async fn list_returns_default_and_bb() {
    let pool = fresh_pool().await;
    yo::upsert_oauth_by_label(&pool, "bb", "a", "r", "u", "c", "s", "sc", None)
        .await
        .unwrap();
    let all = yo::list_oauths(&pool).await.unwrap();
    let labels: Vec<&str> = all.iter().map(|o| o.label.as_str()).collect();
    assert!(labels.contains(&"default"), "have {labels:?}");
    assert!(labels.contains(&"bb"), "have {labels:?}");
}
