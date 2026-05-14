//! CRUD tests for `oauth_device_grants` (pending Device Flow state).

use crate::db::oauth_device_grants as g;
use crate::db::{create_memory_pool, run_migrations};
use chrono::Utc;

async fn pool() -> sqlx::SqlitePool {
    let p = create_memory_pool().await.unwrap();
    run_migrations(&p).await.unwrap();
    p
}

#[tokio::test]
async fn insert_then_get_by_label() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(
        &p,
        "bb",
        "DEV123",
        "USR1",
        "https://www.google.com/device",
        5,
        &exp,
        &now,
    )
    .await
    .unwrap();
    let got = g::get_by_label(&p, "bb").await.unwrap().expect("row");
    assert_eq!(got.label, "bb");
    assert_eq!(got.device_code, "DEV123");
    assert_eq!(got.user_code, "USR1");
    assert_eq!(got.verification_url, "https://www.google.com/device");
    assert_eq!(got.interval_secs, 5);
    assert_eq!(got.status, "pending");
}

#[tokio::test]
async fn insert_replaces_existing_label() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "OLD", "OLDUC", "https://x", 5, &exp, &now)
        .await
        .unwrap();
    g::insert(&p, "bb", "NEW", "NEWUC", "https://x", 5, &exp, &now)
        .await
        .unwrap();
    let got = g::get_by_label(&p, "bb").await.unwrap().expect("row");
    assert_eq!(got.device_code, "NEW");
    assert_eq!(got.user_code, "NEWUC");
}

#[tokio::test]
async fn list_pending_returns_only_pending() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "D1", "U1", "https://x", 5, &exp, &now)
        .await
        .unwrap();
    g::insert(&p, "snv", "D2", "U2", "https://x", 5, &exp, &now)
        .await
        .unwrap();
    g::update_status(&p, "snv", "granted", None).await.unwrap();
    let pending = g::list_pending(&p).await.unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].label, "bb");
}

#[tokio::test]
async fn update_status_sets_error_field() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "D", "U", "https://x", 5, &exp, &now)
        .await
        .unwrap();
    g::update_status(&p, "bb", "error", Some("invalid_grant: bad payload"))
        .await
        .unwrap();
    let got = g::get_by_label(&p, "bb").await.unwrap().expect("row");
    assert_eq!(got.status, "error");
    assert_eq!(got.error.as_deref(), Some("invalid_grant: bad payload"));
}

#[tokio::test]
async fn delete_removes_row() {
    let p = pool().await;
    let now = Utc::now().to_rfc3339();
    let exp = (Utc::now() + chrono::Duration::seconds(900)).to_rfc3339();
    g::insert(&p, "bb", "D", "U", "https://x", 5, &exp, &now)
        .await
        .unwrap();
    g::delete(&p, "bb").await.unwrap();
    assert!(g::get_by_label(&p, "bb").await.unwrap().is_none());
}
