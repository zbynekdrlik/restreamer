//! Tests for SQLite pool initialization PRAGMAs (busy_timeout, synchronous).
//!
//! Split out of `tests.rs` to keep that file under the 1000-line file-size gate.

#[tokio::test]
async fn create_pool_sets_busy_timeout_and_synchronous() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let pool = crate::db::create_pool(tmp.path()).await.unwrap();

    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(busy_timeout, 5000, "busy_timeout must be 5000 ms");

    let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sync, 1, "synchronous must be NORMAL (1), got {sync}");
}

#[tokio::test]
async fn create_pool_enables_wal_journal_mode() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let pool = crate::db::create_pool(tmp.path()).await.unwrap();

    // WAL is the whole point of the SQLite hardening for the uploader —
    // it allows concurrent readers during a writer transaction and is
    // required for the pragma-only BUSY mitigation to hold under load.
    let journal_mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        journal_mode.to_ascii_lowercase(),
        "wal",
        "journal_mode must be WAL, got {journal_mode}"
    );
}

#[tokio::test]
async fn create_memory_pool_sets_busy_timeout() {
    let pool = crate::db::create_memory_pool().await.unwrap();
    let busy_timeout: i64 = sqlx::query_scalar("PRAGMA busy_timeout")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(busy_timeout, 5000);
}
