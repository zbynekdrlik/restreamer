//! Tests for SQLite pool initialization PRAGMAs (busy_timeout, synchronous).
//!
//! Split out of `tests.rs` to keep that file under the 1000-line file-size gate.

/// A file-backed test DB. Dropping it must remove the DB and its SQLite
/// `-wal`/`-shm` siblings, even while a pool still holds the file open (#421:
/// 17 GB of orphaned WALs in dev2's /tmp).
/// The DB lives in its own directory, and the directory's drop removes all
/// three files.
struct TempDb {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
}

fn temp_db() -> TempDb {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.db");
    TempDb { _dir: dir, path }
}

fn sibling(db: &std::path::Path, suffix: &str) -> std::path::PathBuf {
    let mut name = db.as_os_str().to_owned();
    name.push(suffix);
    std::path::PathBuf::from(name)
}

// A WAL that is still open cannot be deleted on Windows; the leak is on the
// Linux boxes (dev2, the Linux runner).
#[cfg(unix)]
#[tokio::test]
async fn temp_db_drop_leaves_no_wal_or_shm() {
    let db = temp_db();
    let path = db.path.clone();
    let pool = crate::db::create_pool(&path).await.unwrap();
    sqlx::query("CREATE TABLE t (v INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO t VALUES (1)")
        .execute(&pool)
        .await
        .unwrap();
    let wal = sibling(&path, "-wal");
    let shm = sibling(&path, "-shm");
    assert!(wal.exists(), "the write must create {}", wal.display());

    // A test never closes its pool: the process exits with it open.
    drop(db);

    assert!(!path.exists(), "{} survived the drop", path.display());
    assert!(!wal.exists(), "{} survived the drop", wal.display());
    assert!(!shm.exists(), "{} survived the drop", shm.display());
    drop(pool);
}

#[tokio::test]
async fn create_pool_sets_busy_timeout_and_synchronous() {
    let db = temp_db();
    let pool = crate::db::create_pool(&db.path).await.unwrap();

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
    let db = temp_db();
    let pool = crate::db::create_pool(&db.path).await.unwrap();

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
