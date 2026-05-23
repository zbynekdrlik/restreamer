//! Outage continuity: a network outage longer than the OLD 10-attempt cap
//! must NOT permanently drop any chunk. A mock S3 returns 503 for the first
//! 15 PUTs (well past the old 10-attempt budget), then 200. The chunk must
//! end up uploaded, with ZERO permanently-failed rows.
//!
//! This drives the REAL uploader path (`upload_one` → `should_abandon_upload`)
//! via `rs_endpoint::testing::run_uploader_until_idle`, so it genuinely fails
//! if anyone reintroduced an attempt-based permanent-drop for network classes:
//! a `5xx` error abandoned before attempt 16 would leave the chunk
//! permanently-failed (assert 1) and pending (assert 2).
//!
//! Requires the `testing` feature (CI: `cargo test -p rs-endpoint --features testing`).
#![cfg(feature = "testing")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::put;
use rs_core::db;
use tokio::net::TcpListener;

/// Number of PUTs that fail with 503 before the mock starts returning 200.
/// 15 is comfortably past the OLD 10-attempt permanent-drop cap.
const FAIL_PUTS: usize = 15;

#[derive(Clone)]
struct MockS3 {
    puts: Arc<AtomicUsize>,
}

/// First `FAIL_PUTS` PUTs return 503 (classifies as "5xx" → retry forever),
/// then 200.
async fn handle_put(
    State(s): State<MockS3>,
    Path((_bucket, _key)): Path<(String, String)>,
    _body: Bytes,
) -> StatusCode {
    let n = s.puts.fetch_add(1, Ordering::SeqCst);
    if n < FAIL_PUTS {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

#[tokio::test]
async fn outage_longer_than_old_cap_drops_nothing() {
    // 1. Start a mock S3 on an ephemeral port.
    let puts = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route("/{bucket}/{*key}", put(handle_put))
        .with_state(MockS3 { puts: puts.clone() });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let endpoint = format!("http://{addr}");

    // 2. In-memory chunk DB with one event + one chunk on a temp file.
    let pool = db::create_memory_pool().await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
    db::upsert_streaming_event(&pool, "test-outage")
        .await
        .unwrap();
    let event = db::get_streaming_event(&pool).await.unwrap().unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chunk1.flv");
    std::fs::write(&path, b"FLVchunkbytes").unwrap();
    let chunk_id = db::insert_chunk(
        &pool,
        event.id,
        path.to_str().unwrap(),
        13,
        "deadbeef",
        2000,
    )
    .await
    .unwrap();

    // 3. Drive the REAL uploader against the mock S3 until idle. Backoff is
    //    accelerated by the test driver (5ms), so >15 retries finish fast.
    rs_endpoint::testing::run_uploader_until_idle(&pool, &endpoint, "test-bucket")
        .await
        .expect("uploader driver must reach idle before the deadline");

    // 4a. ZERO chunks may be abandoned during a network outage.
    let permanent = db::count_permanently_failed_since(&pool, 0).await.unwrap();
    assert_eq!(
        permanent, 0,
        "no chunk may be permanently abandoned during a network (5xx) outage"
    );

    // 4b. The chunk must eventually upload once S3 recovers.
    let pending = db::get_pending_chunk_count_for_event(&pool, event.id)
        .await
        .unwrap();
    assert_eq!(
        pending, 0,
        "the buffered chunk must upload after S3 recovers (none left pending)"
    );

    // 4c. The chunk is actually marked sent (not skipped via a missing event).
    let sent: i64 = sqlx::query_scalar("SELECT sent FROM chunk_records WHERE id = ?1")
        .bind(chunk_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(sent, 1, "the chunk must be marked sent after upload");

    // 4d. The uploader must have retried PAST the old 10-attempt cap — proving
    //     the never-drop behavior, not an early give-up that masked the bug.
    let total_puts = puts.load(Ordering::SeqCst);
    assert!(
        total_puts > FAIL_PUTS,
        "uploader must have retried past the old cap (saw {total_puts} PUTs, expected > {FAIL_PUTS})"
    );
}
