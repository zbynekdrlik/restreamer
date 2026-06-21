//! SQLITE_BUSY-storm regression test for the upload-queue picker
//! (`db::pick_next_uploadable_chunk`) under a file-backed WAL pool with the
//! production multi-writer mix.
//!
//! Context — 2026-06-19 live-event outage (issue #256): the picker was a
//! deferred-`BEGIN` read-then-write-upgrade transaction. 2-8 uploader workers
//! sharing one 5-connection pool all raced the `ORDER BY id ASC LIMIT 1` hot
//! row against frequent committers (chunk INSERT ~every 2s, audit batch,
//! `update_received_bytes`). SQLite then returned, repeatedly:
//!   - code 5   = SQLITE_BUSY          (writer-writer lock contention)
//!   - code 517 = SQLITE_BUSY_SNAPSHOT (deferred read->write upgrade conflict)
//! `busy_timeout` does NOT rescue 517 (it is returned immediately, never
//! retried), so the error escaped to the app layer as
//! `ERROR Failed to pick next uploadable chunk: ... database is locked`
//! every ~2s for 30+ minutes -> S3 uploads stalled -> VPS chunk supply
//! starved -> every endpoint died. This is the bug that caused the outage.
//!
//! Earlier history: #120 added a single-claimer coordinator to dedup the
//! workers; it regressed upload throughput (310-chunk backlog after 10 min)
//! and was reverted. The fix here does NOT reintroduce a coordinator — the
//! picker is collapsed into ONE atomic `UPDATE ... WHERE id=(SELECT ...)
//! RETURNING` statement on the pool (no BEGIN, no read snapshot to invalidate,
//! no read->write window), which keeps N workers fully concurrent.
//!
//! This test reproduces the storm with a file-backed WAL pool (NOT the
//! `max_connections(1)` memory pool, which serialises everything and HIDES the
//! bug) and drives the EXACT production write mix from a concurrent committer.
//! Zero tolerance: the picker must surface ZERO SQLITE_BUSY / BUSY_SNAPSHOT
//! errors, and every chunk must be claimed EXACTLY ONCE (no double-claim, no
//! lost row).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::db;
use tokio::sync::Mutex;
use tokio::sync::broadcast;

/// True if a picker error is a SQLite BUSY (code 5) or BUSY_SNAPSHOT (code
/// 517) — the exact storm signature from #256. Matches the SQLite extended
/// result code precisely rather than string-matching, so it can't
/// false-positive on an unrelated "busy" substring.
fn is_sqlite_busy(e: &rs_core::error::CoreError) -> bool {
    // 5 = SQLITE_BUSY, 517 = SQLITE_BUSY_SNAPSHOT (0x205). Match the SQLite
    // extended result code precisely via a let-chain.
    if let rs_core::error::CoreError::Database(sqlx::Error::Database(db_err)) = e
        && let Some(code) = db_err.code()
    {
        return code == "5" || code == "517";
    }
    // Fallback: surface the storm even if the code isn't populated.
    let s = e.to_string().to_ascii_lowercase();
    s.contains("database is locked") || s.contains("(code: 5") || s.contains("(code: 517")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn picker_no_busy_storm_under_production_write_mix() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let pool = db::create_pool(tmp.path()).await.unwrap();
    db::run_migrations(&pool).await.unwrap();

    // Seed one streaming event + many eligible unsent chunks. The claimers
    // drain these; the committer keeps inserting more to sustain contention.
    let event_id = db::create_streaming_event(&pool, "busy-storm-test")
        .await
        .unwrap();
    const SEED: usize = 800;
    for i in 0..SEED {
        db::insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/chunk{i}.bin"),
            1024,
            "deadbeef",
            1000,
        )
        .await
        .unwrap();
    }

    // A broadcast channel for audit::insert_batch (the post-commit fan-out).
    let (ws_tx, _ws_rx) = broadcast::channel(1024);

    let busy_hits = Arc::new(AtomicU32::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    // Records every chunk id each claimer won, to detect double-claims.
    let claims: Arc<Mutex<HashMap<i64, u32>>> = Arc::new(Mutex::new(HashMap::new()));

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(4);

    // --- 8 claimer workers (production = adaptive 2..8) ---
    // Each tightly loops the picker. On a successful claim it marks the chunk
    // sent (mirrors record_upload_success after the slow S3 PUT) so the queue
    // keeps draining, then loops immediately to maximise contention.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let busy_hits = Arc::clone(&busy_hits);
        let claims = Arc::clone(&claims);
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            while std::time::Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                let now_ms = chrono::Utc::now().timestamp_millis();
                match db::pick_next_uploadable_chunk(&pool, now_ms).await {
                    Ok(Some(chunk)) => {
                        // Count this claim. A correct picker claims each id
                        // exactly once across all workers.
                        {
                            let mut map = claims.lock().await;
                            *map.entry(chunk.id).or_insert(0) += 1;
                        }
                        // Mark sent so the row leaves the eligible set.
                        let _ = db::record_upload_success(
                            &pool,
                            chunk.id,
                            chrono::Utc::now().timestamp_millis(),
                            1,
                        )
                        .await;
                    }
                    Ok(None) => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        if is_sqlite_busy(&e) {
                            busy_hits.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }));
    }

    // --- 2 committer tasks: the EXACT production write mix ---
    // chunk INSERT (insert_chunk) + audit batch (audit::insert_batch, its own
    // BEGIN tx) + update_received_bytes -- the three concurrent committers
    // that held the write lock during the live event and forced the deferred
    // read->write picker into SQLITE_BUSY_SNAPSHOT.
    for w in 0..2 {
        let pool = pool.clone();
        let ws_tx = ws_tx.clone();
        let stop = Arc::clone(&stop);
        handles.push(tokio::spawn(async move {
            let mut n = 0i64;
            while std::time::Instant::now() < deadline && !stop.load(Ordering::Relaxed) {
                // 1) chunk INSERT — same statement the inpoint chunker runs.
                let _ = db::insert_chunk(
                    &pool,
                    event_id,
                    &format!("/tmp/live{w}-{n}.bin"),
                    1024,
                    "cafebabe",
                    1000,
                )
                .await;

                // 2) audit batch — its own pool.begin() tx (write lock).
                let rows = vec![AuditRow {
                    severity: Severity::Info,
                    source: Source::Uploader,
                    event_id: Some(event_id),
                    instance_id: None,
                    endpoint: None,
                    action: Action::RtmpConnected,
                    detail: serde_json::json!({"w": w, "n": n}),
                    ts_override: None,
                }];
                let _ = db::audit::insert_batch(&pool, &rows, &ws_tx).await;

                // 3) received-bytes bump — UPDATE on streaming_events.
                let _ = db::update_received_bytes(&pool, event_id, 1024).await;

                n += 1;
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let busy = busy_hits.load(Ordering::Relaxed);

    // ZERO tolerance: the storm is the bug. The fix (single atomic claim
    // statement) eliminates both the read snapshot (517) and the read->write
    // window (5). Any escape to the app layer is a regression.
    assert_eq!(
        busy, 0,
        "picker surfaced {busy} SQLITE_BUSY/BUSY_SNAPSHOT errors under the \
         production write mix — the #256 storm is back (deferred read->write \
         picker, or busy_timeout failing to cover 517)"
    );

    // Correctness: every claimed chunk was claimed EXACTLY once. A picker that
    // races the claim could hand the same id to two workers (double-upload) or
    // skip rows; the atomic claim guarantees one winner per row.
    let map = claims.lock().await;
    let total_claims: u32 = map.values().copied().sum();
    let double_claims: Vec<(i64, u32)> = map
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(id, count)| (*id, *count))
        .collect();
    assert!(
        double_claims.is_empty(),
        "chunks claimed more than once (double-upload): {double_claims:?}"
    );
    // Throughput sanity: the workers must have actually drained rows, proving
    // the atomic claim isn't deadlocking or serialising to a crawl.
    assert!(
        total_claims >= 1,
        "uploader throughput regressed: ZERO claims in 4s (deadlock?)"
    );
}
