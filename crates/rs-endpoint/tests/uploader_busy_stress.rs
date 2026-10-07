//! SQLITE_BUSY-storm regression test for the upload-queue picker
//! (`db::pick_next_uploadable_chunk`) under a file-backed WAL pool with the
//! production multi-writer mix.
//!
//! Context — 2026-06-19 live-event outage (issue #256): the picker was a
//! deferred-`BEGIN` read-then-write-upgrade transaction. 2-8 uploader workers
//! sharing one 5-connection pool all raced the `ORDER BY id ASC LIMIT 1` hot
//! row against frequent committers (chunk INSERT ~every 2s, audit batch,
//! `update_received_bytes`). SQLite then returned, repeatedly, code 5
//! (SQLITE_BUSY — writer-writer lock contention) and code 517
//! (SQLITE_BUSY_SNAPSHOT — deferred read->write upgrade conflict).
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
//!
//! The run is COUNT-based, not time-boxed (#382): the workers drain until every
//! seeded chunk is claimed, however long a loaded runner takes. A liveness cap
//! of max(60 s, 10x the measured seed time) bounds it; it trips on a hang, a
//! deadlock, or a drain more than 10x slower than serial seeding on the same
//! runner, never on plain runner slowness. The old shape (claim for 4 s, then require >= 100 claims) failed on a
//! starved Windows runner with no logic fault (PR run 37558129876: 19 claims).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

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

/// Floor of the liveness cap on the whole claim phase. It is NOT a throughput
/// target: a correct picker drains 800 chunks in about 0.5 s on a normal host.
const LIVENESS_CAP_FLOOR: Duration = Duration::from_secs(60);

/// How many seed-phase durations the claim phase may take before the cap
/// trips. Seeding is 800 serial INSERTs; draining is at least 2 serialised
/// writes per seeded chunk (the claim UPDATE + `record_upload_success`) plus
/// the committer writes, so a correct picker needs about 2-3x the seed time.
const LIVENESS_CAP_SEED_FACTOR: u32 = 10;

/// The liveness cap, scaled to THIS runner's measured write speed. A starved
/// CI runner (#382: a Windows job whose seeding alone took ~80 s) gets a
/// proportionally longer cap, so only a real hang (a pick that never returns,
/// a pool deadlock) or a drain more than 10x slower than serial seeding on the
/// same runner trips it, never plain runner slowness.
fn liveness_cap(seed_elapsed: Duration) -> Duration {
    LIVENESS_CAP_FLOOR.max(seed_elapsed * LIVENESS_CAP_SEED_FACTOR)
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
    let seed_started = Instant::now();
    let mut seeded_ids = HashSet::with_capacity(SEED);
    for i in 0..SEED {
        let id = db::insert_chunk(
            &pool,
            event_id,
            &format!("/tmp/chunk{i}.bin"),
            1024,
            "deadbeef",
            1000,
        )
        .await
        .unwrap();
        seeded_ids.insert(id);
    }
    let seed_elapsed = seed_started.elapsed();
    let cap = liveness_cap(seed_elapsed);
    assert_eq!(seeded_ids.len(), SEED, "seeding produced duplicate ids");
    let seeded_ids = Arc::new(seeded_ids);

    // A broadcast channel for audit::insert_batch (the post-commit fan-out).
    let (ws_tx, _ws_rx) = broadcast::channel(1024);

    let busy_hits = Arc::new(AtomicU32::new(0));
    // Non-BUSY errors from the picker or the sent-mark. Not part of the #256
    // signature, but counted so a persistent one shows up in the output
    // instead of looping silently.
    let other_errors = Arc::new(AtomicU32::new(0));
    // Committer rounds whose three writes ALL succeeded: proves the write mix
    // really ran against the claimers, so the zero-BUSY result is not an
    // uncontended run.
    let committer_ok_rounds = Arc::new(AtomicU32::new(0));
    // Records every chunk id each claimer won (seeded AND committer-inserted),
    // to detect double-claims.
    let claims: Arc<Mutex<HashMap<i64, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    // Claims of SEEDED ids only. This is the workers' stop condition. It must
    // not count committer-inserted chunks: when several workers pass the
    // `< SEED` check together, the extras claim live chunks, and counting
    // those would end the run with seeded rows still unclaimed.
    let seeded_claimed = Arc::new(AtomicUsize::new(0));
    // Set once the workers are done, so the committers keep the write lock
    // contended for the whole claim phase and no longer.
    let workers_done = Arc::new(AtomicBool::new(false));

    // Elapsed covers the whole claim phase, from the first spawn.
    let started = Instant::now();

    // --- 8 claimer workers (production = adaptive 2..8) ---
    // Each tightly loops the picker. On a successful claim it marks the chunk
    // sent (mirrors record_upload_success after the slow S3 PUT) so the queue
    // keeps draining, then loops immediately to maximise contention.
    let mut workers = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let busy_hits = Arc::clone(&busy_hits);
        let other_errors = Arc::clone(&other_errors);
        let claims = Arc::clone(&claims);
        let seeded_ids = Arc::clone(&seeded_ids);
        let seeded_claimed = Arc::clone(&seeded_claimed);
        workers.push(tokio::spawn(async move {
            while seeded_claimed.load(Ordering::SeqCst) < SEED {
                let now_ms = chrono::Utc::now().timestamp_millis();
                match db::pick_next_uploadable_chunk(&pool, now_ms).await {
                    Ok(Some(chunk)) => {
                        // Count this claim. A correct picker claims each id
                        // exactly once across all workers.
                        {
                            let mut map = claims.lock().await;
                            *map.entry(chunk.id).or_insert(0) += 1;
                        }
                        if seeded_ids.contains(&chunk.id) {
                            seeded_claimed.fetch_add(1, Ordering::SeqCst);
                        }
                        // Mark sent so the row leaves the eligible set. This is
                        // a real production write (execute(pool) UPDATE), so a
                        // BUSY here is ALSO part of the storm the fix must
                        // eliminate — count it into busy_hits, don't swallow it.
                        if let Err(e) = db::record_upload_success(
                            &pool,
                            chunk.id,
                            chrono::Utc::now().timestamp_millis(),
                            1,
                        )
                        .await
                        {
                            if is_sqlite_busy(&e) {
                                busy_hits.fetch_add(1, Ordering::Relaxed);
                            } else {
                                other_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Ok(None) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    Err(e) => {
                        if is_sqlite_busy(&e) {
                            busy_hits.fetch_add(1, Ordering::Relaxed);
                        } else {
                            other_errors.fetch_add(1, Ordering::Relaxed);
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
    let mut committers = Vec::new();
    for w in 0..2 {
        let pool = pool.clone();
        let ws_tx = ws_tx.clone();
        let workers_done = Arc::clone(&workers_done);
        let committer_ok_rounds = Arc::clone(&committer_ok_rounds);
        committers.push(tokio::spawn(async move {
            let mut n = 0i64;
            while !workers_done.load(Ordering::SeqCst) {
                // 1) chunk INSERT — same statement the inpoint chunker runs.
                let inserted = db::insert_chunk(
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
                let audited = db::audit::insert_batch(&pool, &rows, &ws_tx).await;

                // 3) received-bytes bump — UPDATE on streaming_events.
                let bumped = db::update_received_bytes(&pool, event_id, 1024).await;

                n += 1;
                if inserted.is_ok() && audited.is_ok() && bumped.is_ok() {
                    committer_ok_rounds.fetch_add(1, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }));
    }

    // The liveness cap wraps the WHOLE claim phase in one timeout, so it also
    // trips when a single pick (or commit) never returns, not only when the
    // worker loop is slow. On a trip, abort every task so the test fails fast
    // instead of leaving them running against the pool.
    let abort_handles: Vec<_> = workers
        .iter()
        .chain(committers.iter())
        .map(|h| h.abort_handle())
        .collect();
    let phase = tokio::time::timeout(cap, async {
        for h in workers {
            h.await.expect("claimer worker panicked");
        }
        workers_done.store(true, Ordering::SeqCst);
        for h in committers {
            h.await.expect("committer task panicked");
        }
    })
    .await;
    let elapsed = started.elapsed();
    let cap_hit = phase.is_err();
    if cap_hit {
        for h in &abort_handles {
            h.abort();
        }
    }

    let busy = busy_hits.load(Ordering::Relaxed);
    let other = other_errors.load(Ordering::Relaxed);
    let rounds = committer_ok_rounds.load(Ordering::Relaxed);
    let seeded_done = seeded_claimed.load(Ordering::SeqCst);
    let map = claims.lock().await;
    let total_claims: u32 = map.values().copied().sum();
    let mut missing: Vec<i64> = seeded_ids
        .iter()
        .filter(|id| !map.contains_key(*id))
        .copied()
        .collect();
    missing.sort_unstable();
    println!(
        "uploader_busy_stress: {seeded_done}/{SEED} seeded chunks claimed, \
         {total_claims} claims in total (incl. committer-inserted), \
         {busy} BUSY, {other} other claim/mark errors, {rounds} committer rounds ok, \
         elapsed {elapsed:?} (seed {seed_elapsed:?}, cap {cap:?})"
    );

    // Liveness: a distinct message, so a hang is never mistaken for the BUSY
    // storm or for a lost row. It carries those counts too, because a stranded
    // seeded row or a BUSY storm can also keep the workers from finishing.
    assert!(
        !cap_hit,
        "liveness cap hit: only {seeded_done}/{SEED} seeded chunks claimed after \
         {cap:?} (seeding took {seed_elapsed:?}) — the picker or a committer hung \
         or deadlocked, or the drain ran more than 10x slower than serial \
         seeding. {busy} BUSY, {other} other claim/mark errors, {} seeded ids \
         never claimed (first: {:?})",
        missing.len(),
        &missing[..missing.len().min(10)]
    );

    // ZERO tolerance: the storm is the bug. The fix (single atomic claim
    // statement) eliminates both the read snapshot (517) and the read->write
    // window (5). Any escape to the app layer is a regression.
    assert_eq!(
        busy, 0,
        "picker surfaced {busy} SQLITE_BUSY/BUSY_SNAPSHOT errors under the \
         production write mix — the #256 storm is back (deferred read->write \
         picker, or busy_timeout failing to cover 517)"
    );

    // The write mix must really have run while the workers claimed; otherwise
    // the zero-BUSY result above proves nothing about contention.
    assert!(
        rounds > 0,
        "the committer write mix completed no successful round during the \
         claim phase"
    );

    // Correctness: every claimed chunk was claimed EXACTLY once. A picker that
    // races the claim could hand the same id to two workers (double-upload) or
    // skip rows; the atomic claim guarantees one winner per row.
    let double_claims: Vec<(i64, u32)> = map
        .iter()
        .filter(|(_, count)| **count > 1)
        .map(|(id, count)| (*id, *count))
        .collect();
    assert!(
        double_claims.is_empty(),
        "chunks claimed more than once (double-upload): {double_claims:?}"
    );

    // Completeness: the run is count-based, so on any runner speed ALL seeded
    // chunks were drained by 8 workers racing one another under the committer
    // mix. That is what makes the zero-double-claim result above a real claim
    // race and not a vacuous, under-contended run.
    assert!(
        missing.is_empty(),
        "{} of {SEED} seeded chunks were never claimed (lost rows): {missing:?}",
        missing.len()
    );
    assert_eq!(
        seeded_done, SEED,
        "seeded-claim counter is {seeded_done}, expected exactly {SEED}"
    );
}
