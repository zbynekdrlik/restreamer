//! #330/#331/#332/#333/#335 disk-cache stall/recovered bracket tests.
//!
//! Split out of `disk_cache_stall_tests.rs` to stay under the 1000-line file
//! cap (rust-crate-hygiene). A child module of `disk_cache_stall_tests`, so it
//! shares that module's mock S3 backends + `real_fetcher` harness via `super`.

use std::sync::Arc;
use std::time::Duration;

use rs_core::audit::Action;

use crate::audit_ring::AuditRing;
use crate::disk_cache::{ChunkAvailability, DiskCache, DiskCacheConfig};
use crate::disk_cache_fetcher::DiskCacheFetcher;
use crate::endpoint_task::ChunkFetcher;

use super::{
    ErroringBackend, STALL_TIMEOUT_SECS, ServeOnceThenErrorBackend, StallSetBackend,
    StallThenMissBackend, real_fetcher,
};

// ---------------------------------------------------------------------------
// #333 — was_stalled must clear on the NotFound / Evicted terminal arms too,
// not only Available, so the DiskCacheReaderRecovered bracket closes on the
// transition it actually recovered on.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn notfound_recovery_after_failed_arm_closes_bracket() {
    // An error storm on chunk 1 arms `was_stalled` via the bounded-attempts
    // Failed arm (one DiskCacheStallTimeout row). When S3 recovers and the
    // producer's next fetch lands on a clean 404 (the top-level NotFound arm),
    // the outage bracket MUST close with exactly one DiskCacheReaderRecovered.
    // Pre-fix the NotFound arm returned Ok(None) without clearing was_stalled,
    // so NO recovered row fired on that transition (it would only fire on a
    // much-later Available, mis-bracketing the window).
    let tmp = tempfile::tempdir().expect("tempdir");
    let ring = AuditRing::new(500);
    let backend = Arc::new(StallThenMissBackend { stall_chunk: 1 });
    let fetcher = real_fetcher(backend, &tmp, "notfound-recover", Some(ring.clone())).await;

    // 1) Stall on chunk 1 (bounded-attempts Failed arm -> Err, was_stalled armed).
    let budget = Duration::from_secs(STALL_TIMEOUT_SECS * 4);
    let r1 = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(1)).await;
    assert!(
        matches!(r1, Ok(Err(_))),
        "chunk 1 must stall (Err) via the Failed arm, got {r1:?}"
    );
    let (rows, _) = ring.since(0);
    assert!(
        rows.iter()
            .any(|r| r.action == Action::DiskCacheStallTimeout),
        "setup: the Failed arm must record a DiskCacheStallTimeout"
    );
    assert!(
        !rows
            .iter()
            .any(|r| r.action == Action::DiskCacheReaderRecovered),
        "setup: no recovered row before the recovery fetch"
    );

    // 2) Recovery fetch on chunk 2: S3 clean-404s -> top-level NotFound arm -> Ok(None).
    let r2 = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(2)).await;
    assert!(
        matches!(r2, Ok(Ok(None))),
        "chunk 2 clean-404 must be a cache miss Ok(None), got {r2:?}"
    );
    let (rows, _) = ring.since(0);
    let recovered = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheReaderRecovered)
        .count();
    assert_eq!(
        recovered, 1,
        "#333 REGRESSION: a NotFound recovery after a stall must close the \
         bracket with exactly one DiskCacheReaderRecovered; found {recovered}"
    );
}

// ---------------------------------------------------------------------------
// #331 — every DiskCacheReaderRecovered must pair with an emitted
// DiskCacheStallTimeout. Arming was_stalled unconditionally while the stall
// row is rate-limited produced unpaired recovered edges.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn recovered_rows_stay_paired_when_stall_row_is_rate_limited() {
    // Two bounded-attempts error-storm blips in rapid succession (well inside
    // the RateLimiter's real-clock 60s window, so the SECOND stall row is
    // suppressed), each followed by an Available recovery. The stall/recovered
    // bracket must stay paired: exactly as many DiskCacheReaderRecovered rows
    // as DiskCacheStallTimeout rows. Pre-fix, was_stalled armed on BOTH blips
    // (unconditionally) while only the first stall row emitted, so the second
    // recovery emitted an unpaired DiskCacheReaderRecovered.
    let tmp = tempfile::tempdir().expect("tempdir");
    let ring = AuditRing::new(500);
    // stall on 1 and 100; serve everything else. 100 is far outside the
    // window-4 prefetch of the low-chunk fetches, so background prefetch never
    // pre-marks it and the second stall is a clean, independent blip.
    let backend = Arc::new(StallSetBackend {
        stall_chunks: vec![1, 100],
    });
    let fetcher = real_fetcher(backend, &tmp, "pairing", Some(ring.clone())).await;
    let budget = Duration::from_secs(STALL_TIMEOUT_SECS * 4);

    // Blip 1: stall on chunk 1 (Failed arm) -> stall row #1 + was_stalled armed.
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(1)).await;
    assert!(matches!(r, Ok(Err(_))), "blip 1 must stall, got {r:?}");
    // Recovery 1: chunk 2 served -> Available -> recovered row #1.
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(2)).await;
    assert!(
        matches!(r, Ok(Ok(Some(_)))),
        "recovery 1 must serve a chunk, got {r:?}"
    );

    // Blip 2: stall on chunk 100 (Failed arm) -> stall row SUPPRESSED by the
    // rate limiter (same key, within 60s real-clock).
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(100)).await;
    assert!(matches!(r, Ok(Err(_))), "blip 2 must stall, got {r:?}");
    // Recovery 2: chunk 101 served -> Available.
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(101)).await;
    assert!(
        matches!(r, Ok(Ok(Some(_)))),
        "recovery 2 must serve a chunk, got {r:?}"
    );

    let (rows, _) = ring.since(0);
    let stalls = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheStallTimeout)
        .count();
    let recovered = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheReaderRecovered)
        .count();
    assert_eq!(
        stalls, 1,
        "setup: the rate limiter must suppress the second same-shape stall row \
         (found {stalls}) so the pairing invariant is actually exercised"
    );
    assert_eq!(
        recovered, stalls,
        "#331 REGRESSION: every DiskCacheReaderRecovered must pair with an \
         emitted DiskCacheStallTimeout; got {recovered} recovered vs {stalls} \
         stall rows (unpaired recovered edge from arming was_stalled while the \
         stall row was rate-limited)"
    );
}

// ---------------------------------------------------------------------------
// #330 — the evicted-chunk refetch stall arms must record DiskCacheStallTimeout
// like the main path, so a resume-after-eviction outage is not silent.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn evicted_refetch_failure_records_stall_for_audit_bracket() {
    // Reproduce the #252 resume-after-eviction race, then break S3 so the
    // refetch resolves Failed: the registry slot reads stale `Available`, the
    // local file is gone, and the re-request lands in an error storm. The
    // refetch's Failed arm MUST route through note_stall and record a
    // DiskCacheStallTimeout. Pre-fix it returned a raw Err string, leaving the
    // resume-after-eviction outage silent on the audit timeline.
    let tmp = tempfile::tempdir().expect("tempdir");
    let backend = Arc::new(ServeOnceThenErrorBackend::default());
    let ring = AuditRing::new(500);
    let evicted_id: i64 = 877;
    let cfg = DiskCacheConfig {
        cache_dir: tmp.path().to_path_buf(),
        window_chunks: 4,
        s3_ingress_cap_mbit: 10_000,
        eviction_interval_secs: 3600,
        read_stall_timeout_secs: STALL_TIMEOUT_SECS,
        download_queue_capacity: 50,
    };
    let cache = Arc::new(
        DiskCache::new(
            cfg,
            backend.clone(),
            "evict-refetch-evt".to_string(),
            Some(ring.clone()),
        )
        .await
        .expect("DiskCache::new"),
    );

    // Prime: the FIRST fetch serves -> registry Available + 877.bin on disk.
    cache.download_service.request_chunk(evicted_id).await;
    let path = cache.event_dir().join(format!("{evicted_id}.bin"));
    assert!(
        path.exists(),
        "setup: chunk file must exist after priming GET"
    );
    assert!(
        matches!(
            cache.registry.peek(evicted_id),
            Some(ChunkAvailability::Available { .. })
        ),
        "setup: registry must read Available before eviction"
    );
    // Simulate eviction: delete ONLY the local file; the slot stays Available.
    tokio::fs::remove_file(&path)
        .await
        .expect("setup: simulate eviction by deleting the local file");

    let fetcher = DiskCacheFetcher::new(
        Arc::clone(&cache),
        "evict-refetch".to_string(),
        evicted_id,
        4,
        STALL_TIMEOUT_SECS,
        Some(ring.clone()),
    );

    // The refetch re-requests into the now-erroring S3 -> Failed -> Err.
    let got = tokio::time::timeout(
        Duration::from_secs(STALL_TIMEOUT_SECS * 4),
        fetcher.fetch_chunk_with_meta(evicted_id),
    )
    .await
    .expect("fetch must not hang");
    assert!(
        got.is_err(),
        "the evicted-chunk refetch into an S3 storm must surface as Err, got {got:?}"
    );

    let (rows, _) = ring.since(0);
    assert!(
        rows.iter()
            .any(|r| r.action == Action::DiskCacheStallTimeout),
        "#330 REGRESSION: the evicted-chunk refetch stall arms must route \
         through note_stall so a resume-after-eviction outage records a \
         DiskCacheStallTimeout (found none)"
    );
}

// ---------------------------------------------------------------------------
// #332 — the bounded-attempts (~3s) Failed arm must be distinguishable from a
// stall_timeout-length wedge: a "shape" discriminator in the detail, no
// timeout_secs on the bounded branch, and a shape-aware last_error string.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn bounded_attempts_stall_row_is_shaped_and_not_timeout_labelled() {
    // The Failed arm resolves after ~3s of bounded attempts and never consults
    // stall_timeout, so its DiskCacheStallTimeout row must carry
    // shape="bounded_attempts" and NO timeout_secs, and the returned Err string
    // must read "bounded attempts", not "stall". Pre-fix every row hardcoded
    // timeout_secs=stall_timeout and had no shape, so a 3s transient read as a
    // 60s cache-window-exceeding outage.
    let tmp = tempfile::tempdir().expect("tempdir");
    let ring = AuditRing::new(500);
    let backend = Arc::new(ErroringBackend::default());
    let fetcher = real_fetcher(backend, &tmp, "shaped", Some(ring.clone())).await;

    let budget = Duration::from_secs(STALL_TIMEOUT_SECS * 4);
    let got = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(1)).await;
    let err = match got {
        Ok(Err(e)) => e,
        other => panic!("bounded-attempts Failed arm must surface Err, got {other:?}"),
    };
    assert!(
        err.contains("bounded attempts"),
        "#332 REGRESSION: the bounded-attempts last_error must keep its shape \
         hint (\"bounded attempts\"), got {err:?}"
    );

    let (rows, _) = ring.since(0);
    let stall = rows
        .iter()
        .find(|r| r.action == Action::DiskCacheStallTimeout)
        .expect("a DiskCacheStallTimeout row must have been emitted");
    assert_eq!(
        stall.detail.get("shape").and_then(|v| v.as_str()),
        Some("bounded_attempts"),
        "#332 REGRESSION: the bounded-attempts stall row must carry \
         shape=\"bounded_attempts\"; detail was {:?}",
        stall.detail
    );
    assert!(
        stall.detail.get("timeout_secs").is_none(),
        "#332 REGRESSION: the bounded-attempts path never consults \
         stall_timeout, so its detail must NOT report timeout_secs; detail was {:?}",
        stall.detail
    );
}

// ---------------------------------------------------------------------------
// #335 — the canonical Available-recovery bracket: a stall followed by a served
// chunk must emit exactly one DiskCacheReaderRecovered carrying the recovering
// chunk_id. This is what a mutation dropping `was_stalled.store(true)` from
// note_stall would silently disable, and nothing asserted it before.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn available_recovery_after_stall_emits_reader_recovered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let ring = AuditRing::new(500);
    // stall on chunk 1, serve everything else (chunk 2 recovers via Available).
    let backend = Arc::new(StallSetBackend {
        stall_chunks: vec![1],
    });
    let fetcher = real_fetcher(backend, &tmp, "avail-recover", Some(ring.clone())).await;
    let budget = Duration::from_secs(STALL_TIMEOUT_SECS * 4);

    // Stall on chunk 1 (bounded-attempts Failed arm) -> was_stalled armed.
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(1)).await;
    assert!(matches!(r, Ok(Err(_))), "chunk 1 must stall, got {r:?}");

    // Recovery on chunk 2: served -> Available arm -> the paired recovered row.
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(2)).await;
    assert!(
        matches!(r, Ok(Ok(Some(_)))),
        "chunk 2 must serve (recover), got {r:?}"
    );

    let (rows, _) = ring.since(0);
    let recovered: Vec<_> = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheReaderRecovered)
        .collect();
    assert_eq!(
        recovered.len(),
        1,
        "#335: exactly one DiskCacheReaderRecovered must close the bracket; \
         found {}",
        recovered.len()
    );
    assert_eq!(
        recovered[0].detail.get("chunk_id").and_then(|v| v.as_i64()),
        Some(2),
        "the recovered row must carry the recovering chunk_id (2); detail was {:?}",
        recovered[0].detail
    );
}

// ---------------------------------------------------------------------------
// #333 review finding — a STALE registry `Available` (slot reads Available but
// the local file was evicted, so request_chunk dedup-skips and no fresh S3 GET
// is issued) must NOT close an open outage bracket. Only a genuine clean
// terminal (successful read / a refetch that actually hit S3) does.
// ---------------------------------------------------------------------------

#[tokio::test(start_paused = true)]
async fn stale_available_during_outage_does_not_emit_spurious_recovered() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let ring = AuditRing::new(500);
    // Serves the FIRST fetch (priming 877) then errors: the refetch into the
    // storm resolves Failed while S3 is still down.
    let backend = Arc::new(ServeOnceThenErrorBackend::default());
    let evicted_id: i64 = 877;
    let cfg = DiskCacheConfig {
        cache_dir: tmp.path().to_path_buf(),
        window_chunks: 4,
        s3_ingress_cap_mbit: 10_000,
        eviction_interval_secs: 3600,
        read_stall_timeout_secs: STALL_TIMEOUT_SECS,
        download_queue_capacity: 50,
    };
    let cache = Arc::new(
        DiskCache::new(
            cfg,
            backend.clone(),
            "stale-avail-evt".to_string(),
            Some(ring.clone()),
        )
        .await
        .expect("DiskCache::new"),
    );

    // Prime 877 (first fetch serves) -> registry Available + 877.bin, then delete
    // ONLY the file: the slot keeps reading stale `Available`.
    cache.download_service.request_chunk(evicted_id).await;
    let path = cache.event_dir().join(format!("{evicted_id}.bin"));
    assert!(path.exists(), "setup: priming GET must land 877.bin");
    tokio::fs::remove_file(&path)
        .await
        .expect("setup: simulate eviction by deleting the local file");

    let fetcher = DiskCacheFetcher::new(
        Arc::clone(&cache),
        "stale-avail".to_string(),
        evicted_id,
        4,
        STALL_TIMEOUT_SECS,
        Some(ring.clone()),
    );
    let budget = Duration::from_secs(STALL_TIMEOUT_SECS * 4);

    // 1) Arm the bracket with a REAL stall on chunk 1 (bounded-attempts Failed).
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(1)).await;
    assert!(
        matches!(r, Ok(Err(_))),
        "chunk 1 must stall to arm the bracket, got {r:?}"
    );
    let (rows, _) = ring.since(0);
    assert!(
        rows.iter()
            .any(|r| r.action == Action::DiskCacheStallTimeout),
        "setup: chunk 1 must arm the bracket (one stall row)"
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.action == Action::DiskCacheReaderRecovered)
            .count(),
        0,
        "setup: no recovery yet"
    );

    // 2) The stale-Available 877 fetch: refetch re-requests into the storm ->
    //    Failed -> Err. S3 is STILL down, so the bracket must NOT close.
    let r = tokio::time::timeout(budget, fetcher.fetch_chunk_with_meta(evicted_id)).await;
    assert!(
        matches!(r, Ok(Err(_))),
        "the stale-Available refetch into the storm must Err, got {r:?}"
    );

    let (rows, _) = ring.since(0);
    let recovered = rows
        .iter()
        .filter(|r| r.action == Action::DiskCacheReaderRecovered)
        .count();
    assert_eq!(
        recovered, 0,
        "#333 review REGRESSION: a STALE registry Available (file evicted, S3 \
         still down, no fresh GET) must NOT emit a DiskCacheReaderRecovered -- \
         the bracket stays open until a genuine recovery; found {recovered}"
    );
}
