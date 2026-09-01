//! `DiskCacheFetcher` — `ChunkFetcher` backed by the per-event `DiskCache`.
//!
//! Replaces the direct `S3Fetcher` used by the producer-consumer pipeline.
//! `fetch_chunk_with_meta` triggers a background fetch into the disk cache
//! (deduplicated, bandwidth-managed) and waits for the chunk to land on
//! local SSD before returning the bytes. The bandwidth-managed downloader
//! also pre-fetches `[id+1, id+window-1]` so the producer keeps reading
//! from disk at line speed even when S3 has transient failures.
//!
//! Issue #174: this is the integration point that decouples upstream S3
//! ingress from the downstream RTMP push hot path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::disk_cache::{ChunkAvailability, DiskCache};
use crate::endpoint_task::ChunkFetcher;

/// Discriminates the two shapes of a disk-cache stall. Keeps the rate-limiter
/// window separate per class (#331) so a bounded-attempts error storm and a
/// stall_timeout-length wedge in the same 60s window each keep their audit row
/// and the class transition stays visible on the timeline.
#[derive(Clone, Copy)]
enum StallShape {
    /// `MAX_FETCH_ATTEMPTS` exhausted in ~3s (a persistently-erroring S3).
    BoundedAttempts,
    /// The outer `stall_timeout` deadline elapsed (a wedge >= the cache window).
    StallTimeout,
}

impl StallShape {
    fn as_str(self) -> &'static str {
        match self {
            StallShape::BoundedAttempts => "bounded_attempts",
            StallShape::StallTimeout => "stall_timeout",
        }
    }
}

pub struct DiskCacheFetcher {
    cache: Arc<DiskCache>,
    alias: String,
    /// `{cache_dir}/{event_id}/`.
    event_dir: PathBuf,
    /// Endpoint window length in chunks. Used for prefetch-ahead and the
    /// position-registry registration.
    window_chunks: i64,
    /// Stall deadline: how long the producer waits for a single chunk
    /// (the `tokio::time::timeout` wrapping `request_chunk` + `wait_for_chunk`)
    /// before returning Err. The producer's existing backoff loop turns the
    /// Err into a retry.
    stall_timeout_secs: u64,
    /// VPS audit ring for outage-forensics events (stall-timeout,
    /// reader-recovered, prefill-started). `None` outside production.
    audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    /// True after a stall-timeout, until the next successful `Available`
    /// fetch — that transition emits `DiskCacheReaderRecovered` so the
    /// audit timeline brackets each outage window. `&self` fetch path, so
    /// an atomic (not Cell).
    was_stalled: std::sync::atomic::AtomicBool,
    /// Rate-limits the `DiskCacheStallTimeout` emit (a sustained outage
    /// would otherwise emit one row per stall_timeout window).
    stall_rl: rs_core::audit::RateLimiter,
}

impl DiskCacheFetcher {
    pub fn new(
        cache: Arc<DiskCache>,
        alias: String,
        start_chunk_id: i64,
        window_chunks: i64,
        stall_timeout_secs: u64,
        audit_ring: Option<Arc<crate::audit_ring::AuditRing>>,
    ) -> Self {
        let event_dir = cache.event_dir();
        // Register synchronously: a same-tick `advance` from the producer
        // would otherwise silently no-op on an unknown alias and the
        // EvictionTask could delete chunks this endpoint still needs
        // (#174 review finding 1). Single alias clone for register;
        // advance takes a borrow.
        let alias_for_register = alias.clone();
        let positions = &cache.position_registry;
        positions.register(alias_for_register, window_chunks);
        positions.advance(&alias, start_chunk_id);
        // Outage forensics: one fetcher per endpoint => construction is the
        // endpoint's first cache registration. Bracket the prefill with
        // PrefillStarted here and PrefillReady at warmup-complete (rescue.rs).
        if let Some(ring) = &audit_ring {
            ring.push_parts(crate::audit_ring::RingRowParts {
                severity: rs_core::audit::Severity::Info,
                source: rs_core::audit::Source::Vps,
                endpoint: Some(alias.clone()),
                action: rs_core::audit::Action::DiskCachePrefillStarted,
                detail: serde_json::json!({ "start_chunk_id": start_chunk_id }),
            });
        }
        Self {
            cache,
            alias,
            event_dir,
            window_chunks,
            stall_timeout_secs,
            audit_ring,
            was_stalled: std::sync::atomic::AtomicBool::new(false),
            stall_rl: rs_core::audit::RateLimiter::new(),
        }
    }
}

impl DiskCacheFetcher {
    /// Shared stall bookkeeping (#284): record the stall for the paired
    /// `DiskCacheReaderRecovered` bracket, emit the rate-limited
    /// `DiskCacheStallTimeout` audit row, and build the Err string the
    /// producer's backoff loop expects.
    fn note_stall(&self, chunk_id: i64, shape: StallShape, detail: &str) -> String {
        if let Some(ring) = &self.audit_ring {
            // #331: key the limiter on (action, alias, shape) so a
            // bounded-attempts storm and a stall_timeout wedge in the same 60s
            // window each keep their row. Arm `was_stalled` ONLY when the stall
            // row is actually emitted -- a rate-limited (suppressed) stall must
            // NOT arm the flag, or the next recovery would emit a
            // DiskCacheReaderRecovered with no matching stall row.
            if self.stall_rl.allow(
                rs_core::audit::Action::DiskCacheStallTimeout,
                &format!("{}:{}", self.alias, shape.as_str()),
            ) {
                self.was_stalled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
                // #332: stamp the shape so a ~3s bounded-attempts storm is not
                // read as a stall_timeout-length outage, and set `timeout_secs`
                // ONLY on the stall_timeout branch -- the bounded-attempts path
                // reaches here after ~3s and never consults the timeout.
                let mut row_detail = serde_json::json!({
                    "chunk_id": chunk_id,
                    "shape": shape.as_str(),
                    "detail": detail,
                });
                if matches!(shape, StallShape::StallTimeout) {
                    row_detail["timeout_secs"] = serde_json::json!(self.stall_timeout_secs);
                }
                ring.push_parts(crate::audit_ring::RingRowParts {
                    severity: rs_core::audit::Severity::Error,
                    source: rs_core::audit::Source::Vps,
                    endpoint: Some(self.alias.clone()),
                    action: rs_core::audit::Action::DiskCacheStallTimeout,
                    detail: row_detail,
                });
            }
        }
        // #332: keep the shape in the operator-facing last_error string
        // (stored on stats.last_error and shown on the dashboard) so a 3-attempt
        // cap is not misread as a 60s stall.
        match shape {
            StallShape::BoundedAttempts => {
                format!("disk_cache bounded attempts exhausted on chunk {chunk_id}: {detail}")
            }
            StallShape::StallTimeout => format!("disk_cache stall on chunk {chunk_id}: {detail}"),
        }
    }

    /// Close the outage bracket (#333): if a stall was recorded, clear the
    /// flag and emit the paired `DiskCacheReaderRecovered`. Called on EVERY
    /// clean terminal state -- `Available`, `NotFound`, AND `Evicted` --
    /// because all three mean S3 answered and the reader is no longer
    /// stalled. `swap` is the atomic test-and-clear so exactly one recovered
    /// row closes each bracket regardless of which clean state arrives first.
    /// Emitting on `NotFound`/`Evicted` too avoids a mis-bracketed window: a
    /// stall armed by the ~3s `Failed` arm, followed by the producer's
    /// skip-ahead probe recovering on a clean 404 hundreds of chunks later,
    /// would otherwise stay open until an unrelated `Available`.
    fn note_recovered(&self, chunk_id: i64) {
        if self
            .was_stalled
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            if let Some(ring) = &self.audit_ring {
                ring.push_parts(crate::audit_ring::RingRowParts {
                    severity: rs_core::audit::Severity::Info,
                    source: rs_core::audit::Source::Vps,
                    endpoint: Some(self.alias.clone()),
                    action: rs_core::audit::Action::DiskCacheReaderRecovered,
                    detail: serde_json::json!({ "chunk_id": chunk_id }),
                });
            }
        }
    }
}

impl ChunkFetcher for DiskCacheFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        // Prefetch the upcoming window in ONE spawned task that loops
        // (instead of N spawns per fetch). The DownloadService's own
        // semaphore + dedup keeps the actual S3 concurrency bounded;
        // collapsing the spawn ladder cuts ~window-1 spawn calls per
        // chunk per endpoint (#174 review-of-review #3).
        let prefetch_window = self.window_chunks;
        let prefetch_svc = Arc::clone(&self.cache.download_service);
        tokio::spawn(async move {
            for ahead in 1..=prefetch_window {
                prefetch_svc.request_chunk(chunk_id + ahead).await;
            }
        });

        // Update position registry so eviction protects this endpoint's window.
        self.cache.position_registry.advance(&self.alias, chunk_id);

        // Trigger the targeted fetch and wait for a terminal state — BOTH
        // under ONE stall_timeout deadline (#284). `request_chunk()` itself
        // blocks until the download task reaches a terminal state, and
        // pre-#284 it was awaited UNBOUNDED: a wedged/erroring fetch (the
        // registry slot stuck InFlight while fetch_with_retry retried
        // transient S3 errors forever) parked the producer BEFORE the
        // timeout-guarded registry wait below ever started. producer_active
        // never flipped, the consumer's rescue gate (!producer_active)
        // stayed shut, and every endpoint went dark with no audit row — the
        // #280 operator incident. With the deadline spanning request+wait,
        // EVERY stall shape surfaces as Err to the producer's
        // consecutive-error counter within a bounded budget.
        //
        // The stall arms are audit-only forensics — do NOT abort; the
        // producer's outer backoff retries and rescue covers the gap. The
        // next successful Available fetch emits the paired
        // DiskCacheReaderRecovered to bracket the outage window.
        let state =
            match tokio::time::timeout(Duration::from_secs(self.stall_timeout_secs), async {
                self.cache.download_service.request_chunk(chunk_id).await;
                self.cache.registry.wait_for_chunk(chunk_id).await
            })
            .await
            {
                Ok(s) => s,
                Err(_elapsed) => {
                    return Err(self.note_stall(
                        chunk_id,
                        StallShape::StallTimeout,
                        &format!(
                            "request+wait exceeded stall_timeout {}s",
                            self.stall_timeout_secs
                        ),
                    ));
                }
            };

        match state {
            ChunkAvailability::Available { .. } => {
                // Recovered after a stall: emit the paired ReaderRecovered
                // exactly once per outage so the audit timeline brackets the gap.
                self.note_recovered(chunk_id);
                let path = self.event_dir.join(format!("{chunk_id}.bin"));
                // A registry-`Available` chunk whose LOCAL file is missing is
                // a CACHE MISS, not a disk error (#252). Two ways to get here:
                //  1. An EvictionTask sweep raced this reader between the
                //     registry mark_available and the tokio::fs::read (#174
                //     review finding 3), OR
                //  2. the resume position landed on a chunk whose file was
                //     evicted while its slot still read `Available` (a
                //     re-download -> sweep race) — the crash-exhaustion
                //     recovery path, run 29969272303.
                // In BOTH cases the chunk still lives on S3, so the fix is the
                // same: refetch it. The subtlety that made this loop forever
                // pre-fix: `request_chunk` dedup-skips when
                // `registry.exists()` is true (the stale `Available`), so a
                // bare re-request issued NO S3 GET and the re-read hit the
                // same missing file — the producer then looped on
                // "S3 fetch error, retrying in 60s" (mode=rescue,
                // prod_active=false for the whole 420s recovery window).
                // `mark_in_flight` invalidates the stale slot (the #184 reset
                // pattern) so the re-request issues a genuine S3 GET.
                let data = match tokio::fs::read(&path).await {
                    Ok(d) => d,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        self.cache.registry.mark_in_flight(chunk_id);
                        // Same single-deadline bound as the main path (#284):
                        // an unbounded request_chunk().await here would park
                        // the producer identically.
                        // #330: route the evicted-chunk refetch stall arms
                        // through note_stall (like the main path) so a
                        // resume-after-eviction that lands in an S3 storm also
                        // records DiskCacheStallTimeout and arms the paired
                        // DiskCacheReaderRecovered bracket -- previously these
                        // returned raw strings and left the resume outage silent
                        // on the timeline. was_stalled was cleared at the top of
                        // the Available arm, so re-arming here brackets the
                        // resume outage correctly.
                        let refetched = match tokio::time::timeout(
                            Duration::from_secs(self.stall_timeout_secs),
                            async {
                                self.cache.download_service.request_chunk(chunk_id).await;
                                self.cache.registry.wait_for_chunk(chunk_id).await
                            },
                        )
                        .await
                        {
                            Ok(s) => s,
                            Err(_elapsed) => {
                                return Err(self.note_stall(
                                    chunk_id,
                                    StallShape::StallTimeout,
                                    "evicted-chunk refetch request+wait exceeded stall_timeout",
                                ));
                            }
                        };

                        match refetched {
                            // Refetch landed the file back on disk — read it.
                            // If it is STILL missing (a second eviction race),
                            // degrade to a cache miss so the producer's
                            // skip-ahead probe advances past it — never loop
                            // on the disk path.
                            ChunkAvailability::Available { .. } => {
                                match tokio::fs::read(&path).await {
                                    Ok(d) => d,
                                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                        return Ok(None);
                                    }
                                    Err(e) => {
                                        return Err(format!(
                                            "disk read {} (refetch): {e}",
                                            path.display()
                                        ));
                                    }
                                }
                            }
                            // The chunk is genuinely gone from S3 (or evicted
                            // past every window): a cache miss the producer
                            // resolves by probing ahead — NOT a hard Err that
                            // would re-arm the 60s backoff loop.
                            ChunkAvailability::NotFound | ChunkAvailability::Evicted => {
                                return Ok(None);
                            }
                            // A GENUINE S3 failure — surface as Err so the
                            // producer's consecutive-error rescue counter
                            // advances and it retries on its backoff cadence.
                            // Only real S3 failures keep the retry/backoff.
                            // #330: route through note_stall (bounded-attempts
                            // shape) like the main-path Failed arm so this
                            // outage class also brackets on the timeline.
                            ChunkAvailability::Failed { error } => {
                                return Err(self.note_stall(
                                    chunk_id,
                                    StallShape::BoundedAttempts,
                                    &format!("evicted-chunk refetch failed: {error}"),
                                ));
                            }
                            // #330: unreachable in practice -- wait_for_chunk
                            // only resolves on a TERMINAL state, never while
                            // still InFlight (mirrors the main-path arm below).
                            // Kept only for match exhaustiveness.
                            ChunkAvailability::InFlight => {
                                return Err(format!(
                                    "disk_cache: chunk {chunk_id} stuck InFlight \
                                     after evicted-chunk refetch"
                                ));
                            }
                        }
                    }
                    Err(e) => return Err(format!("disk read {}: {e}", path.display())),
                };
                let duration_ms = self
                    .cache
                    .download_service
                    .get_duration(chunk_id)
                    .await
                    .unwrap_or(0);
                Ok(Some((data, duration_ms)))
            }
            ChunkAvailability::NotFound => {
                // A clean 404 means S3 answered -- the reader is no longer
                // stalled, so close any open outage bracket (#333) before
                // reporting the cache miss.
                self.note_recovered(chunk_id);
                Ok(None)
            }
            ChunkAvailability::Evicted => {
                // The chunk used to exist on disk and was swept. The
                // producer treats `None` as "not on S3", which triggers
                // its skip-ahead probe loop. That's the right recovery
                // because eviction only happens for chunks outside any
                // endpoint's window. A clean Evicted is likewise an S3
                // answer -- close any open outage bracket (#333).
                self.note_recovered(chunk_id);
                Ok(None)
            }
            ChunkAvailability::Failed { error } => {
                // #284: the download task exhausted its bounded attempts
                // (persistently-erroring S3). Surface as Err so the
                // producer's consecutive-error counter advances toward the
                // rescue flip; the producer's backoff loop re-requests, so
                // retrying never stops system-wide (#184). #286: route
                // through note_stall (like the sibling Ok(Err(e)) /
                // Err(_elapsed) branches above) so this outage class also
                // records the DiskCacheStallTimeout audit row and arms the
                // paired DiskCacheReaderRecovered bracket -- MAX_FETCH_ATTEMPTS
                // typically resolves well before the outer stall_timeout, so
                // without this the common error-storm outage left no stall
                // row on the audit timeline even though rescue still fired.
                Err(self.note_stall(chunk_id, StallShape::BoundedAttempts, &error))
            }
            // Unreachable in practice: `wait_for_chunk` only returns once a
            // slot transitions to a TERMINAL state (Available / NotFound /
            // Evicted / Failed) -- it never resolves while still InFlight.
            // Kept only for match exhaustiveness over `ChunkAvailability`.
            ChunkAvailability::InFlight => Err(format!(
                "disk_cache: chunk {chunk_id} stuck InFlight after timeout"
            )),
        }
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        // Producer's skip-ahead probe: HEAD-only, no body download.
        // 5s client-side timeout in case the S3 HEAD wedges the
        // connection (#174 review-of-review #2). Transient errors are
        // surfaced as Err so the producer's outer backoff handles them
        // instead of silently advancing past chunks (#174 review-of-
        // review #4).
        let probe = self.cache.download_service.head_duration(chunk_id);
        match tokio::time::timeout(Duration::from_secs(5), probe).await {
            Ok(Ok(Some(ms))) => Ok(Some(ms)),
            Ok(Ok(None)) => Ok(None),
            Ok(Err(e)) => Err(format!("disk_cache HEAD probe error: {e}")),
            Err(_) => Err(format!("disk_cache HEAD probe timeout on chunk {chunk_id}")),
        }
    }
}
