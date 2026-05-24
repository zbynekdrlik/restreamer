use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use sqlx::SqlitePool;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, warn};

use rs_core::audit::{Action, AuditRow, RateLimiter, Severity, Source};
use rs_core::db;
use rs_core::models::{ChunkRecord, WsEvent};

use crate::metrics::{UploadEvent, UploadMetrics};
use crate::s3::S3Client;

/// Wall-clock millis since UNIX epoch. Used for lifecycle stage A/B
/// timestamps (#184). Saturates to 0 on the impossible pre-1970 case
/// so the cast never panics.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Process-wide rate limiter for uploader audit rows. Emits at most one
/// row per minute per (Action, error_class) key so a sustained outage
/// doesn't swamp `audit_log`. See `rs_core::audit::RateLimiter`.
static UPLOAD_RL: std::sync::LazyLock<RateLimiter> = std::sync::LazyLock::new(RateLimiter::new);

/// Bucket the free-text upload error into a small set of durable classes
/// so the rate-limiter has a stable key (and the audit row has an
/// at-a-glance category).
fn classify_upload_error(msg: &str) -> &'static str {
    let m = msg.to_ascii_lowercase();
    if m.contains("timeout") || m.contains("timed out") {
        "timeout"
    } else if m.contains(" 400") || m.contains("bad request") {
        "400"
    } else if m.contains(" 403") || m.contains("forbidden") {
        "403"
    } else if m.contains(" 404") || m.contains("not found") {
        "404"
    } else if m.contains(" 500")
        || m.contains(" 502")
        || m.contains(" 503")
        || m.contains(" 504")
        || m.contains("5xx")
    {
        "5xx"
    } else if m.contains("connection") || m.contains("reset") || m.contains("refused") {
        "conn"
    } else {
        "other"
    }
}

/// Shared long-lived context passed to every upload worker.
#[derive(Clone)]
struct WorkerCtx {
    pool: SqlitePool,
    s3: Arc<S3Client>,
    ws_tx: broadcast::Sender<WsEvent>,
    metrics: Arc<UploadMetrics>,
    in_flight: Arc<AtomicUsize>,
    blocked: Arc<std::sync::atomic::AtomicBool>,
    /// Number of workers that should voluntarily exit on their next iteration
    /// (used by the adaptive controller to scale down).
    drain_needed: Arc<AtomicUsize>,
    /// Per-installation UUID used to prefix S3 chunk keys, preventing cross-instance
    /// collisions when multiple Restreamer installations share one S3 bucket.
    /// See issue #114.
    client_uuid: String,
    /// Optional audit channel. `None` in tests; set via
    /// `ChunkUploader::with_audit_tx`.
    audit_tx: Option<mpsc::Sender<AuditRow>>,
}

/// Pure gate: should the spawner spawn another worker?
#[inline]
fn should_spawn_worker(live: usize, target: usize) -> bool {
    live < target
}

/// Attempt budget for STRUCTURAL-reject classes (400/403/404) only. Network
/// classes (timeout/5xx/conn/other) are never abandoned — see
/// `should_abandon_upload`. This is the continuity guarantee: a long outage
/// must lose nothing while the laptop runs (2026-05-22 event fix).
const ABANDON_ATTEMPT_BUDGET: i64 = 5;
// Concurrency bounds tuned for Hetzner Object Storage NBG1 per-bucket
// rate limits. 32-worker sustained load triggered 503/504 cascades
// during 2026-05-02 4-h soak. boto3 burst test: 30 parallel PUTs from
// the SAME source IP succeeded 30/30 in 14 s — but a burst is not
// sustained load. Hetzner enforces per-bucket request rate limits on
// NBG1 ("we will be reducing the available write limitations for some
// existing buckets on NBG1" per official status page); 32 sustained
// requests/sec exceeds it. 8 sustained workers stays within budget.
const MIN_CONCURRENCY: usize = 2;
pub(crate) const MAX_CONCURRENCY: usize = 8;
// Compile-time invariant (replaces a runtime tautology test).
const _: () = assert!(MAX_CONCURRENCY > MIN_CONCURRENCY);

/// Decide whether an upload error is terminal for the chunk.
///
/// Network-class errors (`timeout`/`5xx`/`conn`/`other`) are NEVER terminal:
/// the chunk stays on disk and is retried forever at capped backoff so an
/// outage of any duration loses nothing (continuity guarantee). Only
/// structural client rejects (`400`/`403`/`404`) — where retrying can never
/// succeed — abandon, and only after `ABANDON_ATTEMPT_BUDGET` attempts to
/// absorb transient auth/propagation hiccups.
fn should_abandon_upload(class: &str, attempt: i64) -> bool {
    matches!(class, "400" | "403" | "404") && attempt >= ABANDON_ATTEMPT_BUDGET
}

pub(crate) fn backoff_ms(attempt: i64) -> u64 {
    // 1s, 2s, 4s, 8s, 16s, 30s (cap)
    let base = 1000u64;
    let shift = (attempt.saturating_sub(1) as u32).min(5);
    base.saturating_mul(1 << shift).min(30_000)
}

/// Pure-function core of the adaptive concurrency controller.
/// Scales up (×2) when error_rate == 0 AND median_ms < 500.
/// Scales down (÷2) when error_rate > 0.2.
/// Otherwise holds. Bounded to [MIN_CONCURRENCY, MAX_CONCURRENCY].
pub(crate) fn adjust_target(current: usize, error_rate: f64, median_ms: u32) -> usize {
    if error_rate == 0.0 && median_ms < 500 {
        current.saturating_mul(2).min(MAX_CONCURRENCY)
    } else if error_rate > 0.2 {
        (current / 2).max(MIN_CONCURRENCY)
    } else {
        current
    }
}

/// Watches for unsent chunks and uploads them to S3 using a continuous worker pool.
///
/// Each worker independently atomically SELECT+UPDATEs (`in_process=1`) the
/// oldest eligible chunk via `db::pick_next_uploadable_chunk` and uploads it.
/// Retries live in the DB via `record_upload_failure` (writes
/// `upload_next_retry_at`) so workers naturally re-pick them on a later poll.
///
/// The claim-coordinator pattern (see commit ff86526) was reverted because it
/// regressed upload throughput catastrophically under real load; SQLite BUSY
/// pressure is now mitigated by WAL mode + `busy_timeout` pragmas in
/// `rs-core::db`.
pub struct ChunkUploader {
    pool: SqlitePool,
    s3: Arc<S3Client>,
    ws_tx: broadcast::Sender<WsEvent>,
    /// When true, workers skip uploads (simulates S3 outage for testing).
    upload_blocked: Arc<std::sync::atomic::AtomicBool>,
    metrics: Arc<UploadMetrics>,
    in_flight: Arc<AtomicUsize>,
    /// Workers that should voluntarily exit on next iteration (scale-down).
    drain_needed: Arc<AtomicUsize>,
    /// Per-installation UUID used to prefix S3 chunk keys (#114).
    client_uuid: String,
    /// Audit channel — `None` means no audit rows are emitted from the
    /// uploader (default; tests).
    audit_tx: Option<mpsc::Sender<AuditRow>>,
}

impl ChunkUploader {
    pub fn new(
        pool: SqlitePool,
        s3: S3Client,
        ws_tx: broadcast::Sender<WsEvent>,
        client_uuid: String,
    ) -> Self {
        Self {
            pool,
            s3: Arc::new(s3),
            ws_tx,
            upload_blocked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            metrics: Arc::new(UploadMetrics::default()),
            in_flight: Arc::new(AtomicUsize::new(0)),
            drain_needed: Arc::new(AtomicUsize::new(0)),
            client_uuid,
            audit_tx: None,
        }
    }

    /// Set a shared upload-blocked flag (for test API control).
    pub fn with_upload_blocked(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.upload_blocked = flag;
        self
    }

    /// Replace the default internal metrics with a shared instance.
    pub fn with_metrics(mut self, m: Arc<UploadMetrics>) -> Self {
        self.metrics = m;
        self
    }

    /// Attach an audit channel. Rate-limited S3UploadFailed rows will be
    /// emitted when a chunk upload fails.
    pub fn with_audit_tx(mut self, tx: mpsc::Sender<AuditRow>) -> Self {
        self.audit_tx = Some(tx);
        self
    }

    /// Expose metrics for the /uploads/stats API.
    pub fn metrics(&self) -> Arc<UploadMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Run the worker pool until shutdown signal.
    /// Starts with MIN_CONCURRENCY workers and scales up to MAX_CONCURRENCY
    /// based on observed error_rate and median upload latency.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        // Reset any in_process=1 rows orphaned by a prior crash.
        match db::reset_orphaned_in_process(&self.pool).await {
            Ok(n) if n > 0 => {
                warn!("Reset {n} orphaned in_process rows from prior run")
            }
            Ok(_) => {}
            Err(e) => error!("reset_orphaned_in_process failed: {e}"),
        }

        use tokio::sync::watch;
        let (target_tx, target_rx) = watch::channel::<usize>(MIN_CONCURRENCY);
        self.metrics.set_adaptive_target(MIN_CONCURRENCY);

        // Shared live-worker counter and drain signal.
        let live = Arc::new(AtomicUsize::new(0));
        let drain_needed = Arc::clone(&self.drain_needed);

        // 1. Spawn controller task (adaptive resizing every 10s)
        {
            let metrics = Arc::clone(&self.metrics);
            let tx = target_tx.clone();
            let drain = Arc::clone(&drain_needed);
            let mut shutdown_c = shutdown.resubscribe();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(10));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut current = MIN_CONCURRENCY;
                loop {
                    tokio::select! {
                        _ = shutdown_c.recv() => break,
                        _ = interval.tick() => {
                            let snap = metrics.snapshot(Duration::from_secs(10));
                            let next = adjust_target(current, snap.error_rate, snap.median_ms);
                            if next != current {
                                if next < current {
                                    // Tell (current - next) workers to exit voluntarily.
                                    drain.fetch_add(current - next, Ordering::SeqCst);
                                }
                                tracing::info!(
                                    "Adaptive concurrency {current} -> {next} (err={:.2}, med={}ms)",
                                    snap.error_rate, snap.median_ms,
                                );
                                current = next;
                                metrics.set_adaptive_target(current);
                                let _ = tx.send(current);
                            }
                        }
                    }
                }
            });
        }

        // 2. Spawner loop: spawn when live < target (allows regrowth after scale-down).
        let shared_ctx = WorkerCtx {
            pool: self.pool.clone(),
            s3: Arc::clone(&self.s3),
            ws_tx: self.ws_tx.clone(),
            metrics: Arc::clone(&self.metrics),
            in_flight: Arc::clone(&self.in_flight),
            blocked: Arc::clone(&self.upload_blocked),
            drain_needed: Arc::clone(&drain_needed),
            client_uuid: self.client_uuid.clone(),
            audit_tx: self.audit_tx.clone(),
        };
        let mut worker_id_counter: usize = 0;
        let mut rx = target_rx.clone();
        loop {
            let target = *rx.borrow_and_update();
            while should_spawn_worker(live.load(Ordering::SeqCst), target) {
                let idx = worker_id_counter;
                worker_id_counter += 1;
                let ctx = shared_ctx.clone();
                let live_for_worker = Arc::clone(&live);
                live.fetch_add(1, Ordering::SeqCst);
                let mut worker_shutdown = shutdown.resubscribe();
                tokio::spawn(async move {
                    supervise_future(idx, run_worker(idx, ctx, &mut worker_shutdown)).await;
                    live_for_worker.fetch_sub(1, Ordering::SeqCst);
                });
            }
            tokio::select! {
                _ = shutdown.recv() => break,
                changed = rx.changed() => {
                    if changed.is_err() { break; }
                }
            }
        }
    }
}

/// Wraps a future so that a panic is caught, logged, and does NOT propagate.
/// Safe to call at the top level of `tokio::spawn` closures.
async fn supervise_future<F: std::future::Future<Output = ()>>(label: usize, fut: F) {
    use futures::FutureExt;
    let wrapped = std::panic::AssertUnwindSafe(fut);
    if let Err(panic) = wrapped.catch_unwind().await {
        let msg = if let Some(s) = panic.downcast_ref::<&'static str>() {
            (*s).to_string()
        } else if let Some(s) = panic.downcast_ref::<String>() {
            s.clone()
        } else {
            "worker panic (unknown payload)".to_string()
        };
        error!(worker = label, "Upload worker panicked: {msg}");
    }
}

/// Worker loop: independently pick the next eligible chunk from the DB
/// (atomic SELECT+UPDATE `in_process=1`) and upload it. Voluntarily exits
/// when the adaptive controller signals a scale-down.
async fn run_worker(idx: usize, ctx: WorkerCtx, shutdown: &mut broadcast::Receiver<()>) {
    loop {
        // Voluntary drain: if the adaptive controller requested a scale-down,
        // one worker per iteration claims a drain token and exits.
        let want = ctx.drain_needed.load(Ordering::SeqCst);
        if want > 0
            && ctx
                .drain_needed
                .compare_exchange(want, want - 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            debug!(worker = idx, "Exiting voluntarily (drain requested)");
            return;
        }

        // Check shutdown at the top of each iteration
        if shutdown.try_recv().is_ok() {
            return;
        }

        // Test-only gate: if the uploader is "blocked" (simulated S3 outage),
        // sleep briefly and retry.
        if ctx.blocked.load(Ordering::Relaxed) {
            tokio::select! {
                _ = shutdown.recv() => return,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            continue;
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        match db::pick_next_uploadable_chunk(&ctx.pool, now_ms).await {
            Ok(None) => {
                tokio::select! {
                    _ = shutdown.recv() => return,
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                }
                continue;
            }
            Err(e) => {
                error!("Failed to pick next uploadable chunk: {e}");
                tokio::select! {
                    _ = shutdown.recv() => return,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }
            Ok(Some(chunk)) => {
                upload_one(&ctx, chunk).await;
            }
        }
    }
}

/// Perform a single chunk upload: record attempt → PUT to S3 → record
/// success OR failure. Called by `run_worker` after it has atomically
/// claimed a chunk via `pick_next_uploadable_chunk`.
async fn upload_one(ctx: &WorkerCtx, chunk: ChunkRecord) {
    let WorkerCtx {
        pool,
        s3,
        ws_tx,
        metrics,
        in_flight,
        client_uuid,
        ..
    } = ctx;

    let now_ms = chrono::Utc::now().timestamp_millis();

    // Resolve event identifier; if parent is gone, mark as sent and drop out of queue.
    let event_id = match db::get_streaming_event_by_id(pool, chunk.streaming_event_id).await {
        Ok(Some(ev)) => format!("{client_uuid}/{}", ev.name),
        _ => {
            warn!(
                "Chunk {} references missing/deleted event {}, marking complete",
                chunk.id, chunk.streaming_event_id
            );
            let _ = db::record_upload_success(pool, chunk.id, now_ms, 0).await;
            return;
        }
    };

    // Stage A: capture host_emit_ts immediately before the PUT so the
    // VPS can measure how long the chunk sat in the uploader queue.
    // Stamped AFTER event resolution so chunks belonging to deleted
    // events don't accumulate orphan timestamps. Best-effort — a warn
    // is logged but the upload proceeds regardless. (#184)
    let host_emit = now_millis();
    if let Err(e) = sqlx::query("UPDATE chunk_records SET host_emit_ts = ?1 WHERE id = ?2")
        .bind(host_emit)
        .bind(chunk.id)
        .execute(pool)
        .await
    {
        tracing::warn!(chunk_id = chunk.id, "stamp host_emit_ts failed: {e}");
    }

    let _ = db::record_upload_attempt(pool, chunk.id, now_ms).await;
    let attempt = chunk.upload_attempts + 1;
    let _ = ws_tx.send(WsEvent::ChunkUploadAttempt {
        chunk_id: chunk.id,
        attempt,
    });

    let n = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    metrics.set_in_flight(n);

    let mut meta = std::collections::HashMap::new();
    meta.insert("host-emit-ts".to_string(), host_emit.to_string());

    let started = Instant::now();
    let result = s3
        .upload_chunk_with_metadata(
            Path::new(&chunk.chunk_file_path),
            &event_id,
            chunk.sequence_number,
            chunk.duration_ms,
            meta,
        )
        .await;
    let duration = started.elapsed();
    let n = in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
    metrics.set_in_flight(n);

    // Stage B: capture s3_upload_complete_ts immediately after a successful
    // PUT 200. Best-effort — logged on error but does not affect success path.
    // s3-complete-ts is NOT added to the S3 metadata (stage B is DB-only). (#184)
    if result.is_ok() {
        let s3_complete = now_millis();
        if let Err(e) =
            sqlx::query("UPDATE chunk_records SET s3_upload_complete_ts = ?1 WHERE id = ?2")
                .bind(s3_complete)
                .bind(chunk.id)
                .execute(pool)
                .await
        {
            tracing::warn!(
                chunk_id = chunk.id,
                "stamp s3_upload_complete_ts failed: {e}"
            );
        }
    }

    match result {
        Ok(()) => {
            let completed_at = chrono::Utc::now().timestamp_millis();
            let _ = db::record_upload_success(
                pool,
                chunk.id,
                completed_at,
                duration.as_millis() as i64,
            )
            .await;
            let _ = tokio::fs::remove_file(&chunk.chunk_file_path).await;
            metrics.record(UploadEvent {
                at: Instant::now(),
                duration_ms: duration.as_millis() as u32,
                success: true,
            });
            debug!("Chunk {} uploaded to S3", chunk.id);
            let _ = ws_tx.send(WsEvent::ChunkUploaded { chunk_id: chunk.id });
        }
        Err(e) => {
            let err_msg = e.to_string();
            let class = classify_upload_error(&err_msg);
            // Continuity: network-class errors retry forever; only structural
            // rejects abandon after the budget.
            let permanent = should_abandon_upload(class, attempt);
            if permanent {
                let _ = db::mark_upload_permanently_failed(pool, chunk.id).await;
            } else {
                let backoff = backoff_ms(attempt) as i64;
                let next_retry = chrono::Utc::now().timestamp_millis() + backoff;
                let _ = db::record_upload_failure(
                    pool,
                    chunk.id,
                    &err_msg,
                    next_retry,
                    duration.as_millis() as i64,
                )
                .await;
            }
            let _ = ws_tx.send(WsEvent::ChunkUploadFailed {
                chunk_id: chunk.id,
                error: err_msg.clone(),
                permanent,
            });
            metrics.record(UploadEvent {
                at: Instant::now(),
                duration_ms: duration.as_millis() as u32,
                success: false,
            });

            // Audit: rate-limited S3UploadFailed keyed on error_class so a
            // long outage doesn't flood audit_log (1 row/min per class).
            // Permanent failures always emit (bypass rate limit) because
            // they are terminal for the chunk and worth surfacing.
            if let Some(tx) = &ctx.audit_tx {
                if permanent || UPLOAD_RL.allow(Action::S3UploadFailed, class) {
                    rs_core::audit::record(
                        tx,
                        AuditRow {
                            severity: if permanent {
                                Severity::Error
                            } else {
                                Severity::Warn
                            },
                            source: Source::Uploader,
                            event_id: Some(chunk.streaming_event_id),
                            instance_id: None,
                            endpoint: None,
                            action: Action::S3UploadFailed,
                            detail: serde_json::json!({
                                "chunk_id": chunk.id,
                                "error_class": class,
                                "error_msg": err_msg,
                                "permanent": permanent,
                                "attempt": attempt,
                            }),
                            ts_override: None,
                        },
                    );
                }
            }
        }
    }
}

/// Test-only driver that exercises the REAL `upload_one` path against a
/// mock S3 endpoint with an accelerated retry clock. Compiled only when the
/// `testing` feature (or `cfg(test)`) is on — never in release binaries.
/// Production retry/backoff code is left intact; we only shrink the test
/// clock by resetting `upload_next_retry_at` so the next pick is immediate.
#[cfg(any(test, feature = "testing"))]
pub(crate) mod testing_support {
    use super::*;
    use rs_core::config::S3Config;

    /// Drive the upload worker loop against `s3_endpoint`/`bucket` until no
    /// uploadable chunk remains (two consecutive empty picks) or a 30s wall
    /// deadline. Calls the unchanged `upload_one`, so the never-drop decision
    /// is exercised, not bypassed. After each failed `upload_one`, the failed
    /// chunk's `upload_next_retry_at` is forced to 0 so the next pick is
    /// immediate (5ms apart) instead of waiting the real capped backoff.
    pub async fn drive_until_idle(
        pool: &SqlitePool,
        s3_endpoint: &str,
        bucket: &str,
    ) -> anyhow::Result<()> {
        let s3 = S3Client::new(&S3Config {
            bucket: bucket.to_string(),
            region: "us-east-1".to_string(),
            endpoint: s3_endpoint.to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
        })
        .map_err(|e| anyhow::anyhow!("build mock S3 client: {e}"))?;

        let (ws_tx, _ws_rx) = broadcast::channel::<WsEvent>(64);
        let ctx = WorkerCtx {
            pool: pool.clone(),
            s3: Arc::new(s3),
            ws_tx,
            metrics: Arc::new(UploadMetrics::default()),
            in_flight: Arc::new(AtomicUsize::new(0)),
            blocked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            drain_needed: Arc::new(AtomicUsize::new(0)),
            client_uuid: "test-uuid".to_string(),
            audit_tx: None,
        };

        let deadline = Instant::now() + Duration::from_secs(30);
        let mut consecutive_empty = 0;
        while Instant::now() < deadline {
            let now_ms = chrono::Utc::now().timestamp_millis();
            match db::pick_next_uploadable_chunk(&ctx.pool, now_ms).await {
                Ok(Some(chunk)) => {
                    consecutive_empty = 0;
                    upload_one(&ctx, chunk).await;
                    // Accelerate the test clock: any chunk left unsent and not
                    // permanently failed becomes immediately re-pickable. This
                    // does NOT alter the production decision (still made by
                    // `should_abandon_upload`) — it only collapses the backoff
                    // wait so >15 retries finish in well under the deadline.
                    sqlx::query(
                        "UPDATE chunk_records SET upload_next_retry_at = 0 \
                         WHERE sent = 0 AND upload_failed_permanently = 0",
                    )
                    .execute(&ctx.pool)
                    .await?;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Ok(None) => {
                    consecutive_empty += 1;
                    if consecutive_empty >= 2 {
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                Err(e) => return Err(anyhow::anyhow!("pick chunk failed: {e}")),
            }
        }
        Err(anyhow::anyhow!("drive_until_idle hit the 30s deadline"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::config::S3Config;
    use rs_core::db;
    use std::time::Duration;

    #[test]
    fn network_class_errors_never_abandon_even_after_many_attempts() {
        // Continuity guarantee: a long outage must never drop a chunk.
        for class in ["timeout", "5xx", "conn", "other"] {
            assert!(
                !should_abandon_upload(class, 9_999),
                "network class {class} must retry forever, never abandon"
            );
        }
    }

    #[test]
    fn structural_reject_classes_abandon_only_after_budget() {
        // 403/404 mean S3 structurally rejected the object — retrying can
        // never succeed. Absorb a few transient auth/propagation hiccups,
        // then abandon.
        assert!(
            !should_abandon_upload("403", 4),
            "below budget: keep trying"
        );
        assert!(should_abandon_upload("403", 5), "at budget: abandon");
        assert!(should_abandon_upload("404", 50), "above budget: abandon");
    }

    #[test]
    fn http_400_abandons_only_after_budget() {
        // A 400 Bad Request PUT can never succeed by retrying — it is a
        // structural reject like 403/404. Absorb a few transient hiccups
        // (below budget), then abandon (at/above budget).
        assert!(
            !should_abandon_upload("400", 4),
            "below budget: keep trying"
        );
        assert!(should_abandon_upload("400", 5), "at budget: abandon");
    }

    fn test_s3_config() -> S3Config {
        S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
        }
    }

    async fn setup_db() -> SqlitePool {
        let pool = db::create_pool(std::path::Path::new(":memory:"))
            .await
            .unwrap();
        db::run_migrations(&pool).await.unwrap();
        db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
        pool
    }

    #[tokio::test]
    async fn uploader_shuts_down_cleanly() {
        let pool = setup_db().await;
        let s3 = S3Client::new(&test_s3_config()).unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, ws_tx, "test-client-uuid".to_string());
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });

        // Let workers spin once (should find no chunks and sleep 200ms)
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Signal shutdown
        let _ = shutdown_tx.send(());

        // Should complete without panic
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("uploader timed out")
            .expect("uploader panicked");
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_ms(1), 1000);
        assert_eq!(backoff_ms(2), 2000);
        assert_eq!(backoff_ms(3), 4000);
        assert_eq!(backoff_ms(4), 8000);
        assert_eq!(backoff_ms(5), 16_000);
        assert_eq!(backoff_ms(6), 30_000);
        assert_eq!(backoff_ms(100), 30_000);
    }

    #[test]
    fn backoff_attempt_zero_is_sane() {
        assert!(backoff_ms(0) >= 1000);
    }

    #[test]
    fn adaptive_scales_up_on_zero_errors_fast_median() {
        let mut target = 2usize;
        target = adjust_target(target, 0.0, 200);
        assert_eq!(target, 4);
        target = adjust_target(target, 0.0, 200);
        assert_eq!(target, 8);
        target = adjust_target(target, 0.0, 200);
        assert_eq!(target, 8, "capped at MAX_CONCURRENCY");
    }

    #[test]
    fn adaptive_scales_down_on_errors() {
        let mut target = 8usize;
        target = adjust_target(target, 0.3, 200);
        assert_eq!(target, 4);
        target = adjust_target(target, 0.3, 200);
        assert_eq!(target, 2);
        target = adjust_target(target, 0.3, 200);
        assert_eq!(target, 2, "capped at MIN_CONCURRENCY");
    }

    #[test]
    fn adaptive_holds_when_median_is_slow() {
        // error_rate = 0 but median >= 500ms → do not scale up
        assert_eq!(adjust_target(8, 0.0, 600), 8);
        assert_eq!(adjust_target(8, 0.0, 500), 8);
    }

    #[test]
    fn adaptive_holds_on_borderline_error_rate() {
        // error_rate = 0.2 exactly → does not scale down (strict >)
        assert_eq!(adjust_target(8, 0.2, 200), 8);
    }

    #[tokio::test]
    async fn uploader_metrics_getter_returns_shared_arc() {
        // Kills the `metrics()` survivor that returned Arc::new(Default::default())
        // instead of Arc::clone(&self.metrics).
        let pool = setup_db().await;
        let s3 = S3Client::new(&test_s3_config()).unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let uploader = ChunkUploader::new(pool, s3, ws_tx, "test-client-uuid".to_string());

        let a = uploader.metrics();
        let b = uploader.metrics();
        // Both calls must return handles to the same allocation.
        assert!(
            Arc::ptr_eq(&a, &b),
            "metrics() must return the internal Arc, not a fresh one"
        );
        // Mutating via one handle must be visible via the other.
        a.set_in_flight(42);
        let snap = b.snapshot(Duration::from_secs(1));
        assert_eq!(
            snap.in_flight, 42,
            "both handles must observe the same state"
        );
    }

    // --- supervise_future ---

    #[tokio::test]
    async fn supervise_future_catches_string_panic() {
        // Must NOT propagate the panic — if it did, the test itself would panic and fail.
        supervise_future(0, async { panic!("boom") }).await;
    }

    #[tokio::test]
    async fn supervise_future_returns_on_clean_exit() {
        supervise_future(0, async {}).await;
    }

    // --- should_spawn_worker / spawn gate ---

    #[test]
    fn spawn_gate_requests_new_worker_when_live_below_target() {
        assert!(should_spawn_worker(4, 8));
        assert!(!should_spawn_worker(4, 4));
        assert!(!should_spawn_worker(10, 4));
    }

    #[test]
    fn spawn_gate_reopens_after_drain_below_target() {
        // After scale-down 8→2, live drains from 8 toward 2.
        // Mid-drain (live=5) with target=2: no spawn.
        assert!(!should_spawn_worker(5, 2));
        // After drain (live=2) with target now raised to 4: spawn.
        assert!(should_spawn_worker(2, 4));
    }

    #[tokio::test]
    async fn now_millis_returns_non_zero_monotonic_value() {
        let a = super::now_millis();
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let b = super::now_millis();
        assert!(a > 0, "now_millis must be non-zero post-1970");
        assert!(
            b >= a,
            "now_millis must be monotonic across awaits (got {a} then {b})"
        );
    }

    #[tokio::test]
    async fn stamp_host_emit_and_s3_complete_columns_via_sql() {
        // Verifies the SQL the uploader will issue actually populates
        // both columns. Uses an in-memory pool seeded by the v24
        // migration from Task 6.
        let pool = rs_core::db::create_memory_pool().await.unwrap();
        rs_core::db::run_migrations(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO streaming_events(id, name, received_bytes, receiving_activated, delivering_activated) \
             VALUES (1,'evt',0,0,0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO chunk_records \
             (id, streaming_event_id, sequence_number, chunk_file_path, data_size, md5, sent, in_process, created_at, duration_ms) \
             VALUES (1,1,1,'/tmp/x',0,'',0,0,datetime('now'),2000)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let host_emit = super::now_millis();
        sqlx::query("UPDATE chunk_records SET host_emit_ts = ?1 WHERE id = ?2")
            .bind(host_emit)
            .bind(1i64)
            .execute(&pool)
            .await
            .unwrap();
        let s3_complete = super::now_millis();
        sqlx::query("UPDATE chunk_records SET s3_upload_complete_ts = ?1 WHERE id = ?2")
            .bind(s3_complete)
            .bind(1i64)
            .execute(&pool)
            .await
            .unwrap();
        let row =
            sqlx::query("SELECT host_emit_ts, s3_upload_complete_ts FROM chunk_records WHERE id=1")
                .fetch_one(&pool)
                .await
                .unwrap();
        let h: Option<i64> = sqlx::Row::try_get(&row, "host_emit_ts").unwrap();
        let s: Option<i64> = sqlx::Row::try_get(&row, "s3_upload_complete_ts").unwrap();
        assert!(h.is_some() && s.is_some());
        assert!(s.unwrap() >= h.unwrap());
    }
}
