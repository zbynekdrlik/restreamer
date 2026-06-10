/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
/// Producer -> bounded channel (~20s) -> Consumer (ffmpeg writer).
use async_trait::async_trait;
use rs_core::models::PusherKind;
use rs_ffmpeg::ServiceType;
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::sync::{Arc, atomic::Ordering as AtomicOrdering};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
pub use crate::buffer_state::{BufferState, initial_delivery_mode};
use crate::endpoint_audit;

#[path = "flv_normalizer.rs"]
mod flv_normalizer;
pub use flv_normalizer::FlvStreamNormalizer;

const MAX_FFMPEG_RESTARTS: u32 = 10;
// Producer-loop consts: `pub(crate)` so the extracted `endpoint_producer`
// module (and the producer tests in `fast_self_healing_tests`) can reach them.
pub(crate) const MAX_CHUNK_MISS_COUNT: u32 = 40; // ~80s at 2s polls
pub(crate) const SKIP_AHEAD_PROBE: i64 = 10;
pub(crate) const WRITE_TIMEOUT_SECS: u64 = 30;
const MAX_WRITE_FAILURES_PER_CHUNK: u32 = 3;
/// Base S3 backoff (doubles per error, max 60s, resets on success).
pub(crate) const S3_BACKOFF_BASE_SECS: u64 = 2;
pub(crate) const S3_BACKOFF_MAX_SECS: u64 = 60;
const ENDPOINT_HEARTBEAT_SECS: u64 = 60;
/// Pre-fetch buffer size: 10 chunks (~20s of media). disk_cache lives on
/// local SSD; this mpsc just smooths producer/consumer pacing. See #174.
pub(crate) const PREFETCH_BUFFER_SIZE: usize = 10;

/// A chunk that has been fetched from S3 and is ready for the consumer.
pub(crate) struct PrefetchedChunk {
    pub(crate) chunk_id: i64,
    pub(crate) data: Vec<u8>,
    pub(crate) duration_ms: i64,
}

/// Trait for fetching chunks (S3 or mock).
pub trait ChunkFetcher: Send + Sync {
    fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<(Vec<u8>, i64)>, String>> + Send;

    fn chunk_duration_ms(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<i64>, String>> + Send;
}

/// Trait for output process (ffmpeg or mock).
/// Uses async_trait for object safety (Box<dyn OutputProcess>).
#[async_trait]
pub trait OutputProcess: Send {
    fn is_alive(&mut self) -> bool;
    async fn write(&mut self, data: &[u8]) -> Result<(), String>;
    async fn kill(&mut self);
    fn last_stderr_line(&self) -> Option<String>;
}

/// Factory for spawning output processes.
pub trait OutputProcessFactory: Send + Sync {
    fn spawn(
        &self,
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
    ) -> Result<Box<dyn OutputProcess>, String>;
}

// Trait impls for the real `S3Fetcher` (ChunkFetcher) and real
// `FfmpegProcess` (OutputProcess) plus the `FfmpegProcessFactory` live
// in `endpoint_ffmpeg_impl.rs` so this file stays under the 1000-line
// CI cap (review finding #5). They are re-exported here so external
// callers continue to import `FfmpegProcessFactory` from
// `crate::endpoint_task`.
pub use crate::endpoint_ffmpeg_impl::FfmpegProcessFactory;

pub use crate::endpoint_audit::{
    EndpointRestartState, FfmpegRestartRecord, RESTART_HISTORY_CAP, RtmpPushAuditRecord,
};

#[path = "endpoint_consumer_helpers.rs"]
mod consumer_helpers;
use crate::disk_cache_push_sample::{PushSampleCtx, emit_push_sample};
use consumer_helpers::{FfmpegDeathAction, RustPushAction, handle_ffmpeg_death, handle_rust_push};

// EndpointStats + initial_endpoint_stats + Stats type alias
// extracted to crate::endpoint_stats so this file stays under the
// 1000-line CI cap (#184).
pub use crate::endpoint_stats::{
    EndpointStats, LifecycleSummary, PrefetchFill, Stats, initial_endpoint_stats,
};

pub struct EndpointHandle {
    task: JoinHandle<()>,
    stop_tx: watch::Sender<bool>,
    stats: Stats,
    start_chunk_id: i64,
    cfg: crate::api::EndpointConfig,
}

impl EndpointHandle {
    /// Spawn an endpoint task backed by the shared per-event `DiskCache`.
    /// `disk_cache` is required: if construction failed at /api/init, the
    /// orchestrator already received a 500 and never reaches here.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        ep_cfg: EndpointConfig,
        start_chunk_id: i64,
        delivery_delay_ms: u64,
        rescue_video_url: Option<String>,
        audit_ring: Option<Arc<AuditRing>>,
        disk_cache: Arc<crate::disk_cache::DiskCache>,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        let initial_mode = initial_delivery_mode(
            rescue_video_url.is_some(),
            ep_cfg.is_fast,
            delivery_delay_ms,
        );
        let stats: Stats = Arc::new(Mutex::new(initial_endpoint_stats(
            start_chunk_id,
            initial_mode,
        )));
        let buffer_state = Arc::new(BufferState::new());
        // Fast endpoints skip the delay entirely.
        let effective_delay = if ep_cfg.is_fast { 0 } else { delivery_delay_ms };
        let window = disk_cache.window_chunks;
        let fetcher = crate::disk_cache_fetcher::DiskCacheFetcher::new(
            disk_cache,
            ep_cfg.alias.clone(),
            start_chunk_id,
            window,
            60,
            audit_ring.clone(),
        );
        tracing::info!(alias = %ep_cfg.alias, window, "DiskCacheFetcher wired");
        // Clone for the spawned task so the original survives for the
        // EndpointHandle's `cfg` field. `cfg` powers the `config()` accessor
        // used by api::update_start_handler when it tears down and respawns
        // this endpoint with a new start_chunk_id (#189).
        let cfg = ep_cfg.clone();
        let task = tokio::spawn(endpoint_loop(
            fetcher,
            FfmpegProcessFactory,
            ep_cfg,
            start_chunk_id,
            effective_delay,
            stop_rx,
            stats.clone(),
            rescue_video_url,
            buffer_state,
            audit_ring,
        ));
        Self {
            task,
            stop_tx,
            stats,
            start_chunk_id,
            cfg,
        }
    }

    pub fn start_chunk_id(&self) -> i64 {
        self.start_chunk_id
    }

    pub fn config(&self) -> &crate::api::EndpointConfig {
        &self.cfg
    }

    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    pub async fn stats(&self) -> EndpointStats {
        self.stats.lock().await.clone()
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.task).await;
    }

    /// Test-only stub: creates a no-op EndpointHandle with the given start_chunk_id.
    /// Used by api_update_start_tests to seed AppState without a real DiskCache.
    #[cfg(test)]
    pub fn stub_for_test(start_chunk_id: i64) -> Self {
        let (stop_tx, _stop_rx) = watch::channel(false);
        let task = tokio::spawn(async {});
        let stats = Arc::new(Mutex::new(crate::endpoint_stats::initial_endpoint_stats(
            start_chunk_id,
            "normal".to_string(),
        )));
        let cfg = crate::api::EndpointConfig {
            alias: "stub".to_string(),
            service_type: "TEST_FILE".to_string(),
            stream_key: String::new(),
            is_fast: false,
            chunk_format: "flv".to_string(),
            start_chunk_id: None,
            pusher: Default::default(),
        };
        Self {
            task,
            stop_tx,
            stats,
            start_chunk_id,
            cfg,
        }
    }
}

use crate::endpoint_rtmp_url::build_rtmp_url;
#[cfg(test)]
pub(crate) use crate::endpoint_rtmp_url::build_rtmp_url_pub;

// `producer_task` was extracted to `crate::endpoint_producer` so this file
// stays under the 1000-line CI cap while `consumer_task` keeps room to grow.
// `endpoint_loop` calls it as `crate::endpoint_producer::producer_task(...)`.

/// Consumer task: pulls pre-fetched chunks from the channel, normalizes FLV,
/// writes to the configured output backend (ffmpeg subprocess or Rust RTMP
/// pusher). Never makes S3 calls -- zero network I/O.
///
/// Pacing is done by ffmpeg's `-re` flag alone (ffmpeg path). The
/// `FlvStreamNormalizer` rebases every process's input to start at PTS=0
/// so `-re` paces correctly from process start, and consumer writes are
/// naturally throttled by ffmpeg's stdin read rate. The previous Rust-side
/// pacing layer (removed 2026-04-21) was a workaround for the normalizer not
/// rebasing the first chunk per process -- it fought `-re` and caused
/// cumulative drift + cascading cache growth after ffmpeg restarts.
#[allow(clippy::too_many_arguments)]
async fn consumer_task<P: OutputProcessFactory>(
    mut rx: mpsc::Receiver<PrefetchedChunk>,
    factory: P,
    ep_cfg: EndpointConfig,
    delivery_delay_ms: u64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    rescue_video_url: Option<String>,
    buffer_state: Arc<BufferState>,
    audit_ring: Option<Arc<AuditRing>>,
) {
    let alias = ep_cfg.alias.clone();
    let service_type_str = ep_cfg.service_type.clone();

    let service_type: ServiceType = match ep_cfg.service_type.parse() {
        Ok(st) => st,
        Err(e) => {
            tracing::error!(alias = %alias, "Unknown service type '{}': {e}", ep_cfg.service_type);
            return;
        }
    };

    let mut flv_normalizer = FlvStreamNormalizer::new();
    // `proc` is the ffmpeg-path output handle (None when using Rust pusher).
    let mut proc: Option<Box<dyn OutputProcess>> = None;
    // `rust_pusher` is the Rust-path output handle (None when using ffmpeg).
    let mut rust_pusher: Option<RtmpPusher> = None;
    let mut consecutive_ffmpeg_failures: u32 = 0;
    // Class-aware backoff: tracks the ReasonClass of the most recent death
    // and how many deaths in a row shared that class. See `ffmpeg_reason`.
    let mut restart_state = EndpointRestartState::new();
    let mut proc_spawned_at: Option<tokio::time::Instant> = None;
    let mut circuit_trips: u32 = 0;
    let mut consecutive_write_failures: u32 = 0;
    // Last delivered chunk id, recorded in the rescue audit row on stall.
    let mut last_delivered_chunk_id: i64 = -1;
    // Last full FLV chunk pushed — replayed as a freeze during keepalive.
    // Only populated for fast endpoints (avoids a per-chunk clone on the
    // high-bitrate normal endpoints).
    let mut last_chunk_bytes: Option<std::sync::Arc<Vec<u8>>> = None;
    let mut last_heartbeat = std::time::Instant::now();
    // Consecutive push errors for the Rust pusher exponential backoff ladder.
    let mut consecutive_push_errors: u32 = 0;
    // Phase 1 telemetry for the Rust RTMP pusher -- reset on each connect.
    let mut rust_telemetry = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();
    // Phase 1 (#176): per-consumer rate limiter + clocks for DiskCachePushSample.
    let push_audit_rl = rs_core::audit::RateLimiter::new();
    let push_ctx = PushSampleCtx::new(&audit_ring, &push_audit_rl, &alias, delivery_delay_ms);

    let use_rust_pusher = ep_cfg.pusher == PusherKind::Rust;

    if use_rust_pusher {
        // Rust pusher: lazy-connect on first write. Construct now so the
        // handle is available for the whole consumer lifetime.
        let url = build_rtmp_url(service_type, &ep_cfg.stream_key);
        tracing::info!(alias = %alias, url = %url, "Consumer: endpoint delivery configured (Rust RTMP pusher)");
        rust_pusher = Some(RtmpPusher::new(url, PusherConfig::default()));
    } else {
        // Rust-side pacing was removed 2026-04-21. It fought against ffmpeg
        // `-re`: consumer tried to sleep between writes, but ffmpeg pipe
        // backpressure from `-re` already throttled consumer writes, and the
        // two layers together caused (a) cumulative drift as pacing errors
        // accumulated, (b) broken catchup after ffmpeg restart. With the FLV
        // normalizer now rebasing each ffmpeg process's input stream to
        // PTS=0, ffmpeg `-re` alone paces correctly.
        tracing::info!(alias = %alias, "Consumer: endpoint delivery configured (FLV-only)");
    }

    loop {
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Consumer: stop signal received");
            break;
        }

        // Periodic heartbeat
        if last_heartbeat.elapsed() >= std::time::Duration::from_secs(ENDPOINT_HEARTBEAT_SECS) {
            tracing::info!(
                alias = %alias,
                ffmpeg_alive = proc.as_mut().is_some_and(|p| p.is_alive()),
                "Consumer: delivery endpoint heartbeat"
            );
            last_heartbeat = std::time::Instant::now();
        }

        if use_rust_pusher {
            // Rust pusher path: pusher is always present (lazy-reconnects
            // on next push_flv_bytes call after an error). No process
            // lifecycle management needed here — errors are handled in the
            // write section below.
        } else {
            // ffmpeg path: ensure output process is running.
            if !proc.as_mut().is_some_and(|p| p.is_alive()) {
                if proc.is_some() {
                    // ffmpeg died -- delegate to helper to keep this
                    // function under the 1000-line gate.
                    match handle_ffmpeg_death(
                        &mut proc,
                        proc_spawned_at,
                        &mut restart_state,
                        &service_type_str,
                        &alias,
                        &stats,
                        &audit_ring,
                        &mut stop_rx,
                        &mut flv_normalizer,
                    )
                    .await
                    {
                        FfmpegDeathAction::Break => break,
                        FfmpegDeathAction::Respawn => {}
                    }
                }

                match factory.spawn(service_type, &ep_cfg.stream_key, &alias) {
                    Ok(new_proc) => {
                        tracing::info!(alias = %alias, "Consumer: ffmpeg started");
                        // Previous spawn existed iff we've ever tracked a start
                        // time. The death handler above keeps `proc` as `Some`
                        // with a dead child, so `proc.is_none()` alone cannot
                        // distinguish first spawn from restart.
                        let was_dead = proc_spawned_at.is_some();
                        proc = Some(new_proc);
                        proc_spawned_at = Some(tokio::time::Instant::now());
                        consecutive_ffmpeg_failures = 0;
                        let mut s = stats.lock().await;
                        s.consecutive_ffmpeg_failures = 0;
                        if s.stall_reason.as_deref() == Some("ffmpeg_crash_loop") {
                            s.stall_reason = None;
                        }
                        drop(s);
                        endpoint_audit::emit_spawn_success(
                            &audit_ring,
                            &alias,
                            &ep_cfg.service_type,
                            ep_cfg.stream_key.len(),
                            was_dead,
                        );
                    }
                    Err(e) => {
                        consecutive_ffmpeg_failures += 1;
                        let mut s = stats.lock().await;
                        s.consecutive_ffmpeg_failures = consecutive_ffmpeg_failures;
                        s.last_error = Some(e.clone());
                        drop(s);
                        endpoint_audit::emit_spawn_failed(
                            &audit_ring,
                            &alias,
                            consecutive_ffmpeg_failures,
                            &e,
                        );

                        if consecutive_ffmpeg_failures >= MAX_FFMPEG_RESTARTS {
                            circuit_trips += 1;
                            let cooldown = (30 * 2u64.pow(circuit_trips.min(4) - 1)).min(300);
                            tracing::error!(
                                alias = %alias,
                                failures = consecutive_ffmpeg_failures,
                                circuit_trip = circuit_trips,
                                "Consumer: ffmpeg circuit breaker #{circuit_trips}, cooldown {cooldown}s"
                            );
                            let mut s = stats.lock().await;
                            s.stall_reason = Some("ffmpeg_crash_loop".to_string());
                            drop(s);
                            let sleep_dur = std::time::Duration::from_secs(cooldown);
                            tokio::select! {
                                _ = tokio::time::sleep(sleep_dur) => {}
                                _ = stop_rx.changed() => {
                                    if *stop_rx.borrow() { break; }
                                }
                            }
                            consecutive_ffmpeg_failures = 0;
                            let mut s = stats.lock().await;
                            s.consecutive_ffmpeg_failures = 0;
                        } else {
                            tracing::error!(alias = %alias, "Consumer: failed to spawn ffmpeg: {e}");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        }
                        continue;
                    }
                }
            }
        }

        // Pull next chunk from channel (rescue-mode-aware).
        //
        // FAST + rust pusher only: a short producer gap triggers the
        // never-crash keepalive (freeze last chunk → default rescue) on the
        // SAME rtmp session, so starvation never tears the connection down.
        // EVERY other path (normal YT/FB, any ffmpeg endpoint) keeps the
        // existing select! verbatim — the 8s `run_outage_rescue` behaviour is
        // unchanged byte-for-byte.
        let chunk = if ep_cfg.is_fast && use_rust_pusher {
            tokio::select! {
                maybe_chunk = rx.recv() => {
                    match maybe_chunk {
                        Some(c) => {
                            // SAME buffer bookkeeping as the existing Some(c) arm.
                            let dur = c.duration_ms.max(0) as u64;
                            let current = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
                            buffer_state.buffer_duration_ms.store(current.saturating_sub(dur), AtomicOrdering::Relaxed);
                            last_delivered_chunk_id = c.chunk_id;
                            c
                        }
                        None => {
                            // SAME defensive-rescue None branch as the existing
                            // path (producer gone, rescue before teardown).
                            tracing::warn!(
                                alias = %alias,
                                "Consumer: producer gone, entering defensive rescue before teardown"
                            );
                            let svc_type: rs_ffmpeg::ServiceType = ep_cfg
                                .service_type
                                .parse()
                                .unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                            crate::rescue::run_defensive_rescue(
                                &alias,
                                rescue_video_url.as_deref(),
                                svc_type,
                                &ep_cfg.stream_key,
                                &buffer_state,
                                &stats,
                                &mut stop_rx,
                                &audit_ring,
                                last_delivered_chunk_id,
                                &mut proc,
                                &mut rust_pusher,
                            )
                            .await;
                            break;
                        }
                    }
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(crate::fast_keepalive::FAST_KEEPALIVE_TRIGGER_SECS)) => {
                    // Producer gap exceeded the fast trigger. Feed the existing
                    // session with keepalive frames until a real chunk arrives.
                    // The `if let` borrow of `rust_pusher` is scoped to this
                    // arm: it ends when the arm produces the owned chunk value,
                    // BEFORE the write section below re-borrows `rust_pusher`.
                    if let Some(ref mut pusher) = rust_pusher {
                        match keepalive_until_chunk(
                            pusher,
                            &mut rx,
                            &last_chunk_bytes,
                            &alias,
                            &audit_ring,
                            &mut stop_rx,
                            &stats,
                            &buffer_state,
                        )
                        .await
                        {
                            Some(c) => {
                                // SAME buffer bookkeeping as the Some(c) arm.
                                let dur = c.duration_ms.max(0) as u64;
                                let current = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
                                buffer_state.buffer_duration_ms.store(current.saturating_sub(dur), AtomicOrdering::Relaxed);
                                last_delivered_chunk_id = c.chunk_id;
                                c
                            }
                            None => break,
                        }
                    } else {
                        continue;
                    }
                }
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() {
                        tracing::info!(alias = %alias, "Consumer: stop signal during recv");
                        break;
                    }
                    continue;
                }
            }
        } else {
            // EXISTING chunk-pull select! — non-fast and ffmpeg paths, UNCHANGED.
            tokio::select! {
            maybe_chunk = rx.recv() => {
                match maybe_chunk {
                    Some(c) => {
                        // Decrease buffer duration tracking as consumer pulls chunks
                        let dur = c.duration_ms.max(0) as u64;
                        let current = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
                        buffer_state.buffer_duration_ms.store(current.saturating_sub(dur), AtomicOrdering::Relaxed);
                        last_delivered_chunk_id = c.chunk_id;
                        c
                    }
                    None => {
                        // R2 GREEN (Task 8 scoped, 2026-05-31): defensive
                        // rescue before teardown. Producer disappeared
                        // (panic or stop signal closed the channel). The
                        // helper pushes DEFAULT_RESCUE_FLV (or operator's
                        // custom URL) until the endpoint_task select-loop
                        // tears us down via the consumer-drain timeout
                        // (~30s) or a stop signal arrives. Viewers see
                        // rescue during the teardown window instead of
                        // immediate dark.
                        //
                        // Extracted to `rescue::run_defensive_rescue` so
                        // this fn stays under the 1000-line CI cap and so
                        // the review-finding fixes (#1 drop rust_pusher,
                        // #4 skip emit_recovered) live in one place.
                        tracing::warn!(
                            alias = %alias,
                            "Consumer: producer gone, entering defensive rescue before teardown"
                        );
                        let svc_type: rs_ffmpeg::ServiceType = ep_cfg
                            .service_type
                            .parse()
                            .unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                        crate::rescue::run_defensive_rescue(
                            &alias,
                            rescue_video_url.as_deref(),
                            svc_type,
                            &ep_cfg.stream_key,
                            &buffer_state,
                            &stats,
                            &mut stop_rx,
                            &audit_ring,
                            last_delivered_chunk_id,
                            &mut proc,
                            &mut rust_pusher,
                        )
                        .await;
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS)) => {
                // R1 GREEN (Task 6, 2026-05-31): rescue fires whenever the
                // buffer is empty AND the producer is stalled — regardless
                // of whether the operator configured a custom rescue URL.
                // The pure-rust rescue path (run_rescue_loop →
                // resolve_rescue_bytes → rust_rescue_push) substitutes
                // DEFAULT_RESCUE_FLV when URL is None, so the cache-drain
                // branch always has bytes to push instead of going dark.
                // Closes the 2026-05-30 stream.lan crash root cause where
                // all 5 production templates had rescue_video_url = NULL
                // and consumers fell silent.
                if !buffer_state.producer_active.load(AtomicOrdering::Relaxed) {
                    tracing::warn!(alias = %alias, "Consumer: buffer empty + producer stalled, entering rescue mode");
                    let svc_type: rs_ffmpeg::ServiceType =
                        ep_cfg.service_type.parse().unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                    // Extracted to `rescue::run_outage_rescue` so this fn
                    // stays under the 1000-line CI cap and so the
                    // review-finding #1 fix (drop+reconstruct rust_pusher
                    // around rescue) lives in one place. See the helper's
                    // doc comment for the full rationale.
                    let outcome = crate::rescue::run_outage_rescue(
                        &alias,
                        rescue_video_url.as_deref(),
                        svc_type,
                        &ep_cfg.stream_key,
                        &buffer_state,
                        &stats,
                        &mut stop_rx,
                        &audit_ring,
                        last_delivered_chunk_id,
                        &mut proc,
                        &mut rust_pusher,
                        use_rust_pusher,
                    )
                    .await;
                    match outcome {
                        crate::rescue::OutageRescueOutcome::Stop => return,
                        crate::rescue::OutageRescueOutcome::Recovered => {
                            flv_normalizer = FlvStreamNormalizer::new();
                        }
                    }
                }
                continue;
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    tracing::info!(alias = %alias, "Consumer: stop signal during recv");
                    break;
                }
                continue;
            }
            }
        };

        let chunk_id = chunk.chunk_id;
        let chunk_duration_ms = chunk.duration_ms;

        if use_rust_pusher {
            // Rust RTMP pusher write path. Bypasses flv_normalizer because
            // each S3 chunk is already a self-contained FLV with its own
            // 9-byte header; the rust pusher's push_flv_bytes parses it as
            // a complete FLV and applies its own monotonic-timestamp logic
            // via state.last_output_ts_ms. The normalizer would strip the
            // header on subsequent chunks (correct for ffmpeg's `-re -f flv
            // -i pipe:` which only needs the header on the first write),
            // leaving the pusher with raw tag bytes that fail the FLV
            // signature check at offset 0.
            if let Some(ref mut pusher) = rust_pusher {
                let action = handle_rust_push(
                    pusher,
                    &chunk.data,
                    chunk_id,
                    chunk_duration_ms,
                    &alias,
                    &service_type_str,
                    &mut consecutive_push_errors,
                    &mut consecutive_write_failures,
                    &stats,
                    &audit_ring,
                    &mut rust_telemetry,
                    &mut stop_rx,
                    &mut flv_normalizer,
                )
                .await;
                match action {
                    RustPushAction::Continue => {}
                    RustPushAction::Break => break,
                }
                if matches!(action, RustPushAction::Continue) {
                    // cumulative media pushed (≈ stream age), NOT behind-live (#232)
                    let cumulative_pushed_secs =
                        stats.lock().await.duration_processed_ms as f64 / 1000.0;
                    emit_push_sample(
                        &push_ctx,
                        chunk_id,
                        chunk_duration_ms,
                        cumulative_pushed_secs,
                    );
                    // Fast endpoints only: remember the chunk so keepalive can
                    // replay it as a freeze during a producer gap. Skipped on
                    // normal endpoints to avoid the per-chunk clone.
                    if ep_cfg.is_fast {
                        last_chunk_bytes = Some(std::sync::Arc::new(chunk.data.clone()));
                    }
                }
            }
        } else if let Some(ref mut p) = proc {
            // ffmpeg write path: normalize FLV (PTS rebase, header strip
            // on subsequent chunks) so ffmpeg's `-re` paces correctly and
            // duplicate codec config packets don't break the muxer.
            let processed = flv_normalizer.normalize(&chunk.data);
            let write_result = tokio::time::timeout(
                std::time::Duration::from_secs(WRITE_TIMEOUT_SECS),
                p.write(&processed),
            )
            .await;

            match write_result {
                Ok(Ok(())) => {
                    consecutive_write_failures = 0;
                    if circuit_trips > 0 {
                        circuit_trips = 0;
                        tracing::info!(alias = %alias, "Consumer: circuit breaker reset");
                    }
                    let mut s = stats.lock().await;
                    s.bytes_processed_total += processed.len() as u64;
                    s.duration_processed_ms += chunk_duration_ms.max(0) as u64;
                    s.current_chunk_id = chunk_id;
                    s.chunks_processed += 1;
                    // cumulative media pushed (≈ stream age), NOT behind-live;
                    // same meaning/key as the rust path above (#232)
                    let cumulative_pushed_secs = s.duration_processed_ms as f64 / 1000.0;
                    drop(s);
                    emit_push_sample(
                        &push_ctx,
                        chunk_id,
                        chunk_duration_ms,
                        cumulative_pushed_secs,
                    );
                }
                Ok(Err(e)) => {
                    consecutive_write_failures += 1;
                    tracing::warn!(alias = %alias, chunk_id, failures = consecutive_write_failures, "Consumer: ffmpeg write failed: {e}");
                    let mut s = stats.lock().await;
                    s.last_error = Some(e);
                    drop(s);
                    // Kill the process IN PLACE -- leave the Option as Some
                    // so the death handler at the top of the loop catches
                    // it and applies backoff + audit log on the next
                    // iteration. Previously this called proc.take() which
                    // left proc=None, and the death handler's `if
                    // proc.is_some()` check skipped both backoff AND audit.
                    //
                    // Race note: on a real FfmpegProcess, kill() sends
                    // SIGKILL but is_alive() reads the child's try_wait()
                    // exit code non-blockingly. There's a ~ms window where
                    // is_alive() may still return true on the next loop
                    // iteration, causing one more write attempt before the
                    // death handler catches it. The 10ms sleep below gives
                    // the OS time to reap the child so the death handler
                    // runs on the next iteration with accurate
                    // lifetime_secs in the audit entry.
                    if let Some(p) = proc.as_mut() {
                        p.kill().await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if consecutive_write_failures >= MAX_WRITE_FAILURES_PER_CHUNK {
                        tracing::error!(alias = %alias, chunk_id, "Consumer: skipping chunk after {consecutive_write_failures} write failures");
                        consecutive_write_failures = 0;
                        flv_normalizer = FlvStreamNormalizer::new();
                        let mut s = stats.lock().await;
                        s.current_chunk_id = chunk_id;
                    }
                    continue;
                }
                Err(_) => {
                    consecutive_write_failures += 1;
                    tracing::error!(alias = %alias, chunk_id, failures = consecutive_write_failures, "Consumer: ffmpeg write timed out");
                    let mut s = stats.lock().await;
                    s.last_error = Some("write_timeout".to_string());
                    s.stall_reason = Some("write_timeout".to_string());
                    drop(s);
                    // Same fix as above -- kill in place, let the death
                    // handler record the audit entry and apply backoff.
                    // The 10ms post-kill sleep gives the OS a chance to
                    // reap the child so is_alive() returns false on the
                    // next iteration. See the race note in the Ok(Err)
                    // branch above.
                    if let Some(p) = proc.as_mut() {
                        p.kill().await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if consecutive_write_failures >= MAX_WRITE_FAILURES_PER_CHUNK {
                        tracing::error!(alias = %alias, chunk_id, "Consumer: skipping chunk after {consecutive_write_failures} write timeouts");
                        consecutive_write_failures = 0;
                        flv_normalizer = FlvStreamNormalizer::new();
                        let mut s = stats.lock().await;
                        s.current_chunk_id = chunk_id;
                    }
                    continue;
                }
            }
        }
    }

    // Cleanup
    if let Some(mut p) = proc {
        p.kill().await;
    }
    if let Some(mut pusher) = rust_pusher {
        pusher.close().await;
    }
    tracing::info!(alias = %alias, "Consumer task stopped");
}

/// Keep the existing rust session alive during a fast-endpoint producer gap.
/// Returns the next real chunk when one arrives, or `None` on stop/closed
/// channel. NEVER closes the connection on starvation — a push error just
/// backs off briefly and the pusher lazy-reconnects on the next push.
///
/// Sets `stats.delivery_mode = "rescue"` on entry and resets it to
/// `"normal"` on both exit paths so the dashboard correctly reflects the
/// keepalive gap instead of showing stale `"normal"` state.
#[allow(clippy::too_many_arguments)]
async fn keepalive_until_chunk<P: consumer_helpers::Pushable>(
    pusher: &mut P,
    rx: &mut tokio::sync::mpsc::Receiver<PrefetchedChunk>,
    last_chunk_bytes: &Option<std::sync::Arc<Vec<u8>>>,
    alias: &str,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    stats: &Stats,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
) -> Option<PrefetchedChunk> {
    use crate::fast_keepalive::{KeepaliveMode, keepalive_bytes, keepalive_mode};
    // `tokio::time::Instant` (not `std::time::Instant`) so the gap clock shares
    // the loop's `tokio::time::sleep` time source: identical to the real clock
    // in prod, but advances under `start_paused` so the freeze→rescue transition
    // is deterministically testable without wall-clock waits.
    let started = tokio::time::Instant::now();
    let mut current_mode = keepalive_mode(0, last_chunk_bytes.is_some());
    crate::fast_delay_audit::emit_keepalive_started(
        audit_ring,
        alias,
        if current_mode == KeepaliveMode::Rescue {
            "rescue"
        } else {
            "freeze"
        },
    );
    // Surface keepalive gap on the dashboard: set delivery_mode to "rescue"
    // so the UI doesn't falsely show "normal" during the starvation window.
    // Lock is released immediately (not held across any .await).
    {
        let mut s = stats.lock().await;
        s.delivery_mode = "rescue".to_string();
    }
    loop {
        let gap = started.elapsed().as_secs();
        let mode = keepalive_mode(gap, last_chunk_bytes.is_some());
        if mode != current_mode {
            current_mode = mode;
            crate::fast_delay_audit::emit_keepalive_started(
                audit_ring,
                alias,
                if mode == KeepaliveMode::Rescue {
                    "rescue"
                } else {
                    "freeze"
                },
            );
        }
        // Finding 2: keep the Cow (borrowing last_chunk_bytes) instead of
        // cloning with .into_owned() — avoids a multi-MB heap allocation on
        // every keepalive tick for the freeze window.
        let bytes = keepalive_bytes(mode, last_chunk_bytes);
        tokio::select! {
            maybe = rx.recv() => {
                // Trickle-grow: feed the TRUE measured gap to the producer's
                // adaptive controller (consumed via BufferState on its next fetch).
                let elapsed = consumer_helpers::record_starvation_gap(buffer_state, started);
                crate::fast_delay_audit::emit_keepalive_ended(audit_ring, alias, elapsed.as_secs());
                // Reset delivery_mode so the dashboard reflects normal delivery.
                {
                    let mut s = stats.lock().await;
                    s.delivery_mode = "normal".to_string();
                }
                return maybe;
            }
            res = pusher.push_flv_bytes(&bytes) => {
                if let Err(e) = res {
                    tracing::warn!(alias = %alias, "keepalive push error: {e}; will reconnect on next push");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                // push_flv_bytes self-paces ~1x; loop to push the next tick.
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    crate::fast_delay_audit::emit_keepalive_ended(audit_ring, alias, started.elapsed().as_secs());
                    // Reset delivery_mode before returning None (stop path).
                    {
                        let mut s = stats.lock().await;
                        s.delivery_mode = "normal".to_string();
                    }
                    return None;
                }
            }
        }
    }
}

/// Core endpoint loop -- generic over ChunkFetcher and OutputProcessFactory for testability.
/// Orchestrates buffer fill, then spawns producer-consumer pipeline.
#[allow(clippy::too_many_arguments)]
pub async fn endpoint_loop<F: ChunkFetcher + 'static, P: OutputProcessFactory + 'static>(
    fetcher: F,
    factory: P,
    ep_cfg: EndpointConfig,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    rescue_video_url: Option<String>,
    buffer_state: Arc<BufferState>,
    audit_ring: Option<Arc<AuditRing>>,
) {
    let alias = ep_cfg.alias.clone();

    // Wait for enough duration to buffer before starting (duration-based approach).
    // When rescue_video_url is configured and the endpoint is not fast, the
    // helper also spawns a rescue ffmpeg in parallel so viewers see the
    // rescue video (with countdown) during the initial cache fill. Without
    // this, viewers see nothing until ~120s of buffer has accumulated.
    if delivery_delay_ms > 0 {
        let stopped = crate::rescue::run_warmup_loop(
            &fetcher,
            &alias,
            &ep_cfg,
            start_chunk_id,
            delivery_delay_ms,
            rescue_video_url.as_deref(),
            &stats,
            &mut stop_rx,
            audit_ring.as_ref(),
        )
        .await;
        if stopped {
            return;
        }
    }

    tracing::info!(alias = %alias, "Starting producer-consumer pipeline");

    // Create bounded channel for pre-fetch buffer
    let (tx, rx) = mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);

    let producer_stop = stop_rx.clone();
    let producer_stats = stats.clone();
    let producer_alias = alias.clone();
    let producer_buffer_state = buffer_state.clone();
    let producer_audit_ring = audit_ring.clone();
    // `is_fast` is Copy — read it before `ep_cfg` is moved into consumer_task.
    let producer_is_fast = ep_cfg.is_fast;
    let producer = tokio::spawn(crate::endpoint_producer::producer_task(
        fetcher,
        tx,
        start_chunk_id,
        delivery_delay_ms,
        producer_is_fast,
        producer_stop,
        producer_stats,
        producer_alias,
        producer_buffer_state,
        producer_audit_ring,
    ));

    let consumer_stop = stop_rx.clone();
    let consumer_stats = stats.clone();
    let consumer = tokio::spawn(consumer_task(
        rx,
        factory,
        ep_cfg,
        delivery_delay_ms,
        consumer_stop,
        consumer_stats,
        rescue_video_url,
        buffer_state,
        audit_ring,
    ));

    // Wait for either task to finish or stop signal.
    // Both producer and consumer already listen for stop_rx internally,
    // but we also watch here for cleanup coordination.
    tokio::pin!(producer);
    tokio::pin!(consumer);

    loop {
        tokio::select! {
            result = &mut producer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Producer panicked: {e}");
                }
                tracing::info!(alias = %alias, "Producer finished, waiting for consumer to drain");
                // Consumer will stop when channel is drained (recv returns None)
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    &mut consumer,
                ).await;
                break;
            }
            result = &mut consumer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Consumer panicked: {e}");
                }
                tracing::info!(alias = %alias, "Consumer finished, aborting producer");
                producer.abort();
                break;
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    tracing::info!(alias = %alias, "Stop signal received, aborting pipeline");
                    producer.abort();
                    consumer.abort();
                    break;
                }
            }
        }
    }

    tracing::info!(alias = %alias, "Endpoint pipeline stopped");
}

#[cfg(test)]
#[path = "endpoint_task_test_root.rs"]
mod test_root;
