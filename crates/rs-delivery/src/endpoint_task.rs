/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
/// Producer -> bounded channel (~20s) -> Consumer (ffmpeg writer).
use rs_core::models::PusherKind;
use rs_ffmpeg::ServiceType;
use rs_rtmp_push::{PusherConfig, RtmpPusher};
use std::sync::{Arc, atomic::Ordering as AtomicOrdering};
use tokio::sync::{mpsc, watch};

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

// Core pipeline traits + the `PrefetchedChunk` value type live in
// `endpoint_traits.rs` (a `#[path]` submodule of `endpoint_task`) so this file
// stays under the 1000-line CI cap. Re-exported at the `endpoint_task` level so
// every existing `crate::endpoint_task::{ChunkFetcher, OutputProcess,
// OutputProcessFactory, PrefetchedChunk}` import path keeps resolving unchanged.
#[path = "endpoint_traits.rs"]
mod endpoint_traits;
pub(crate) use endpoint_traits::PrefetchedChunk;
pub use endpoint_traits::{ChunkFetcher, OutputProcess, OutputProcessFactory};

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

// Fast-endpoint keepalive + outage escalation (C1 #251) extracted to keep this
// file under the 1000-line CI cap. The module is a `#[path]` submodule of
// `endpoint_task` (like `consumer_helpers`), so its items reach back via
// `super::`. The two public items are re-exported below so the `consumer_task`
// call site AND the tests (which reach them via
// `super::super::super::{KeepaliveOutcome, keepalive_until_chunk}`) keep
// resolving them at the `endpoint_task` level unchanged.
#[path = "fast_keepalive_escalation.rs"]
mod fast_keepalive_escalation;
pub(crate) use fast_keepalive_escalation::{KeepaliveOutcome, keepalive_until_chunk};

// EndpointStats + initial_endpoint_stats + Stats type alias
// extracted to crate::endpoint_stats so this file stays under the
// 1000-line CI cap (#184).
pub use crate::endpoint_stats::{
    EndpointStats, LifecycleSummary, PrefetchFill, Stats, initial_endpoint_stats,
};

// `EndpointHandle` (the owning handle + its lifecycle methods) lives in
// `endpoint_handle.rs` (a `#[path]` submodule of `endpoint_task`) so this file
// stays under the 1000-line CI cap. Re-exported at the `endpoint_task` level so
// `crate::endpoint_task::EndpointHandle` (and `crate::EndpointHandle` via
// main.rs) keep resolving unchanged.
#[path = "endpoint_handle.rs"]
mod endpoint_handle;
pub use endpoint_handle::EndpointHandle;

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
                    // session with keepalive frames until a real chunk arrives
                    // OR the gap becomes a sustained outage (escalate to rescue).
                    // The keepalive's borrow of `rust_pusher` is scoped to the
                    // inner block so it ends BEFORE the EscalateToRescue arm
                    // re-borrows `rust_pusher` for `run_outage_rescue`.
                    let outcome = if let Some(ref mut pusher) = rust_pusher {
                        keepalive_until_chunk(
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
                    } else {
                        continue;
                    };
                    match outcome {
                        KeepaliveOutcome::Chunk(c) => {
                            // SAME buffer bookkeeping as the Some(c) arm.
                            let dur = c.duration_ms.max(0) as u64;
                            let current = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
                            buffer_state.buffer_duration_ms.store(current.saturating_sub(dur), AtomicOrdering::Relaxed);
                            last_delivered_chunk_id = c.chunk_id;
                            c
                        }
                        KeepaliveOutcome::Stop => break,
                        KeepaliveOutcome::EscalateToRescue => {
                            // C1 (#251): sustained outage on a fast endpoint.
                            // Keepalive could not hold the live session (frozen
                            // or dark) — switch to the SAME fresh-session
                            // rescue the non-fast 8s arm uses. NEVER spliced
                            // into the live session (that is the #249 green-
                            // video corruption); run_outage_rescue drops the
                            // existing rust_pusher and reconnects FRESH for the
                            // rescue clip, then reconstructs the pusher on
                            // recovery so the fast low-latency path resumes.
                            let svc_type: rs_ffmpeg::ServiceType = ep_cfg
                                .service_type
                                .parse()
                                .unwrap_or(rs_ffmpeg::ServiceType::TestFile);
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
                                    // The last-delivered chunk is from BEFORE the
                                    // rescue gap; clear it so the post-rescue
                                    // session never re-pushes a stale freeze on
                                    // top of the fresh session.
                                    last_chunk_bytes = None;
                                }
                            }
                            continue;
                        }
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

    // C3 (#237): wrap the fetcher in an `Arc` so each producer (re)spawn gets
    // a fresh clone of the SAME fetcher without requiring `F: Clone`.
    let fetcher = Arc::new(fetcher);

    // Create bounded channel for pre-fetch buffer. `tx` is held HERE in the
    // endpoint_loop scope for the channel's whole lifetime (the producer tasks
    // get CLONES via the spawn closure): it keeps the channel OPEN even when
    // the active producer task exits (panic / finish), so the consumer never
    // sees a spurious `recv() == None` (which would tear the endpoint down).
    // Instead the consumer's keepalive / 8s-rescue arms cover the gap while
    // `endpoint_loop` respawns the producer. Only a real stop signal or
    // consumer exit ends the endpoint. (#237)
    let (tx, rx) = mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);

    // `is_fast` is Copy — read it before `ep_cfg` is moved into consumer_task.
    let producer_is_fast = ep_cfg.is_fast;

    // Spawn a producer task starting at `from_chunk_id`. The closure OWNS its
    // own clones of every shared handle (moved in below) and re-clones them on
    // each call, so it never borrows the outer `stop_rx` / `stats` /
    // `buffer_state` / … — leaving those free for the select-loop, which needs
    // `&mut stop_rx` for `stop_rx.changed()` and `&` everything else. Without
    // the dedicated owned clones the `Fn` closure's immutable borrow of
    // `stop_rx` would conflict with the loop's mutable `changed()` call.
    let p_fetcher = fetcher.clone();
    let p_tx = tx.clone();
    let p_stop = stop_rx.clone();
    let p_stats = stats.clone();
    let p_alias = alias.clone();
    let p_buffer_state = buffer_state.clone();
    let p_audit_ring = audit_ring.clone();
    let spawn_producer = move |from_chunk_id: i64| {
        tokio::spawn(crate::endpoint_producer::producer_task(
            p_fetcher.clone(),
            p_tx.clone(),
            from_chunk_id,
            delivery_delay_ms,
            producer_is_fast,
            p_stop.clone(),
            p_stats.clone(),
            p_alias.clone(),
            p_buffer_state.clone(),
            p_audit_ring.clone(),
        ))
    };
    let mut producer = spawn_producer(start_chunk_id);

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
        buffer_state.clone(),
        audit_ring.clone(),
    ));

    // Wait for either task to finish or stop signal.
    // Both producer and consumer already listen for stop_rx internally,
    // but we also watch here for cleanup coordination.
    tokio::pin!(consumer);

    // C3 (#237) producer-respawn budget. The accounting, audit emit, and
    // resume-chunk computation live in `crate::endpoint_respawn` (so this file
    // stays under the 1000-line CI cap); the consumer-drain timeout and the
    // backoff+respawn that touch the pinned `consumer` future and the
    // `spawn_producer` closure stay inline below.
    let mut respawns: u32 = 0;
    let mut last_respawn_at = tokio::time::Instant::now();

    loop {
        tokio::select! {
            result = &mut producer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Producer panicked: {e}");
                }
                // Stop requested? Then this is an ordinary shutdown — drain the
                // consumer and break (unchanged teardown path).
                if *stop_rx.borrow() {
                    tracing::info!(alias = %alias, "Producer finished on stop; waiting for consumer to drain");
                    let _ = tokio::time::timeout(
                        std::time::Duration::from_secs(30),
                        &mut consumer,
                    ).await;
                    break;
                }

                // Consumer already gone? Nothing to keep alive — break.
                if consumer.is_finished() {
                    tracing::info!(alias = %alias, "Producer finished and consumer already gone; tearing down");
                    break;
                }

                // C3 (#237): producer exited while the consumer is still alive
                // and no stop was signalled. The helper signals the producer
                // stall, applies the respawn budget, computes the resume chunk
                // and emits the audit row; we act on its decision here (the
                // parts that touch `consumer` / `spawn_producer` can't move).
                let decision = crate::endpoint_respawn::on_producer_finished(
                    &alias,
                    start_chunk_id,
                    &stats,
                    &buffer_state,
                    &audit_ring,
                    &mut respawns,
                    &mut last_respawn_at,
                )
                .await;
                let resume_from = match decision {
                    crate::endpoint_respawn::ProducerFinishedDecision::TearDownBudgetExhausted => {
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_secs(30),
                            &mut consumer,
                        ).await;
                        break;
                    }
                    crate::endpoint_respawn::ProducerFinishedDecision::Respawn { resume_from } => {
                        resume_from
                    }
                };
                // Backoff, but stay responsive to stop.
                tokio::select! {
                    _ = tokio::time::sleep(crate::endpoint_respawn::PRODUCER_RESPAWN_BACKOFF) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() {
                            tracing::info!(alias = %alias, "Stop during respawn backoff; tearing down");
                            consumer.abort();
                            break;
                        }
                    }
                }
                producer = spawn_producer(resume_from);
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

    // `tx` (the endpoint_loop-scoped keepalive sender) and the spawn closure's
    // `p_tx` drop here as the function returns, closing the channel once the
    // active producer is gone — lets any lingering consumer drain on teardown.
    drop(tx);
    tracing::info!(alias = %alias, "Endpoint pipeline stopped");
}

#[cfg(test)]
#[path = "endpoint_task_test_root.rs"]
mod test_root;
