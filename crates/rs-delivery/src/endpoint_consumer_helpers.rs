//! Consumer-task helpers extracted from `endpoint_task.rs` to keep that
//! file under the 1000-line file-size gate. Included via `#[path]` as
//! `mod consumer_helpers` inside `endpoint_task.rs`.

use std::sync::Arc;

use rs_rtmp_push::{PushError, backoff_floor_ms, is_exponential};
use tokio::sync::watch;

use super::{
    EndpointRestartState, FfmpegRestartRecord, FlvStreamNormalizer, OutputProcess,
    RESTART_HISTORY_CAP, RtmpPushAuditRecord, Stats, WRITE_TIMEOUT_SECS,
};
use crate::audit_ring::AuditRing;
use crate::{endpoint_audit, ffmpeg_reason};

/// Record the consumer-measured starvation gap into shared `BufferState` so
/// the producer's adaptive read-delay controller grows by it (trickle-grow
/// fix). `fetch_max` keeps the largest gap seen since the producer last
/// consumed it. Returns the elapsed gap so the caller can reuse it for the
/// keepalive audit without re-reading the clock. Call ONLY on the
/// chunk-resume path — never the stop path (the endpoint is shutting down).
pub(super) fn record_starvation_gap(
    buffer_state: &Arc<crate::buffer_state::BufferState>,
    started: tokio::time::Instant,
) -> tokio::time::Duration {
    let elapsed = started.elapsed();
    buffer_state.starvation_gap_ms.fetch_max(
        elapsed.as_millis() as u64,
        std::sync::atomic::Ordering::Relaxed,
    );
    elapsed
}

/// Return value from `handle_rust_push` telling the consumer loop whether
/// to continue normally or break the loop.
pub(super) enum RustPushAction {
    Continue,
    Break,
}

/// Minimal interface `handle_rust_push` needs from a pusher. Hoisted to
/// `crate::pushable` (#239) so the rescue push loop (`rust_rescue_push`,
/// outside the `endpoint_task` tree) can share it and accept a recording
/// mock. Re-exported here so the consumer path and its tests keep reaching
/// it via `endpoint_task::consumer_helpers::Pushable`.
///
/// **Module path:** `endpoint_task::consumer_helpers::Pushable`. Tests
/// reach it via `super::super::super::consumer_helpers::Pushable` from
/// inside `endpoint_task_rust_push_tests::close_on_error`.
pub(crate) use crate::pushable::Pushable;

/// Handle one Rust RTMP pusher write call (success or error path).
/// Extracted from `consumer_task` to keep that function under 1000 lines.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_rust_push(
    pusher: &mut impl Pushable,
    data: &[u8],
    chunk_id: i64,
    chunk_duration_ms: i64,
    alias: &str,
    service_type: &str,
    consecutive_push_errors: &mut u32,
    consecutive_write_failures: &mut u32,
    stats: &Stats,
    audit_ring: &Option<Arc<AuditRing>>,
    telemetry: &mut crate::rtmp_push_telemetry::RtmpPushTelemetry,
    stop_rx: &mut watch::Receiver<bool>,
    flv_normalizer: &mut FlvStreamNormalizer,
) -> RustPushAction {
    // chunk_duration_ms is no longer needed by push_flv_bytes (per-track
    // output_ts math is fully timestamp-driven from inside the FLV
    // payload — see PusherState::audio_origin_xiu_ts). Kept on the
    // consumer-helper signature for stats reporting (`s.duration_processed_ms`).
    //
    // Phase 2 probe (#177/#178): log push_flv_bytes start so we can
    // correlate stalls across endpoints (shared-supply hypothesis) and
    // detect whether multiple endpoints enter push at the SAME instant.
    let push_start = std::time::Instant::now();
    tracing::info!(
        alias = %alias,
        chunk_id,
        bytes = data.len(),
        "rtmp_push: ENTER push_flv_bytes"
    );
    let push_result = tokio::time::timeout(
        std::time::Duration::from_secs(WRITE_TIMEOUT_SECS),
        pusher.push_flv_bytes(data),
    )
    .await;
    let push_elapsed_ms = push_start.elapsed().as_millis() as u64;
    if push_elapsed_ms >= 2500 {
        tracing::warn!(
            alias = %alias,
            chunk_id,
            push_elapsed_ms,
            "rtmp_push: SLOW push_flv_bytes (>=2.5s) -- chunk supply or TCP backpressure"
        );
    }

    match push_result {
        Ok(Ok(())) => {
            *consecutive_push_errors = 0;
            *consecutive_write_failures = 0;
            telemetry.note_send("flv_bytes", data.len() as u64);
            telemetry.note_chunk_pushed();
            let mut s = stats.lock().await;
            s.bytes_processed_total += data.len() as u64;
            s.duration_processed_ms += chunk_duration_ms.max(0) as u64;
            s.current_chunk_id = chunk_id;
            s.chunks_processed += 1;
            s.reconnect_count = pusher.reconnect_count();
            // Clear sticky error markers: prior timeout / push-error states
            // shouldn't keep showing on the dashboard once writes resume.
            s.stall_reason = None;
            s.last_error = None;
            RustPushAction::Continue
        }
        Ok(Err(push_err)) => {
            *consecutive_push_errors += 1;
            let error_display = push_err.to_string();
            tracing::warn!(
                alias = %alias,
                chunk_id,
                consecutive = *consecutive_push_errors,
                "Consumer: Rust pusher error: {error_display} -- force-closing session"
            );
            let floor = backoff_floor_ms(&push_err);
            let Some(floor_ms) = floor else {
                // LocalCancel is the only None-floor variant. Returning
                // Break lets the consumer task exit; we do NOT call
                // pusher.close() here because close happens via Drop on
                // the consumer's stack unwind. Keeping this short-circuit
                // ABOVE the close() below ensures we don't double-close
                // on shutdown.
                tracing::info!(alias = %alias, "Consumer: Rust pusher cancelled; stopping");
                return RustPushAction::Break;
            };
            // CRITICAL: any push error means the session is in an unknown
            // state (broken socket, half-closed peer, poisoned by read loop).
            // Without close() the next push_flv_bytes would re-use the same
            // wedged session and fail identically forever -- exactly the
            // 2026-05-03 FB-NewLevel/FB-Zbynek freeze where last_error =
            // "I/O error: none return" but ffmpeg_restart_count stayed 0
            // and chunks_processed froze. Close drops the connection so the
            // next call lazily reconnects.
            pusher.close().await;
            let backoff_ms = if is_exponential(&push_err) {
                let factor = 1u64 << (consecutive_push_errors.saturating_sub(1).min(5));
                floor_ms.saturating_mul(factor).min(300_000)
            } else {
                floor_ms
            };
            let timestamp_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let reconnect_count = pusher.reconnect_count();
            // Phase 2 (#177): plumb actual chunks_buffered_in_pipeline + close-buffer bytes from rs-rtmp-push.
            endpoint_audit::emit_rtmp_push_died_detailed(
                audit_ring,
                alias,
                &error_display,
                backoff_ms,
                reconnect_count,
                telemetry,
                &[],
                0,
            );
            // A handshake failure is a distinct, often-transient connect-time
            // fault (TCP/RTMP handshake) vs the generic "push died" row. Emit
            // an additional RtmpHandshakeFailed row so the connect-failure
            // dashboard can isolate it. The EndpointRtmpPushDied row above is
            // kept — the two feed different operator views.
            if matches!(push_err, PushError::HandshakeFailed(_)) {
                if let Some(ring) = audit_ring {
                    ring.push_parts(crate::audit_ring::RingRowParts {
                        severity: rs_core::audit::Severity::Warn,
                        source: rs_core::audit::Source::Vps,
                        endpoint: Some(alias.to_string()),
                        action: rs_core::audit::Action::RtmpHandshakeFailed,
                        detail: serde_json::json!({
                            "error": error_display.clone(),
                            "backend": service_type,
                        }),
                    });
                }
            }
            *telemetry = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();
            let record = RtmpPushAuditRecord {
                timestamp_ms,
                chunk_id,
                reconnect_count,
                error_display: error_display.clone(),
                backoff_ms,
            };
            let mut s = stats.lock().await;
            s.reconnect_count = reconnect_count;
            s.last_error = Some(error_display.clone());
            // Match the Timeout arm: surface the freeze on the dashboard.
            // The success path clears stall_reason once writes resume.
            s.stall_reason = Some(error_display);
            if s.rtmp_push_history.len() >= RESTART_HISTORY_CAP {
                s.rtmp_push_history.pop_front();
            }
            s.rtmp_push_history.push_back(record);
            drop(s);
            *flv_normalizer = FlvStreamNormalizer::new();
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() { return RustPushAction::Break; }
                }
            }
            RustPushAction::Continue
        }
        Err(_timeout) => {
            *consecutive_push_errors += 1;
            tracing::error!(
                alias = %alias,
                chunk_id,
                consecutive = *consecutive_push_errors,
                "Consumer: Rust pusher write timed out -- force-closing session"
            );

            // CRITICAL: Force-close the wedged pusher session. Without this,
            // pusher.session stays alive but unresponsive — every subsequent
            // push_flv_bytes call hits the same blocked write and times out
            // again. Closing drops the TCP/TLS connection and clears
            // self.session, so the next push_flv_bytes triggers lazy
            // reconnect (issue #157).
            pusher.close().await;
            let reconnect_count = pusher.reconnect_count();

            // Audit: emit endpoint_rtmp_push_died on EVERY timeout so the
            // operator sees the silent stall instead of guessing from
            // stall_reason on the dashboard. Backoff matches the fixed
            // 30 s sleep below.
            let backoff_ms: u64 = 30_000;
            // Phase 2 (#177): plumb actual chunks_buffered_in_pipeline + close-buffer bytes from rs-rtmp-push.
            endpoint_audit::emit_rtmp_push_died_detailed(
                audit_ring,
                alias,
                "rtmp_push_timeout",
                backoff_ms,
                reconnect_count,
                telemetry,
                &[],
                0,
            );
            *telemetry = crate::rtmp_push_telemetry::RtmpPushTelemetry::new();
            let timestamp_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            let record = RtmpPushAuditRecord {
                timestamp_ms,
                chunk_id,
                reconnect_count,
                error_display: "rtmp_push_timeout".to_string(),
                backoff_ms,
            };

            let mut s = stats.lock().await;
            s.reconnect_count = reconnect_count;
            s.last_error = Some("rtmp_push_timeout".to_string());
            s.stall_reason = Some("rtmp_push_timeout".to_string());
            if s.rtmp_push_history.len() >= RESTART_HISTORY_CAP {
                s.rtmp_push_history.pop_front();
            }
            s.rtmp_push_history.push_back(record);
            drop(s);
            *flv_normalizer = FlvStreamNormalizer::new();
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)) => {}
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() { return RustPushAction::Break; }
                }
            }
            RustPushAction::Continue
        }
    }
}

/// Return from `handle_ffmpeg_death`: what the consumer loop should do next.
pub(super) enum FfmpegDeathAction {
    /// Continue to the spawn-new-process step.
    Respawn,
    /// ffmpeg was intentionally killed; break the consumer loop.
    Break,
}

/// Handle ffmpeg process death inside the consumer loop:
/// classify stderr, emit audit, update stats, compute backoff, sleep.
/// Extracted from `consumer_task` to keep that function under 1000 lines.
#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_ffmpeg_death(
    proc: &mut Option<Box<dyn OutputProcess>>,
    proc_spawned_at: Option<tokio::time::Instant>,
    restart_state: &mut EndpointRestartState,
    service_type_str: &str,
    alias: &str,
    stats: &Stats,
    audit_ring: &Option<Arc<AuditRing>>,
    stop_rx: &mut watch::Receiver<bool>,
    flv_normalizer: &mut FlvStreamNormalizer,
) -> FfmpegDeathAction {
    const LIFETIME_RESET_SECS: u64 = 60;
    let lifetime_secs = proc_spawned_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
    if lifetime_secs >= LIFETIME_RESET_SECS {
        *restart_state = EndpointRestartState::new();
    }
    let stderr_tail = proc.as_mut().and_then(|p| p.last_stderr_line());
    let class = ffmpeg_reason::classify(service_type_str, stderr_tail.as_deref().unwrap_or(""));
    *restart_state = restart_state.advance(class);
    let floor = ffmpeg_reason::reconnect_floor(
        class,
        restart_state.consecutive_same_class.saturating_sub(1),
    );
    let is_killed = floor.is_none();
    let reason_str = serde_json::to_string(&class)
        .unwrap_or_default()
        .trim_matches('"')
        .to_string();
    let backoff_secs = floor.map(|d| d.as_secs()).unwrap_or(0);
    let current_chunk_id_for_record = {
        let s = stats.lock().await;
        s.current_chunk_id
    };
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    endpoint_audit::emit_ffmpeg_died(
        audit_ring,
        alias,
        lifetime_secs,
        &reason_str,
        stderr_tail.as_deref(),
        backoff_secs,
        restart_state.consecutive_same_class,
    );
    let record = FfmpegRestartRecord {
        timestamp_ms,
        chunk_id: current_chunk_id_for_record,
        lifetime_secs,
        reason: reason_str.clone(),
        stderr_tail: stderr_tail.clone(),
        backoff_secs,
    };
    {
        let mut s = stats.lock().await;
        s.ffmpeg_restart_count += 1;
        s.ffmpeg_last_stderr = stderr_tail;
        if s.restart_history.len() >= RESTART_HISTORY_CAP {
            s.restart_history.pop_front();
        }
        s.restart_history.push_back(record);
    }
    if is_killed {
        tracing::info!(
            alias = %alias,
            reason = %reason_str,
            "Consumer: ffmpeg was intentionally killed; not restarting"
        );
        return FfmpegDeathAction::Break;
    }
    tracing::warn!(
        alias = %alias,
        lifetime_secs,
        consecutive_same_class = restart_state.consecutive_same_class,
        reason = %reason_str,
        backoff_secs,
        "Consumer: ffmpeg died, backing off before restart"
    );
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
        _ = stop_rx.changed() => {
            if *stop_rx.borrow() { return FfmpegDeathAction::Break; }
        }
    }
    *flv_normalizer = FlvStreamNormalizer::new();
    FfmpegDeathAction::Respawn
}
