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

/// #296 buffered-endpoint slow-refill throttle. Called after a successful
/// chunk delivery on the NON-fast path. When the producer has published a
/// below-target cushion deficit (`BufferState::refill_deficit_secs > 0`), add
/// the small per-chunk sleep from `refill::refill_throttle_ms` so the endpoint
/// delivers marginally slower than realtime (0.98x) and the cushion rebuilds.
///
/// Gating (all must hold to throttle):
/// - `!is_fast` — fast endpoints use the #294 read-delay ratchet instead.
/// - `deficit > 0` — cleared by the producer whenever it stalls (outage), so
///   the throttle never fires while there is no source to rebuild from.
/// - delivery mode is not rescue/warmup/recovering — those own their own
///   pacing; refill must not interact with the rescue refill state machine.
///
/// Also surfaces the state to the operator dashboard by setting
/// `delivery_mode = "refilling"` while active (and restoring it to "normal"
/// when the cushion is back at target), reusing the existing VPS→host→UI
/// `delivery_mode` pipeline — no new cross-crate plumbing. The sleep is
/// interruptible by the stop signal and capped far below the rescue-stall /
/// write-timeout thresholds (`refill::REFILL_MAX_THROTTLE_MS`).
pub(super) async fn maybe_refill_throttle(
    is_fast: bool,
    buffer_state: &Arc<crate::buffer_state::BufferState>,
    chunk_duration_ms: i64,
    stats: &Stats,
    stop_rx: &mut watch::Receiver<bool>,
) {
    if is_fast {
        return;
    }
    let deficit = buffer_state
        .refill_deficit_secs
        .load(std::sync::atomic::Ordering::Relaxed);

    // Update the dashboard-visible mode under the stats lock, and decide
    // whether to throttle. Never stomp an active rescue-family mode.
    let should_throttle = {
        let mut s = stats.lock().await;
        let in_rescue_family =
            matches!(s.delivery_mode.as_str(), "rescue" | "warmup" | "recovering");
        if deficit > 0 && !in_rescue_family {
            if s.delivery_mode != "refilling" {
                s.delivery_mode = "refilling".to_string();
            }
            true
        } else {
            // deficit cleared (or a rescue-family mode is active): drop the
            // "refilling" badge back to "normal" without touching rescue modes.
            if s.delivery_mode == "refilling" {
                s.delivery_mode = "normal".to_string();
            }
            false
        }
    };
    if !should_throttle {
        return;
    }

    let throttle_ms = crate::refill::refill_throttle_ms(deficit, chunk_duration_ms);
    if throttle_ms == 0 {
        return;
    }
    tokio::select! {
        _ = tokio::time::sleep(std::time::Duration::from_millis(throttle_ms)) => {}
        _ = stop_rx.changed() => {}
    }
}

/// Return value from `handle_rust_push` telling the consumer loop whether
/// to continue normally or break the loop.
pub(super) enum RustPushAction {
    Continue,
    Break,
}

/// #236: consecutive zero-byte-since-connect deaths before an endpoint is
/// classified "dead target" (the bound remote session/broadcast is gone,
/// not merely a transient outage) -- e.g. an expired FB persistent-key
/// live_video, which FB closes at/just after the RTMP handshake before any
/// FLV bytes go out. At the `RemoteClosed` 3s floor this is ~15s.
const DEAD_TARGET_ZERO_BYTE_THRESHOLD: u32 = 5;

/// Hard backoff floor applied once an endpoint is classified dead-target,
/// overriding whatever the underlying `PushError`'s own floor/ladder would
/// otherwise pick. Stops the sub-second-to-3s reconnect hammer (#236: 3548
/// reconnects, ~3s apart, in one live incident) while still reconnecting
/// fast enough to recover the moment the operator recreates the broadcast.
const DEAD_TARGET_BACKOFF_MS: u64 = 30_000;

/// Build the operator-facing dead-target message for `service_type`,
/// prefixed with `rs_core::endpoint_lifecycle::DEAD_TARGET_STALL_PREFIX` --
/// the SHARED marker (defined in `rs-core`, not duplicated here) that
/// `EndpointLifecycle::compute` matches on `stall_reason` to force
/// `Attention` (red) even while the endpoint keeps reconnecting forever
/// (`alive` never goes false for this failure class -- the consumer task
/// never exits, see `EndpointHandle::is_alive`). Reusing the existing
/// `stall_reason` string field (rather than adding a new boolean field to
/// `DeliveryEndpointMetrics`/`LifecycleInput`) means this reaches the
/// dashboard's lifecycle computation through the pipeline that already
/// threads `stall_reason` end-to-end, with no change needed to the VPS
/// `/api/status` serialization or the host's status-poll mapping.
/// FB-specific wording names the concrete remedy (persistent key stays
/// put, only the broadcast/live_video needs recreating); every other
/// service type gets a generic dead-target message naming the observed
/// signal so an operator can still act on it.
/// `raw_error` (the underlying `PushError`'s own `Display` text, e.g.
/// `"upstream closed connection mid-stream: unexpected end of file"`) is
/// appended so the operator does not lose the concrete signal once the
/// dashboard switches to the dead-target remedy text (review finding: the
/// remedy text alone discarded it).
fn dead_target_message(service_type: &str, raw_error: &str) -> String {
    let prefix = rs_core::endpoint_lifecycle::DEAD_TARGET_STALL_PREFIX;
    if service_type.eq_ignore_ascii_case("FB") {
        format!(
            "{prefix}FB broadcast expired/killed -- recreate the live broadcast on Facebook (stream key stays the same) (last error: {raw_error})"
        )
    } else {
        format!(
            "{prefix}{service_type} endpoint rejected {DEAD_TARGET_ZERO_BYTE_THRESHOLD} consecutive connects with 0 bytes sent -- the remote target/session looks dead; check it on the provider side (last error: {raw_error})"
        )
    }
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
    consecutive_zero_byte_deaths: &mut u32,
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
            // #236: a connect that sends real bytes is never a dead target,
            // even if it later dies mid-stream (that's the unchanged
            // transient-outage path in the Err arm below).
            *consecutive_zero_byte_deaths = 0;
            telemetry.note_send("flv_bytes", data.len() as u64);
            telemetry.note_chunk_pushed();
            let mut s = stats.lock().await;
            s.bytes_processed_total += data.len() as u64;
            s.duration_processed_ms += chunk_duration_ms.max(0) as u64;
            s.current_chunk_id = chunk_id;
            s.chunks_processed += 1;
            s.reconnect_count = pusher.reconnect_count();
            // #257: surface the live content-PTS A/V skew so the dashboard can
            // alarm on a desync and the #258 E2E gate can assert it stays ~0.
            // Updated on the SUCCESS path only — last-success semantics; on an
            // error chunk the error fields below dominate the dashboard and
            // this keeps its last-known-good value.
            s.av_skew_ms = pusher.av_skew_ms();
            // #284 disambiguation telemetry: timestamp of the last
            // successful push, surfaced as last_push_ok_age_ms on
            // /api/status so a live stall self-classifies (producer starved
            // vs pusher stalled).
            s.last_push_ok_unix_ms = Some(crate::endpoint_stats::unix_ms_now());
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
            // #236: read bytes_sent BEFORE the telemetry reset below. Only
            // `RemoteClosed` with 0 bytes sent this connect is the
            // dead-target signal (the confirmed FB signature: "upstream
            // closed connection mid-stream" with nothing ever pushed).
            // REVIEW FINDING (adversarial pass, fixed): gating on
            // bytes_sent()==0 alone was wrong -- EVERY connect-time
            // failure has 0 bytes sent at that point (push_flv_bytes was
            // never reached), including HandshakeFailed (DNS/TCP/TLS
            // failure -- wrong remedy: "check network", not "recreate the
            // FB broadcast") and PublishRejected BadName (wrong stream
            // key -- already has its own actionable last_error text via
            // `last_error_is_actionable`'s "badname"/"rejected" match,
            // which the old code would eventually OVERWRITE with the
            // dead-target message after 5 consecutive rejects). Scoping to
            // `RemoteClosed` alone matches the ONE failure mode this
            // classifier is meant to detect; every other connect-time
            // error variant resets the counter (its own distinct signal
            // deserves its own distinct handling, not folding into this
            // one).
            let is_zero_byte_remote_close =
                matches!(push_err, PushError::RemoteClosed(_)) && telemetry.bytes_sent() == 0;
            if is_zero_byte_remote_close {
                *consecutive_zero_byte_deaths = consecutive_zero_byte_deaths.saturating_add(1);
            } else {
                *consecutive_zero_byte_deaths = 0;
            }
            // RED (#236): classification not implemented yet -- the counter
            // above already tracks consecutive zero-byte RemoteClosed
            // deaths, but nothing acts on it yet, so the endpoint keeps
            // death-looping on the unmodified backoff/last_error path
            // below exactly like it did before this ticket. This is the
            // failing-test commit; the next commit flips these two lines
            // to the real threshold check.
            let is_dead_target = false;
            let just_became_dead_target = false;
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
            // #236: once classified dead-target, stop the fast (down to 3s)
            // reconnect hammer regardless of the underlying error's own
            // floor -- the remote session is gone, not merely rotating.
            let backoff_ms = if is_dead_target {
                backoff_ms.max(DEAD_TARGET_BACKOFF_MS)
            } else {
                backoff_ms
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
            let dead_target_msg = if is_dead_target {
                Some(dead_target_message(service_type, &error_display))
            } else {
                None
            };
            // #236: emit ONCE, at the threshold transition -- every retry
            // afterwards would just spam the audit log while the endpoint
            // stays classified dead-target.
            if just_became_dead_target {
                if let Some(msg) = &dead_target_msg {
                    endpoint_audit::emit_endpoint_dead_target(
                        audit_ring,
                        alias,
                        msg,
                        *consecutive_zero_byte_deaths,
                        backoff_ms,
                    );
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
            let dashboard_message = dead_target_msg.unwrap_or(error_display);
            s.last_error = Some(dashboard_message.clone());
            // Match the Timeout arm: surface the freeze on the dashboard.
            // The success path clears stall_reason once writes resume.
            s.stall_reason = Some(dashboard_message);
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
            // #236: a write TIMEOUT means the peer held the TCP connection
            // open for the full WRITE_TIMEOUT_SECS instead of closing it --
            // the opposite of the dead-target signature (FB closes the
            // connection immediately, it does not let the write hang).
            // Reset so a timeout sandwiched between RemoteClosed deaths
            // never contributes to (or silently preserves) that streak.
            *consecutive_zero_byte_deaths = 0;
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
