//! Rescue mode: plays a looped video with countdown overlay when the
//! delivery buffer is empty (warmup or outage recovery).
use std::borrow::Cow;

use crate::rescue_default::DEFAULT_RESCUE_FLV;

/// Fixed buffer refill target before resuming normal delivery (seconds).
pub const RESCUE_REFILL_TARGET_SECS: u64 = 120;

/// Seconds of channel starvation before entering rescue mode. The consumer
/// pulls chunks from a 10-slot channel; when starved for this long AND
/// the producer has signalled stalled (no chunks on S3), rescue activates.
/// Lower values mean rescue kicks in faster after OBS stops — at the cost
/// of sensitivity to transient hiccups (normally producer_active will be
/// true during those, preventing rescue from triggering).
pub const RESCUE_STALL_THRESHOLD_SECS: u64 = 8;

/// Delivery mode state machine.
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryMode {
    /// Normal chunk delivery.
    Normal,
    /// Playing rescue video (warmup or buffer empty).
    Rescue { reason: RescueReason },
}

#[derive(Debug, Clone, PartialEq)]
pub enum RescueReason {
    /// Initial buffer fill — stream hasn't started yet.
    Warmup,
    /// Buffer drained during an outage.
    BufferEmpty,
}

/// Format the countdown text for the rescue video overlay.
pub fn format_countdown_text(mode: &DeliveryMode, eta_secs: u64) -> String {
    match mode {
        DeliveryMode::Normal => String::new(),
        DeliveryMode::Rescue { reason } => {
            let prefix = match reason {
                RescueReason::Warmup => "Stream starting",
                RescueReason::BufferEmpty => "Stream recovering",
            };
            if eta_secs == 0 {
                format!("{prefix} soon")
            } else if eta_secs >= 60 {
                let mins = eta_secs / 60;
                let secs = eta_secs % 60;
                format!("{prefix} ~ {mins}m {secs}s")
            } else {
                format!("{prefix} ~ {eta_secs}s")
            }
        }
    }
}

/// Path to the countdown text file for a given endpoint alias.
///
/// Uses the platform temp dir so tests work on both Linux (VPS) and
/// Windows (stream.lan CI). The rescue ffmpeg drawtext filter reads the
/// file path literally, so whatever path we return here must be a path
/// that ffmpeg can open.
pub fn countdown_file_path(alias: &str) -> String {
    let safe_alias = alias.replace([' ', '/', '\\'], "_");
    std::env::temp_dir()
        .join(format!("rescue_{safe_alias}.txt"))
        .to_string_lossy()
        .into_owned()
}

/// Write the countdown text to the file. Called periodically by the producer.
pub fn write_countdown_file(alias: &str, text: &str) {
    let path = countdown_file_path(alias);
    if let Err(e) = std::fs::write(&path, text) {
        tracing::warn!(alias, path, "Failed to write countdown file: {e}");
    }
}

/// Clean up the countdown file when rescue mode ends.
pub fn cleanup_countdown_file(alias: &str) {
    let path = countdown_file_path(alias);
    let _ = std::fs::remove_file(&path);
}

/// Run the rescue push loop: resolve FLV bytes (operator URL or embedded
/// default) and push via `rust_rescue_push` until the buffer is refilled
/// or a stop signal arrives.
///
/// Task 6 (R1 GREEN): the body no longer requires a configured rescue
/// URL. `resolve_rescue_bytes(None, ...)` substitutes the embedded
/// `DEFAULT_RESCUE_FLV` blob so rescue ALWAYS has something to push —
/// closing the 2026-05-30 stream.lan crash gap where all 5 production
/// templates had `rescue_video_url = NULL` and the cache-drain branch
/// went silent. The pure-rust pusher replaces the legacy ffmpeg spawn.
///
/// Returns `true` if a stop signal was received (caller should exit),
/// `false` if the buffer was refilled and normal delivery can resume.
#[allow(clippy::too_many_arguments)]
pub async fn run_rescue_loop(
    alias: &str,
    rescue_url: Option<&str>,
    service_type: rs_ffmpeg::ServiceType,
    stream_key: &str,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
) -> bool {
    // Resolve the FLV bytes to push. Falls back to DEFAULT_RESCUE_FLV
    // when URL is None / empty / non-FLV / fetch-failed (audit events
    // emitted by resolve_rescue_bytes for the rejection paths).
    let bytes_cow = resolve_rescue_bytes(rescue_url, audit_ring, alias).await;
    let flv_bytes = std::sync::Arc::new(bytes_cow.into_owned());

    // Seed countdown overlay at the start of the refill window — the
    // pusher pacing-loop updates it on each tick, but the file must
    // exist by the time the first push completes so the file-based
    // status surface stays consistent.
    let initial_text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        RESCUE_REFILL_TARGET_SECS,
    );
    write_countdown_file(alias, &initial_text);

    let stopped = crate::rust_rescue_push::rust_rescue_push(
        alias,
        service_type,
        stream_key,
        flv_bytes,
        buffer_state.clone(),
        stats.clone(),
        stop_rx,
    )
    .await;

    cleanup_countdown_file(alias);
    stopped
}

/// Run the warmup phase: push rescue (default or operator-configured FLV)
/// via the pure-rust pusher, then probe S3 for chunks until the target
/// delay is accumulated. Returns `true` if a stop signal was received.
///
/// R3 GREEN (Task 7, 2026-05-31): non-fast endpoints ALWAYS push rescue
/// during warmup, regardless of whether the operator configured a custom
/// URL. `resolve_rescue_bytes(None, ...)` substitutes the embedded
/// `DEFAULT_RESCUE_FLV` blob so viewers never see a blank screen during
/// the initial cache fill (~120s). Fast endpoints still skip rescue per
/// the low-latency design trade-off.
///
/// The pusher runs as a background `tokio::task` so it streams in
/// parallel with the chunk-probe loop. When the probe loop exits (buffer
/// target met, or stop signal), the handle is aborted — terminating the
/// rescue stream cleanly. This closes the 2026-05-30 stream.lan blank-
/// warmup gap (gap #3 of 3 in the design spec).
#[allow(clippy::too_many_arguments)]
pub async fn run_warmup_loop<F: crate::endpoint_task::ChunkFetcher>(
    fetcher: &F,
    alias: &str,
    ep_cfg: &crate::api::EndpointConfig,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    rescue_video_url: Option<&str>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    audit_ring: Option<&std::sync::Arc<crate::audit_ring::AuditRing>>,
) -> bool {
    // R3 GREEN: always push rescue during warmup for non-fast endpoints.
    // The outer `if let Some(rescue_url) = ...` guard from the pre-fix
    // body is GONE — `resolve_rescue_bytes(None, ...)` falls back to
    // DEFAULT_RESCUE_FLV so blank-warmup is impossible. Fast endpoints
    // continue to skip rescue per design (low-latency trade-off).
    let warmup_handle: Option<tokio::task::JoinHandle<bool>> = if !ep_cfg.is_fast {
        let svc_type: rs_ffmpeg::ServiceType = ep_cfg
            .service_type
            .parse()
            .unwrap_or(rs_ffmpeg::ServiceType::TestFile);

        // Resolve bytes BEFORE spawning so the audit_ring borrow stays
        // local to this function — the spawned task only owns the
        // resolved Arc<Vec<u8>>.
        let audit_ring_owned: Option<std::sync::Arc<crate::audit_ring::AuditRing>> =
            audit_ring.cloned();
        let bytes_cow = resolve_rescue_bytes(rescue_video_url, &audit_ring_owned, alias).await;
        let flv_bytes = std::sync::Arc::new(bytes_cow.into_owned());

        // Seed the countdown overlay + stats so the dashboard reflects
        // warmup state from the first frame. The pusher pacing loop
        // updates these on each tick.
        let initial_text = format_countdown_text(
            &DeliveryMode::Rescue {
                reason: RescueReason::Warmup,
            },
            delivery_delay_ms / 1000,
        );
        write_countdown_file(alias, &initial_text);
        {
            let mut s = stats.lock().await;
            s.delivery_mode = "warmup".to_string();
            s.rescue_eta_secs = Some(delivery_delay_ms / 1000);
        }

        // Construct a dummy BufferState with producer_active=false so
        // `rust_rescue_push`'s refill-detection exit condition never
        // fires during warmup. Warmup has its own exit logic — the
        // probe loop below decides when to stop, and we abort this
        // handle. The pusher here is purely a fire-and-forget "keep
        // pushing bytes until aborted" worker.
        let dummy_buffer_state = std::sync::Arc::new(crate::buffer_state::BufferState::new());
        dummy_buffer_state
            .producer_active
            .store(false, std::sync::atomic::Ordering::Relaxed);

        let alias_owned = alias.to_string();
        let stream_key_owned = ep_cfg.stream_key.clone();
        let stats_clone = stats.clone();
        let mut warmup_stop = stop_rx.clone();
        Some(tokio::spawn(async move {
            crate::rust_rescue_push::rust_rescue_push(
                &alias_owned,
                svc_type,
                &stream_key_owned,
                flv_bytes,
                dummy_buffer_state,
                stats_clone,
                &mut warmup_stop,
            )
            .await
        }))
    } else {
        None
    };

    // Warmup exits when the buffer is filled. During warmup, rescue
    // bytes are being pushed concurrently by the background task spawned
    // above and the countdown overlay shows time remaining until normal
    // delivery starts.
    //
    // If chunks already exist on S3 when the VPS boots (which they
    // usually do because OBS has been streaming during the ~60-90s VPS
    // boot), accum_ms grows fast through the existing chunks and
    // warmup exits quickly — viewers see real content ASAP. If cache
    // really is being built from zero, the rescue stream plays
    // throughout.
    let mut accum_ms: u64 = 0;
    let mut probe_id = start_chunk_id;
    tracing::info!(
        alias,
        delivery_delay_ms,
        "Warmup started — waiting for buffer target"
    );

    // Hardening (#146): if the same chunk_id returns Ok(None) for too
    // long, advance probe_id rather than spinning silently. Production
    // bug: when start_chunk_id is below S3 live-edge (chunks pruned),
    // the loop hung forever with no log output.
    const CONSECUTIVE_NONE_THRESHOLD: u32 = 30; // 30 × 2s sleep ≈ 60s
    let mut consecutive_none: u32 = 0;
    let mut stuck_chunk: i64 = probe_id;

    let stopped = loop {
        if *stop_rx.borrow() {
            break true;
        }
        match fetcher.chunk_duration_ms(probe_id).await {
            Ok(Some(dur_ms)) => {
                consecutive_none = 0;
                stuck_chunk = probe_id;
                accum_ms += dur_ms.max(0) as u64;
                probe_id += 1;

                // R3 GREEN: non-fast endpoints always have rescue pushing
                // in the background, so the countdown overlay + warmup
                // stats must reflect progress regardless of URL config.
                // Fast endpoints skip rescue entirely (per design) and
                // therefore skip the countdown/stats update too.
                if !ep_cfg.is_fast {
                    let remaining_ms = delivery_delay_ms.saturating_sub(accum_ms);
                    let eta_secs = remaining_ms.div_ceil(1000);

                    {
                        let mut s = stats.lock().await;
                        s.delivery_mode = "warmup".to_string();
                        s.rescue_eta_secs = Some(eta_secs);
                    }

                    let text = format_countdown_text(
                        &DeliveryMode::Rescue {
                            reason: RescueReason::Warmup,
                        },
                        eta_secs,
                    );
                    write_countdown_file(alias, &text);
                }

                if accum_ms >= delivery_delay_ms {
                    tracing::info!(
                        alias,
                        accum_ms,
                        probe_id,
                        "Warmup complete — buffer target met"
                    );
                    // Outage forensics: warmup-complete == the cache window
                    // first reached its delivery target. Pairs with the
                    // DiskCachePrefillStarted emitted at fetcher construction.
                    if let Some(ring) = audit_ring {
                        ring.push_parts(crate::audit_ring::RingRowParts {
                            severity: rs_core::audit::Severity::Info,
                            source: rs_core::audit::Source::Vps,
                            endpoint: Some(alias.to_string()),
                            action: rs_core::audit::Action::DiskCachePrefillReady,
                            detail: serde_json::json!({ "alias": alias }),
                        });
                    }
                    break false;
                }
            }
            Ok(None) => {
                if probe_id == stuck_chunk {
                    consecutive_none += 1;
                } else {
                    stuck_chunk = probe_id;
                    consecutive_none = 1;
                }
                if consecutive_none >= CONSECUTIVE_NONE_THRESHOLD {
                    // Exponential probe forward to find the live edge.
                    // Bounded: jump grows 1, 2, 4, ..., capped so the worst
                    // case is O(log n) probes for an n-chunk gap. Linear
                    // increment alone would take 60s × n on a large gap
                    // (e.g. 600 pruned chunks = 10 hours); exponential is
                    // ~10 probes for the same gap, each a single S3 HEAD.
                    //
                    // Overshoot is intentional: for a 600-chunk gap the
                    // probe lands at +1024 (the first power of two past
                    // the gap), skipping ~424 chunks of available history.
                    // Warmup only needs to find ANY live chunk to start
                    // filling the buffer; missing old history doesn't
                    // affect time-to-stream-start (still ~target_delay_ms
                    // wall time of fresh content needed).
                    //
                    // MAX_PROBE_JUMP = 4096 ≈ 2h 16m at 2s/chunk. Beyond
                    // that we degrade to `+= 1` (60s/chunk). 4th line of
                    // defense — the chunker fix (#146), DB fallback, and
                    // initial CONSECUTIVE_NONE_THRESHOLD all prevent this
                    // path in normal operation.
                    const MAX_PROBE_JUMP: i64 = 4096;
                    tracing::warn!(
                        alias,
                        stuck_chunk,
                        consecutive_none,
                        "Warmup stuck on missing chunk; probing forward for live edge"
                    );
                    let mut jump: i64 = 1;
                    let mut new_probe = probe_id + jump;
                    let mut found_live_edge = false;
                    loop {
                        match fetcher.chunk_duration_ms(new_probe).await {
                            Ok(Some(_)) => {
                                tracing::info!(
                                    alias,
                                    stuck_chunk,
                                    new_probe,
                                    jump,
                                    "Warmup found live edge; resuming"
                                );
                                probe_id = new_probe;
                                found_live_edge = true;
                                break;
                            }
                            Ok(None) => {
                                if jump >= MAX_PROBE_JUMP {
                                    break;
                                }
                                jump *= 2;
                                new_probe = probe_id + jump;
                            }
                            Err(e) => {
                                tracing::warn!(alias, new_probe, "Probe-forward fetch error: {e}");
                                break;
                            }
                        }
                    }
                    if !found_live_edge {
                        // Exponential probe gave up; fall back to +1 so we
                        // still make progress (caller's existing recovery).
                        probe_id += 1;
                    }
                    consecutive_none = 0;
                    stuck_chunk = probe_id;
                    continue;
                }
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break true; }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(alias, "Buffer fill fetch error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
    };

    // Tear down the warmup rescue pusher task and countdown file.
    // Aborting the JoinHandle drops the spawned task; the RtmpPusher
    // inside is dropped, which closes its session (kill_on_drop-equivalent
    // for pure-rust). No external ffmpeg process to reap.
    if let Some(handle) = warmup_handle {
        handle.abort();
        tracing::info!(alias, "Warmup rescue pusher stopped");
    }
    cleanup_countdown_file(alias);

    if !stopped {
        let mut s = stats.lock().await;
        s.delivery_mode = "normal".to_string();
        s.rescue_eta_secs = None;
    }

    stopped
}

/// Resolve the FLV bytes to push during rescue for this endpoint.
///
/// Returns `Cow::Borrowed(DEFAULT_RESCUE_FLV)` when:
///   * no operator URL configured (None / empty)
///   * URL is non-FLV (legacy MP4 / MOV / etc) — emits `RescueLegacyFormatRejected`
///   * S3 fetch fails — emits `RescueCustomFetchFailed`
///
/// Returns `Cow::Owned(<S3 bytes>)` when a custom `.flv` URL fetches
/// successfully.
///
/// Caller wraps the result in `Arc<Vec<u8>>` for cheap cloning across
/// rust_rescue_push loop iterations.
pub async fn resolve_rescue_bytes(
    rescue_video_url: Option<&str>,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    alias: &str,
) -> Cow<'static, [u8]> {
    let url = match rescue_video_url {
        Some(u) if !u.is_empty() => u,
        _ => return Cow::Borrowed(DEFAULT_RESCUE_FLV),
    };

    if !url.to_lowercase().ends_with(".flv") {
        tracing::warn!(alias, url, "Non-FLV rescue URL rejected; using default");
        crate::rescue_audit::emit_legacy_rejected(audit_ring, alias, url);
        return Cow::Borrowed(DEFAULT_RESCUE_FLV);
    }

    match fetch_flv_from_s3(url).await {
        Ok(bytes) => Cow::Owned(bytes),
        Err(e) => {
            tracing::warn!(alias, url, "Rescue FLV fetch failed: {e}; using default");
            crate::rescue_audit::emit_custom_fetch_failed(audit_ring, alias, url, &e.to_string());
            Cow::Borrowed(DEFAULT_RESCUE_FLV)
        }
    }
}

async fn fetch_flv_from_s3(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()).into());
    }
    Ok(resp.bytes().await?.to_vec())
}

#[cfg(test)]
#[path = "rescue_tests.rs"]
mod tests;

#[cfg(test)]
mod resolve_rescue_bytes_tests {
    use super::*;
    use crate::rescue_default::DEFAULT_RESCUE_FLV;

    #[tokio::test]
    async fn returns_default_when_url_none() {
        let result = resolve_rescue_bytes(None, &None, "test-alias").await;
        assert_eq!(result.as_ref(), DEFAULT_RESCUE_FLV);
    }

    #[tokio::test]
    async fn returns_default_when_url_empty() {
        let result = resolve_rescue_bytes(Some(""), &None, "test-alias").await;
        assert_eq!(result.as_ref(), DEFAULT_RESCUE_FLV);
    }

    #[tokio::test]
    async fn returns_default_when_url_not_flv() {
        // Legacy MP4 URL → reject, fallback. No audit ring so no panic on emit.
        let result = resolve_rescue_bytes(
            Some("https://example.com/rescue-videos/abc.mp4"),
            &None,
            "test-alias",
        )
        .await;
        assert_eq!(result.as_ref(), DEFAULT_RESCUE_FLV);
    }
}
