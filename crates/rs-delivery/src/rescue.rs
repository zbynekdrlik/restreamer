//! Rescue mode: plays pre-rendered Slovak rescue segments with a live
//! viewer-facing recovery countdown when the delivery buffer is empty
//! (warmup or outage recovery). See `rescue_segments` for the segment set
//! and the ETA-bucket selection (#259).

/// Fixed buffer refill target before resuming normal delivery (seconds).
pub const RESCUE_REFILL_TARGET_SECS: u64 = 120;

/// Seconds of channel starvation before entering rescue mode. The consumer
/// pulls chunks from a 10-slot channel; when starved for this long AND
/// the producer has signalled stalled (no chunks on S3), rescue activates.
/// Lower values mean rescue kicks in faster after OBS stops — at the cost
/// of sensitivity to transient hiccups (normally producer_active will be
/// true during those, preventing rescue from triggering).
pub const RESCUE_STALL_THRESHOLD_SECS: u64 = 8;

// #124: `keepalive_escalate_after` anchors the non-fast escalation to the last
// real chunk by subtracting FAST_KEEPALIVE_TRIGGER_SECS from this threshold. A
// future edit that inverted the two would collapse the anchor to 0 (escalate
// immediately) via saturating_sub — guard the ordering at compile time so that
// can never happen silently.
const _: () =
    assert!(RESCUE_STALL_THRESHOLD_SECS > crate::fast_keepalive::FAST_KEEPALIVE_TRIGGER_SECS);

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

/// Human-readable Slovak rescue status text for the dashboard + logs (#259).
///
/// This is the STATUS string (an exact-seconds countdown a viewer/operator
/// reads on the dashboard and that we log on rescue transitions), distinct
/// from the pre-rendered VIEWER segments in `rescue_segments` (which show a
/// coarser bucketed countdown because there is no runtime text renderer on
/// the VPS). Both derive from the same (reason, eta) rescue state.
///
/// Warmup → "Vysielanie sa spustí o ~…"; buffer-empty/recovery →
/// "Obnovujeme o ~…"; `Normal` → empty (no rescue active).
pub fn format_countdown_text(mode: &DeliveryMode, eta_secs: u64) -> String {
    match mode {
        DeliveryMode::Normal => String::new(),
        DeliveryMode::Rescue { reason } => {
            let prefix = match reason {
                RescueReason::Warmup => "Vysielanie sa spustí",
                RescueReason::BufferEmpty => "Obnovujeme",
            };
            if eta_secs == 0 {
                format!("{prefix} o chvíľu")
            } else if eta_secs >= 60 {
                let mins = eta_secs / 60;
                let secs = eta_secs % 60;
                format!("{prefix} o ~{mins}m {secs}s")
            } else {
                format!("{prefix} o ~{eta_secs}s")
            }
        }
    }
}

// #259: the temp-file countdown plumbing (`countdown_file_path` /
// `write_countdown_file` / `cleanup_countdown_file`) has been REMOVED. It
// wrote a text file that a since-deleted ffmpeg drawtext filter was meant to
// read; on the pure-Rust pusher path nothing ever read it, so the countdown
// was dead. The countdown is now genuinely viewer-visible via the pre-rendered
// segment set (`rescue_segments`), swapped by the pusher as the ETA changes.

/// Run the rescue push loop: resolve the rescue clip SOURCE (operator custom
/// FLV, or the embedded Slovak countdown segment set) and push via
/// `rust_rescue_push` until the buffer is refilled or a stop signal arrives.
///
/// Task 6 (R1 GREEN): the body no longer requires a configured rescue URL.
/// `resolve_rescue_source(None, ...)` returns the `Countdown` segment set so
/// rescue ALWAYS has something to push — closing the 2026-05-30 stream.lan
/// crash gap where all 5 production templates had `rescue_video_url = NULL`
/// and the cache-drain branch went silent. The pure-rust pusher replaces the
/// legacy ffmpeg spawn.
///
/// #259: for the `Countdown` source the pusher swaps the ETA-bucket segment
/// each iteration so viewers see a live "Obnovujeme o ~…" countdown.
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
    // Resolve the rescue clip source. Falls back to the Slovak countdown
    // segment set when URL is None / empty / non-FLV / fetch-failed (audit
    // events emitted by resolve_rescue_source for the rejection paths).
    let source = resolve_rescue_source(rescue_url, audit_ring, alias).await;

    // Log the human Slovak status at rescue entry (comprehensive-logging:
    // the rescue state must be reconstructable from logs alone).
    tracing::info!(
        alias,
        status = %format_countdown_text(
            &DeliveryMode::Rescue { reason: RescueReason::BufferEmpty },
            RESCUE_REFILL_TARGET_SECS,
        ),
        "Rescue: entering outage rescue"
    );

    crate::rust_rescue_push::rust_rescue_push(
        alias,
        service_type,
        stream_key,
        source,
        buffer_state.clone(),
        stats.clone(),
        stop_rx,
        crate::rust_rescue_push::RescuePushMode::Outage,
    )
    .await
}

/// Result of a cache-drain rescue cycle handled by `run_outage_rescue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutageRescueOutcome {
    /// Stop signal fired while in rescue — consumer should return.
    Stop,
    /// Rescue exited (refill complete or — in current scoped fix — never).
    /// Consumer should reset its FLV normalizer and resume normal delivery.
    Recovered,
}

/// Cache-drain outage rescue invoked from `consumer_task` when the buffer
/// is empty AND the producer has stalled.
///
/// Review-finding fixes baked in here:
///
/// * **#1 (duplicate-publish):** drops the existing `rust_pusher` (closing
///   its RTMP `Session`) BEFORE entering rescue, then reconstructs a fresh
///   pusher on the Recovered path so normal delivery resumes against a
///   fresh `Session`. Without the drop, two `RtmpPusher` instances would
///   race to publish on the same URL+stream_key — YouTube/FB rejects one
///   as "publish busy" and the stream breaks.
/// * **#5 (file-size cap):** extracting this from inline code in
///   `consumer_task` keeps `endpoint_task.rs` under the 1000-line cap.
///
/// The `RescueRecovered` audit row is still emitted here (unlike
/// `run_defensive_rescue`), because the cache-drain branch CAN recover —
/// the producer may revive and the consumer continues normal delivery.
#[allow(clippy::too_many_arguments)]
pub async fn run_outage_rescue(
    alias: &str,
    rescue_video_url: Option<&str>,
    service_type: rs_ffmpeg::ServiceType,
    stream_key: &str,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    last_delivered_chunk_id: i64,
    proc: &mut Option<Box<dyn crate::endpoint_task::OutputProcess>>,
    rust_pusher: &mut Option<rs_rtmp_push::RtmpPusher>,
    use_rust_pusher: bool,
) -> OutageRescueOutcome {
    let rescue_started = std::time::Instant::now();
    crate::rescue_audit::emit_activated(audit_ring, alias, last_delivered_chunk_id);

    // Review #1: kill the legacy ffmpeg child AND drop the existing
    // rust_pusher BEFORE entering rescue. The rescue loop constructs its
    // own `RtmpPusher` to the SAME URL+stream_key; if our pre-existing
    // rust_pusher still holds the original `Session` open, YouTube/FB
    // sees two concurrent publishes and rejects/kills one of them. Both
    // takes are no-ops when None.
    if let Some(mut p) = proc.take() {
        p.kill().await;
    }
    if let Some(p) = rust_pusher.take() {
        drop(p);
    }

    {
        let mut s = stats.lock().await;
        s.delivery_mode = "rescue".to_string();
        s.rescue_eta_secs = Some(RESCUE_REFILL_TARGET_SECS);
    }

    let should_stop = run_rescue_loop(
        alias,
        rescue_video_url,
        service_type,
        stream_key,
        buffer_state,
        stats,
        stop_rx,
        audit_ring,
    )
    .await;
    if should_stop {
        return OutageRescueOutcome::Stop;
    }

    // Review #1 (cont): reconstruct the rust_pusher so the consumer can
    // resume normal-delivery writes against a fresh `Session`
    // (lazy-connects on next push). Timestamps reset from zero — that's
    // expected after a rescue gap.
    if use_rust_pusher {
        let url = crate::endpoint_rtmp_url::build_rtmp_url(service_type, stream_key);
        *rust_pusher = Some(rs_rtmp_push::RtmpPusher::new(
            url,
            rs_rtmp_push::PusherConfig::default(),
        ));
    }
    {
        let mut s = stats.lock().await;
        s.delivery_mode = "normal".to_string();
        s.rescue_eta_secs = None;
    }
    let gap = rescue_started.elapsed().as_secs();
    crate::rescue_audit::emit_recovered(audit_ring, alias, gap);
    tracing::info!(alias, "Consumer: resumed normal delivery");
    OutageRescueOutcome::Recovered
}

/// Defensive rescue when the consumer's `rx.recv()` returns `None`
/// (producer panicked or stop_tx closed the channel).
///
/// Used by `consumer_task` to push DEFAULT_RESCUE_FLV (or the operator's
/// custom URL) during the ~30s endpoint_task teardown window so viewers
/// see rescue content instead of an immediate black screen.
///
/// Review-finding fixes baked in here:
///
/// * **#1 (duplicate-publish):** drops the existing `rust_pusher` (closing
///   its RTMP `Session`) BEFORE entering rescue, so the fresh `RtmpPusher`
///   spawned inside `run_rescue_loop` doesn't race the old one to publish
///   on the same URL. YouTube/FB reject the second publish as "publish
///   busy" otherwise.
/// * **#4 (misleading recovery):** does NOT emit `RescueRecovered` on
///   exit — the producer is dead and the consumer immediately breaks,
///   so there was no actual recovery. The audit timeline shows just
///   `RescueActivated`, which operators can correlate with the
///   surrounding endpoint teardown rows.
/// * **#5 (file-size cap):** lives here instead of inline in
///   `endpoint_task.rs::consumer_task` so the consumer fn drops well
///   below the 1000-line CI cap.
#[allow(clippy::too_many_arguments)]
pub async fn run_defensive_rescue(
    alias: &str,
    rescue_video_url: Option<&str>,
    service_type: rs_ffmpeg::ServiceType,
    stream_key: &str,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    last_delivered_chunk_id: i64,
    proc: &mut Option<Box<dyn crate::endpoint_task::OutputProcess>>,
    rust_pusher: &mut Option<rs_rtmp_push::RtmpPusher>,
) {
    crate::rescue_audit::emit_activated(audit_ring, alias, last_delivered_chunk_id);

    // Review #1: kill any orphaned ffmpeg child AND drop the existing
    // rust_pusher so its RTMP `Session` is closed BEFORE `run_rescue_loop`
    // constructs a fresh pusher to the same URL+stream_key. Two concurrent
    // publishes to the same key trigger publish-busy on YouTube/FB and
    // break the stream.
    if let Some(mut p) = proc.take() {
        p.kill().await;
    }
    if let Some(p) = rust_pusher.take() {
        drop(p);
    }

    {
        let mut s = stats.lock().await;
        s.delivery_mode = "rescue".to_string();
        s.rescue_eta_secs = Some(RESCUE_REFILL_TARGET_SECS);
    }

    // Returns when stop_rx fires (endpoint_task tearing us down via the
    // select-loop consumer-drain timeout). No producer respawn in this
    // scoped fix, so refill never completes and rescue runs until stop.
    let _should_stop = run_rescue_loop(
        alias,
        rescue_video_url,
        service_type,
        stream_key,
        buffer_state,
        stats,
        stop_rx,
        audit_ring,
    )
    .await;

    // Review #4: NO emit_recovered here. The producer is dead, the
    // consumer breaks immediately after this returns — there is no
    // recovery to report. A `RescueRecovered` row would mislead operators
    // reading the audit timeline ("rescue recovered" right before the
    // endpoint silently disappears).
    tracing::info!(
        alias,
        "Consumer: defensive rescue exited; consumer will break"
    );
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

        // Resolve the rescue clip source BEFORE spawning so the audit_ring
        // borrow stays local to this function — the spawned task only owns
        // the resolved source. Warmup mode always shows the "Vysielanie sa o
        // chvíľu spustí…" segment (the probe loop below owns warmup timing),
        // so segment swapping is a no-op here; the source is still resolved so
        // a custom operator clip plays during warmup if configured.
        let audit_ring_owned: Option<std::sync::Arc<crate::audit_ring::AuditRing>> =
            audit_ring.cloned();
        let source = resolve_rescue_source(rescue_video_url, &audit_ring_owned, alias).await;

        // Seed warmup stats so the dashboard reflects warmup state from the
        // first frame. The probe loop below updates rescue_eta_secs each tick.
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
                source,
                dummy_buffer_state,
                stats_clone,
                &mut warmup_stop,
                crate::rust_rescue_push::RescuePushMode::Warmup,
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

                // R3 GREEN: non-fast endpoints always have rescue pushing in
                // the background, so the warmup stats must reflect progress
                // (the dashboard reads rescue_eta_secs) regardless of URL
                // config. Fast endpoints skip rescue entirely (per design) and
                // therefore skip the stats update too. #259: the viewer sees
                // the "Vysielanie sa o chvíľu spustí…" warmup segment; the
                // exact-seconds countdown lives on the dashboard via
                // rescue_eta_secs.
                if !ep_cfg.is_fast {
                    let remaining_ms = delivery_delay_ms.saturating_sub(accum_ms);
                    let eta_secs = remaining_ms.div_ceil(1000);

                    let mut s = stats.lock().await;
                    s.delivery_mode = "warmup".to_string();
                    s.rescue_eta_secs = Some(eta_secs);
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

    // Tear down the warmup rescue pusher task.
    // Aborting the JoinHandle drops the spawned task; the RtmpPusher
    // inside is dropped, which closes its session (kill_on_drop-equivalent
    // for pure-rust). No external ffmpeg process to reap.
    //
    // Review finding #3: AWAIT the aborted handle so the spawned task
    // actually unwinds (dropping the RtmpPusher's Session and closing
    // its TCP socket) BEFORE this function returns. Without the await,
    // consumer_task immediately constructs a new RtmpPusher to the same
    // RTMP URL and the still-alive warmup Session would race to publish
    // — YouTube/FB would reject one of the two as publish-busy.
    if let Some(handle) = warmup_handle {
        handle.abort();
        let _ = handle.await;
        tracing::info!(alias, "Warmup rescue pusher stopped");
    }

    if !stopped {
        let mut s = stats.lock().await;
        s.delivery_mode = "normal".to_string();
        s.rescue_eta_secs = None;
    }

    stopped
}

/// Resolve the FLV bytes to push during rescue for this endpoint.
///
/// Returns `RescueClipSource::Countdown` (the embedded Slovak segment set)
/// when:
///   * no operator URL configured (None / empty)
///   * URL is non-FLV (legacy MP4 / MOV / etc) — emits `RescueLegacyFormatRejected`
///   * S3 fetch fails — emits `RescueCustomFetchFailed`
///
/// Returns `RescueClipSource::Fixed(<S3 bytes>)` when a custom `.flv` URL
/// fetches successfully — a custom operator clip plays as-is (we cannot
/// composite a countdown onto it without a runtime renderer).
///
/// #259: the fallback is no longer a single static blob but the ETA-bucket
/// segment set, so the DEFAULT (no-custom-URL) rescue path — which is what
/// every production template uses — gets the live viewer-facing countdown.
pub async fn resolve_rescue_source(
    rescue_video_url: Option<&str>,
    audit_ring: &Option<std::sync::Arc<crate::audit_ring::AuditRing>>,
    alias: &str,
) -> crate::rescue_segments::RescueClipSource {
    use crate::rescue_segments::RescueClipSource;

    let url = match rescue_video_url {
        Some(u) if !u.is_empty() => u,
        _ => return RescueClipSource::Countdown,
    };

    if !url.to_lowercase().ends_with(".flv") {
        tracing::warn!(alias, url, "Non-FLV rescue URL rejected; using countdown");
        crate::rescue_audit::emit_legacy_rejected(audit_ring, alias, url);
        return RescueClipSource::Countdown;
    }

    match fetch_flv_from_s3(url).await {
        Ok(bytes) => RescueClipSource::Fixed(std::sync::Arc::new(bytes)),
        Err(e) => {
            tracing::warn!(alias, url, "Rescue FLV fetch failed: {e}; using countdown");
            crate::rescue_audit::emit_custom_fetch_failed(audit_ring, alias, url, &e.to_string());
            RescueClipSource::Countdown
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
mod resolve_rescue_source_tests {
    use super::*;
    use crate::rescue_segments::RescueClipSource;

    #[tokio::test]
    async fn returns_countdown_when_url_none() {
        let result = resolve_rescue_source(None, &None, "test-alias").await;
        assert!(matches!(result, RescueClipSource::Countdown));
    }

    #[tokio::test]
    async fn returns_countdown_when_url_empty() {
        let result = resolve_rescue_source(Some(""), &None, "test-alias").await;
        assert!(matches!(result, RescueClipSource::Countdown));
    }

    #[tokio::test]
    async fn returns_countdown_when_url_not_flv() {
        // Legacy MP4 URL → reject, fall back to the countdown segment set.
        // No audit ring so no panic on emit.
        let result = resolve_rescue_source(
            Some("https://example.com/rescue-videos/abc.mp4"),
            &None,
            "test-alias",
        )
        .await;
        assert!(matches!(result, RescueClipSource::Countdown));
    }
}
