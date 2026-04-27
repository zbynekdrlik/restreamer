//! Rescue mode: plays a looped video with countdown overlay when the
//! delivery buffer is empty (warmup or outage recovery).
use rs_ffmpeg::ServiceType;

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

/// Build ffmpeg arguments for the rescue video loop with drawtext overlay.
/// All service types use FLV output (YT_HLS removed in #135).
pub fn build_rescue_ffmpeg_args(
    rescue_video_url: &str,
    endpoint_url: &str,
    alias: &str,
) -> Vec<String> {
    let countdown_path = countdown_file_path(alias);

    // Normalize the source video to stable YouTube-safe parameters so
    // switching between the real OBS stream and the rescue video doesn't
    // confuse YouTube's ingestion (format/resolution/framerate changes
    // cause "bad" health reports, reconnects, or outright rejection).
    //
    // Pipeline:
    //   scale to 1920x1080 preserving aspect, letterbox with black bars,
    //   set sample aspect to 1:1, force 30fps, yuv420p color.
    //   Then overlay the countdown text read from disk (reload=1).
    //
    // 1080p30 is the lowest common denominator that every RTMP/HLS
    // destination accepts without complaint. It handles source videos of
    // any resolution or framerate.
    let vf = format!(
        concat!(
            "scale=1920:1080:force_original_aspect_ratio=decrease,",
            "pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black,",
            "setsar=1,fps=30,format=yuv420p,",
            "drawtext=textfile={}:reload=1:fontsize=48:fontcolor=white:",
            "x=(w-tw)/2:y=h-80:borderw=2:bordercolor=black"
        ),
        countdown_path
    );

    // Standard 1080p30 H.264 + AAC encoder settings. Keyframe every 2s
    // (-g 60 at 30fps) matches what OBS typically sends and what YouTube
    // expects for low-latency ingestion.
    let mut args = vec![
        "-stream_loop".into(),
        "-1".into(),
        "-re".into(),
        "-i".into(),
        rescue_video_url.to_string(),
        "-vf".into(),
        vf,
        // Video encoder
        "-c:v".into(),
        "libx264".into(),
        "-preset".into(),
        "veryfast".into(),
        "-profile:v".into(),
        "main".into(),
        "-level".into(),
        "4.0".into(),
        "-pix_fmt".into(),
        "yuv420p".into(),
        "-r".into(),
        "30".into(),
        "-g".into(),
        "60".into(),
        "-keyint_min".into(),
        "60".into(),
        "-sc_threshold".into(),
        "0".into(),
        "-b:v".into(),
        "4500k".into(),
        "-maxrate".into(),
        "4500k".into(),
        "-bufsize".into(),
        "9000k".into(),
        // Audio encoder
        "-c:a".into(),
        "aac".into(),
        "-b:a".into(),
        "128k".into(),
        "-ar".into(),
        "48000".into(),
        "-ac".into(),
        "2".into(),
    ];

    // All service types use FLV output (YT_HLS removed in #135).
    args.extend_from_slice(&[
        "-f".into(),
        "flv".into(),
        "-flvflags".into(),
        "no_duration_filesize".into(),
        endpoint_url.to_string(),
    ]);

    args
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

/// Determine the output format string based on service type.
/// All supported service types use FLV output.
pub fn output_format_for_service(_service_type: ServiceType) -> &'static str {
    "flv"
}

/// Build the endpoint URL for a given service type and stream key.
pub fn endpoint_url_for_service(service_type: ServiceType, stream_key: &str) -> String {
    match service_type {
        ServiceType::YtRtmp => format!("rtmp://a.rtmp.youtube.com/live2/{stream_key}"),
        ServiceType::Facebook => format!("rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}"),
        ServiceType::Vimeo => format!("rtmps://rtmp-global.cloud.vimeo.com:443/live/{stream_key}"),
        ServiceType::Instagram => {
            format!("rtmps://live-upload.instagram.com:443/rtmp/{stream_key}")
        }
        ServiceType::TestFile => {
            let output_dir = std::env::var("RESTREAMER_TEST_OUTPUT_DIR")
                .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
            let safe = stream_key.replace([' ', '/'], "_");
            format!("{output_dir}/restreamer_rescue_{safe}.flv")
        }
    }
}

/// Run the rescue ffmpeg loop: spawn rescue ffmpeg, update countdown every 5s,
/// wait until buffer is refilled or stop signal received.
///
/// Returns `true` if a stop signal was received (caller should exit),
/// `false` if the buffer was refilled and normal delivery can resume.
#[allow(clippy::too_many_arguments)]
pub async fn run_rescue_loop(
    alias: &str,
    rescue_url: &str,
    service_type: rs_ffmpeg::ServiceType,
    stream_key: &str,
    buffer_state: &std::sync::Arc<crate::buffer_state::BufferState>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let ep_url = endpoint_url_for_service(service_type, stream_key);
    let rescue_args = build_rescue_ffmpeg_args(rescue_url, &ep_url, alias);

    let initial_text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        RESCUE_REFILL_TARGET_SECS,
    );
    write_countdown_file(alias, &initial_text);

    let mut rescue_proc = match tokio::process::Command::new("ffmpeg")
        .args(&rescue_args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(alias, "Failed to spawn rescue ffmpeg: {e}");
            cleanup_countdown_file(alias);
            return false;
        }
    };

    tracing::info!(alias, "Rescue ffmpeg started");

    // Exit condition: producer must be active (finding chunks on S3) for
    // RESCUE_REFILL_TARGET_SECS continuous seconds. This proves OBS is back
    // streaming AND enough time has passed to refill the cache window.
    //
    // The original design tracked `buffer_duration_ms` via the producer, but
    // that counter is capped by the prefetch channel capacity (10 chunks /
    // ~20s) because the consumer is blocked in rescue mode — so it could
    // never reach the 120s target. A time-based check sidesteps that and
    // correctly models "OBS is back and stable".
    let target_secs = RESCUE_REFILL_TARGET_SECS;
    let mut continuous_active_secs: u64 = 0;
    let should_stop = loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                let is_active = buffer_state.producer_active.load(std::sync::atomic::Ordering::Relaxed);
                if is_active {
                    continuous_active_secs = continuous_active_secs.saturating_add(5);
                } else {
                    continuous_active_secs = 0;
                }

                let eta_secs = target_secs.saturating_sub(continuous_active_secs);

                let text = format_countdown_text(
                    &DeliveryMode::Rescue { reason: RescueReason::BufferEmpty },
                    eta_secs,
                );
                write_countdown_file(alias, &text);

                {
                    let mut s = stats.lock().await;
                    s.delivery_mode = if is_active {
                        "recovering".to_string()
                    } else {
                        "rescue".to_string()
                    };
                    s.rescue_eta_secs = Some(eta_secs);
                }

                if continuous_active_secs >= target_secs {
                    tracing::info!(
                        alias,
                        continuous_active_secs,
                        "Producer active for target window, exiting rescue mode"
                    );
                    break false;
                }
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    break true;
                }
            }
        }
    };

    let _ = rescue_proc.kill().await;
    cleanup_countdown_file(alias);
    should_stop
}

/// Run the warmup phase: spawn rescue ffmpeg (if configured), then probe S3
/// for chunks until the target delay is accumulated. Returns `true` if a
/// stop signal was received.
///
/// When `rescue_video_url` is Some and the endpoint is not fast, the rescue
/// ffmpeg runs alongside the chunk probing so viewers see the rescue video
/// during the initial cache fill — otherwise they'd see nothing for ~120s.
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
) -> bool {
    let mut warmup_proc: Option<tokio::process::Child> = None;

    // Spawn rescue ffmpeg if configured (non-fast endpoints only)
    if let Some(rescue_url) = rescue_video_url {
        if !ep_cfg.is_fast {
            let svc_type: rs_ffmpeg::ServiceType = ep_cfg
                .service_type
                .parse()
                .unwrap_or(rs_ffmpeg::ServiceType::TestFile);
            let ep_url = endpoint_url_for_service(svc_type, &ep_cfg.stream_key);
            let rescue_args = build_rescue_ffmpeg_args(rescue_url, &ep_url, alias);

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

            match tokio::process::Command::new("ffmpeg")
                .args(&rescue_args)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .kill_on_drop(true)
                .spawn()
            {
                Ok(p) => {
                    tracing::info!(alias, "Warmup rescue ffmpeg started");
                    warmup_proc = Some(p);
                }
                Err(e) => {
                    tracing::error!(alias, "Failed to spawn warmup rescue ffmpeg: {e}");
                    cleanup_countdown_file(alias);
                }
            }
        }
    }

    // Warmup exits when the buffer is filled. During warmup, rescue
    // ffmpeg plays the configured rescue video to the endpoint and the
    // countdown overlay shows time remaining until normal delivery
    // starts.
    //
    // If chunks already exist on S3 when the VPS boots (which they
    // usually do because OBS has been streaming during the ~60-90s VPS
    // boot), accum_ms grows fast through the existing chunks and
    // warmup exits quickly — viewers see real content ASAP. If cache
    // really is being built from zero, warmup plays rescue video
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

                if rescue_video_url.is_some() {
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

    // Tear down warmup rescue ffmpeg and countdown file
    if let Some(mut p) = warmup_proc.take() {
        let _ = p.kill().await;
        tracing::info!(alias, "Warmup rescue ffmpeg stopped");
    }
    cleanup_countdown_file(alias);

    if !stopped {
        let mut s = stats.lock().await;
        s.delivery_mode = "normal".to_string();
        s.rescue_eta_secs = None;
    }

    stopped
}

#[cfg(test)]
#[path = "rescue_tests.rs"]
mod tests;
