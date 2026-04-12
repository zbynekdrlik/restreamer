//! Rescue mode: plays a looped video with countdown overlay when the
//! delivery buffer is empty (warmup or outage recovery).
use rs_ffmpeg::ServiceType;

/// Fixed buffer refill target before resuming normal delivery (seconds).
pub const RESCUE_REFILL_TARGET_SECS: u64 = 120;

/// Seconds of producer stall (no new chunks) before entering rescue mode.
pub const RESCUE_STALL_THRESHOLD_SECS: u64 = 30;

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
pub fn build_rescue_ffmpeg_args(
    rescue_video_url: &str,
    endpoint_url: &str,
    output_format: &str,
    alias: &str,
) -> Vec<String> {
    let countdown_path = countdown_file_path(alias);
    let drawtext = format!(
        "drawtext=textfile={}:reload=1:fontsize=48:fontcolor=white:x=(w-tw)/2:y=h-80:borderw=2:bordercolor=black",
        countdown_path
    );

    let mut args = vec![
        "-stream_loop".into(),
        "-1".into(),
        "-re".into(),
        "-i".into(),
        rescue_video_url.to_string(),
        "-vf".into(),
        drawtext,
        "-c:v".into(),
        "libx264".into(),
        "-preset".into(),
        "ultrafast".into(),
        "-c:a".into(),
        "aac".into(),
        "-b:a".into(),
        "128k".into(),
    ];

    match output_format {
        "hls" => {
            args.extend_from_slice(&[
                "-f".into(),
                "hls".into(),
                "-hls_segment_type".into(),
                "mpegts".into(),
                "-hls_list_size".into(),
                "5".into(),
                "-hls_time".into(),
                "2".into(),
                "-hls_flags".into(),
                "delete_segments".into(),
                "-start_number".into(),
                "0".into(),
                "-method".into(),
                "PUT".into(),
                "-flags".into(),
                "+cgop".into(),
                "-muxdelay".into(),
                "0".into(),
                "-muxpreload".into(),
                "0".into(),
                "-reset_timestamps".into(),
                "1".into(),
                endpoint_url.to_string(),
            ]);
        }
        _ => {
            args.extend_from_slice(&[
                "-f".into(),
                "flv".into(),
                "-flvflags".into(),
                "no_duration_filesize".into(),
                endpoint_url.to_string(),
            ]);
        }
    }

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
pub fn countdown_file_path(alias: &str) -> String {
    let safe_alias = alias.replace([' ', '/', '\\'], "_");
    format!("/tmp/rescue_{safe_alias}.txt")
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
pub fn output_format_for_service(service_type: ServiceType) -> &'static str {
    match service_type {
        ServiceType::YtHls => "hls",
        _ => "flv",
    }
}

/// Build the endpoint URL for a given service type and stream key.
pub fn endpoint_url_for_service(service_type: ServiceType, stream_key: &str) -> String {
    match service_type {
        ServiceType::YtHls => format!(
            "https://a.upload.youtube.com/http_upload_hls?cid={stream_key}&copy=0&file=out1248.ts"
        ),
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
    buffer_state: &std::sync::Arc<crate::endpoint_task::BufferState>,
    stats: &crate::endpoint_task::Stats,
    stop_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> bool {
    let ep_url = endpoint_url_for_service(service_type, stream_key);
    let out_fmt = output_format_for_service(service_type);
    let rescue_args = build_rescue_ffmpeg_args(rescue_url, &ep_url, out_fmt, alias);

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

    let target_ms = RESCUE_REFILL_TARGET_SECS * 1000;
    let should_stop = loop {
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                let buf_ms = buffer_state.buffer_duration_ms.load(std::sync::atomic::Ordering::Relaxed);
                let eta_secs = if buf_ms >= target_ms { 0 } else { (target_ms - buf_ms) / 1000 };

                let text = format_countdown_text(
                    &DeliveryMode::Rescue { reason: RescueReason::BufferEmpty },
                    eta_secs,
                );
                write_countdown_file(alias, &text);

                {
                    let mut s = stats.lock().await;
                    s.delivery_mode = if buffer_state.producer_active.load(std::sync::atomic::Ordering::Relaxed) {
                        "recovering".to_string()
                    } else {
                        "rescue".to_string()
                    };
                    s.rescue_eta_secs = Some(eta_secs);
                }

                if buf_ms >= target_ms {
                    tracing::info!(alias, buf_ms, "Buffer refilled, exiting rescue mode");
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

#[cfg(test)]
#[path = "rescue_tests.rs"]
mod tests;
