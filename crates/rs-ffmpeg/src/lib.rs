/// FFmpeg process management for streaming endpoints.
///
/// Spawns and manages ffmpeg processes for different streaming service types.
/// Each service type (YouTube HLS, Facebook, etc.) has a specific ffmpeg
/// command configuration.
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};

#[derive(Debug, Error)]
pub enum FfmpegError {
    #[error("ffmpeg spawn failed: {0}")]
    SpawnFailed(#[from] std::io::Error),
    #[error("ffmpeg stdin closed")]
    StdinClosed,
    #[error("ffmpeg process exited with code {0}")]
    ProcessExited(i32),
    #[error("ffmpeg not found")]
    NotFound,
}

/// Supported streaming service types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ServiceType {
    #[serde(rename = "YT_HLS")]
    YtHls,
    #[serde(rename = "FB")]
    Facebook,
    #[serde(rename = "YT_RTMP")]
    YtRtmp,
    #[serde(rename = "VIMEO")]
    Vimeo,
    #[serde(rename = "INSTAGRAM")]
    Instagram,
    #[serde(rename = "TEST_FILE")]
    TestFile,
}

impl std::fmt::Display for ServiceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::YtHls => write!(f, "YT_HLS"),
            Self::Facebook => write!(f, "FB"),
            Self::YtRtmp => write!(f, "YT_RTMP"),
            Self::Vimeo => write!(f, "VIMEO"),
            Self::Instagram => write!(f, "INSTAGRAM"),
            Self::TestFile => write!(f, "TEST_FILE"),
        }
    }
}

impl std::str::FromStr for ServiceType {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "YT_HLS" => Ok(Self::YtHls),
            "FB" => Ok(Self::Facebook),
            "YT_RTMP" => Ok(Self::YtRtmp),
            "VIMEO" => Ok(Self::Vimeo),
            "INSTAGRAM" => Ok(Self::Instagram),
            "TEST_FILE" => Ok(Self::TestFile),
            other => Err(format!("unknown service type: {other}")),
        }
    }
}

/// Consumer read rate for ffmpeg, tuned to match measured producer FLV-timestamp
/// rate (OBS encodes ~30.30 fps but stamps FLV tags at 1/30 s increments, causing
/// producer timestamps to advance ~0.6% slower than wall-clock).
///
/// At `-re` (= `-readrate 1.0`), ffmpeg drains 1000 ms of timestamp per wall-clock
/// second while producer fills only ~994 ms — cache shrinks at ~20 s/hour on
/// multi-hour streams. Setting to 0.994 matches producer and holds cache stable.
///
/// See `docs/superpowers/specs/2026-04-23-phase2-evidence/analysis.md` (issue #135).
/// If OBS profile changes (e.g. 60 fps, 24 fps NTSC), re-measure producer rate
/// via `/api/v1/diagnostics/pacing` and re-tune this constant.
const CONSUMER_READRATE: &str = "0.994";

/// Initial burst duration (seconds) before `-readrate` throttling kicks in.
/// YouTube HLS ingest flags "videoIngestionStarved: Video output low" when the
/// initial few seconds of bitrate fall below the declared stream bitrate.
/// With pure `-readrate 0.994`, the 0.6% slowdown trips YouTube at warmup. The
/// burst lets ffmpeg ship the first 10 seconds of media at native rate (matches
/// old `-re` behavior during warmup) before settling into the drift-corrected
/// steady state.
const CONSUMER_INITIAL_BURST_SECS: &str = "10";

/// Build the ffmpeg command arguments for a given service type and stream key.
/// All endpoints use FLV input from pipe.
pub fn build_ffmpeg_args(service_type: ServiceType, stream_key: &str, alias: &str) -> Vec<String> {
    match service_type {
        ServiceType::YtHls => build_yt_hls_args(stream_key),
        ServiceType::YtRtmp => {
            build_flv_rtmp_args(&format!("rtmp://a.rtmp.youtube.com/live2/{stream_key}"))
        }
        ServiceType::Facebook => build_flv_rtmp_args(&format!(
            "rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}"
        )),
        ServiceType::Vimeo => build_flv_rtmp_args(&format!(
            "rtmps://rtmp-global.cloud.vimeo.com:443/live/{stream_key}"
        )),
        ServiceType::Instagram => build_flv_rtmp_args(&format!(
            "rtmps://live-upload.instagram.com:443/rtmp/{stream_key}"
        )),
        ServiceType::TestFile => build_test_file_args(alias),
    }
}

/// YT_HLS: FLV input, HLS output via HTTPS PUT.
fn build_yt_hls_args(stream_key: &str) -> Vec<String> {
    let output_url = format!(
        "https://a.upload.youtube.com/http_upload_hls?cid={stream_key}&copy=0&file=out1248.ts"
    );
    vec![
        // -readrate 0.994: steady-state drain 0.6% below -re nominal, matching
        //   the producer's measured FLV-tag-advance rate (see #135 Phase 2
        //   evidence). Prevents cache from drifting down on multi-hour streams.
        // -readrate_initial_burst 10: 10s initial burst at full rate so YouTube
        //   HLS ingest sees expected bitrate at warmup (avoids
        //   "videoIngestionStarved" flag in the first seconds).
        "-readrate".into(),
        CONSUMER_READRATE.into(),
        "-readrate_initial_burst".into(),
        CONSUMER_INITIAL_BURST_SECS.into(),
        "-f".into(),
        "flv".into(),
        "-loglevel".into(),
        "info".into(),
        "-i".into(),
        "pipe:".into(),
        "-avoid_negative_ts".into(),
        "make_zero".into(),
        "-f".into(),
        "hls".into(),
        "-hls_segment_type".into(),
        "mpegts".into(),
        "-hls_segment_options".into(),
        "mpegts_flags=+pat_pmt_at_frames+resend_headers".into(),
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
        "-c".into(),
        "copy".into(),
        "-flags".into(),
        "+cgop".into(),
        "-muxdelay".into(),
        "0".into(),
        "-muxpreload".into(),
        "0".into(),
        "-reset_timestamps".into(),
        "1".into(),
        output_url,
    ]
}

/// FLV->FLV passthrough for RTMP/RTMPS endpoints.
/// Minimal flags: input is already valid FLV, just forward bytes.
/// No genpts, no avoid_negative_ts, no copytb, no bsf needed.
fn build_flv_rtmp_args(url: &str) -> Vec<String> {
    vec![
        // -readrate / -readrate_initial_burst: see build_yt_hls_args for rationale.
        "-readrate".into(),
        CONSUMER_READRATE.into(),
        "-readrate_initial_burst".into(),
        CONSUMER_INITIAL_BURST_SECS.into(),
        "-f".into(),
        "flv".into(),
        "-loglevel".into(),
        "info".into(),
        "-i".into(),
        "pipe:".into(),
        "-c".into(),
        "copy".into(),
        "-f".into(),
        "flv".into(),
        "-flvflags".into(),
        "no_duration_filesize".into(),
        url.to_string(),
    ]
}

fn build_test_file_args(alias: &str) -> Vec<String> {
    let output_dir = std::env::var("RESTREAMER_TEST_OUTPUT_DIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
    let safe_alias = alias.replace([' ', '/'], "_");
    let output_path = PathBuf::from(&output_dir)
        .join(format!("restreamer_test_{safe_alias}.flv"))
        .to_string_lossy()
        .to_string();
    vec![
        // TEST_FILE uses -re for precise 1.0x rate because test suites compare
        // output file length to expected duration. Don't apply the 0.994
        // steady-state throttle here (it's a live-stream drift compensation).
        "-re".into(),
        "-f".into(),
        "flv".into(),
        "-loglevel".into(),
        "info".into(),
        "-i".into(),
        "pipe:".into(),
        "-f".into(),
        "flv".into(),
        "-c".into(),
        "copy".into(),
        output_path,
    ]
}

/// Max stderr lines to keep in the ring buffer.
const STDERR_BUFFER_SIZE: usize = 30;

/// A sample of ffmpeg's internal media-time progress, captured from stderr.
/// `media_time_ms`: ffmpeg's `time=` field parsed to milliseconds.
/// `wall_clock_ms`: Unix epoch ms when we received that progress line.
#[derive(Debug, Clone)]
pub struct FfmpegProgress {
    pub media_time_ms: i64,
    pub wall_clock_ms: i64,
}

/// Parse an ffmpeg stderr progress line and extract the `time=HH:MM:SS.xx` value in ms.
/// Returns `None` if the line has no `time=` field or the value is unparseable.
///
/// Only matches `time=` as a standalone whitespace-delimited token so that
/// fields like `out_time=` or `xtime=` are not mistakenly parsed.
/// Negative time values (e.g. `time=-00:00:05.00`) are rejected and return `None`.
/// Arithmetic that would overflow `i64` also returns `None`.
pub fn parse_ffmpeg_time_ms(line: &str) -> Option<i64> {
    // I3: token-based search — only match "time=" as a standalone token.
    let field = line.split_whitespace().find(|f| f.starts_with("time="))?;
    let field = &field[5..]; // strip "time="
    // I2: reject negative times.
    if field.starts_with('-') {
        return None;
    }
    let field = field.replace(',', ".");
    let mut it = field.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let s: f64 = it.next()?.parse().ok()?;
    // I1: use checked arithmetic to avoid overflow panic.
    let h_ms = h.checked_mul(3_600_000)?;
    let m_ms = m.checked_mul(60_000)?;
    let s_ms = (s * 1000.0) as i64;
    h_ms.checked_add(m_ms)?.checked_add(s_ms)
}

/// Drain an ffmpeg-like stderr stream line-by-line.
///
/// Each line is appended to the ring buffer and, if it parses as a
/// `time=...` progress line and `progress_tx` is `Some`, forwarded as an
/// [`FfmpegProgress`] event on the channel.  Events are sent with
/// `try_send`; if the channel is full the sample is dropped silently
/// (best-effort telemetry).  Exits when the reader reaches EOF.
///
/// ffmpeg uses `\r` (carriage return) to overwrite progress stats in a
/// terminal: multiple stat updates accumulate in a single `\n`-terminated
/// output line, separated by `\r`.  To capture EVERY stat update (not
/// just the first in each burst), each `\n`-line is split on `\r` before
/// parsing.  This multiplies consumer-rate sample density from
/// ~1/output-burst to ~1/stat-update (~0.5 s cadence on typical HLS/RTMP
/// streams).
///
/// Extracted as a public function so it can be unit-tested without
/// spawning a real ffmpeg process (C1 wiring test).
pub async fn drain_ffmpeg_stderr<R: tokio::io::AsyncBufRead + Unpin>(
    reader: R,
    stderr_lines: Arc<Mutex<VecDeque<String>>>,
    progress_tx: Option<tokio::sync::mpsc::Sender<FfmpegProgress>>,
    alias_for_log: &str,
) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        // Ring buffer (existing behaviour): store the raw line.
        if let Ok(mut buf) = stderr_lines.lock() {
            if buf.len() >= STDERR_BUFFER_SIZE {
                buf.pop_front();
            }
            buf.push_back(line.clone());
        }
        // Progress events: split on \r so every stat update within a
        // \n-terminated output burst emits its own FfmpegProgress event.
        if let Some(tx) = &progress_tx {
            for segment in line.split('\r') {
                if let Some(media_time_ms) = parse_ffmpeg_time_ms(segment) {
                    let wall_clock_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as i64;
                    let _ = tx.try_send(FfmpegProgress {
                        media_time_ms,
                        wall_clock_ms,
                    });
                }
            }
        }
    }
    tracing::debug!(alias = %alias_for_log, "ffmpeg stderr drain task finished");
}

/// Managed ffmpeg process with stdin pipe for writing data.
pub struct FfmpegProcess {
    child: Child,
    service_type: ServiceType,
    alias: String,
    stderr_lines: Arc<Mutex<VecDeque<String>>>,
}

impl FfmpegProcess {
    /// Spawn a new ffmpeg process for the given service type.
    /// Backwards-compatible wrapper around [`spawn_with_progress`] with no progress channel.
    pub fn spawn(
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
    ) -> Result<Self, FfmpegError> {
        Self::spawn_with_progress(service_type, stream_key, alias, None)
    }

    /// Spawn a new ffmpeg process, optionally emitting [`FfmpegProgress`] events on `progress_tx`.
    ///
    /// Each time ffmpeg writes a `time=HH:MM:SS.xx` progress line to stderr,
    /// a `FfmpegProgress` sample is sent on the channel (if provided).
    /// The channel is bounded; if the receiver is slow, events are dropped
    /// (best-effort telemetry semantics — `try_send` with silent drop on full).
    pub fn spawn_with_progress(
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
        progress_tx: Option<tokio::sync::mpsc::Sender<FfmpegProgress>>,
    ) -> Result<Self, FfmpegError> {
        let args = build_ffmpeg_args(service_type, stream_key, alias);
        tracing::info!(
            service_type = %service_type,
            alias = alias,
            "Spawning ffmpeg"
        );

        let mut child = Command::new("ffmpeg")
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        // Spawn a background task to drain stderr and capture last N lines.
        let stderr = child.stderr.take();
        let stderr_lines: Arc<Mutex<VecDeque<String>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_BUFFER_SIZE)));
        let stderr_lines_clone = Arc::clone(&stderr_lines);
        let alias_owned = alias.to_string();
        tokio::spawn(async move {
            drain_ffmpeg_stderr(
                BufReader::new(stderr.expect("stderr is piped")),
                stderr_lines_clone,
                progress_tx,
                &alias_owned,
            )
            .await;
        });

        Ok(Self {
            child,
            service_type,
            alias: alias.to_string(),
            stderr_lines,
        })
    }

    /// Write data to ffmpeg's stdin.
    pub async fn write(&mut self, data: &[u8]) -> Result<(), FfmpegError> {
        let stdin = self.child.stdin.as_mut().ok_or(FfmpegError::StdinClosed)?;
        stdin
            .write_all(data)
            .await
            .map_err(|_| FfmpegError::StdinClosed)?;
        stdin.flush().await.map_err(|_| FfmpegError::StdinClosed)?;
        Ok(())
    }

    /// Check if the process is still running.
    pub fn is_alive(&mut self) -> bool {
        self.child.try_wait().ok().flatten().is_none()
    }

    /// Get the exit code if the process has exited.
    pub fn try_exit_code(&mut self) -> Option<i32> {
        self.child
            .try_wait()
            .ok()
            .flatten()
            .map(|s| s.code().unwrap_or(-1))
    }

    /// Kill the process.
    pub async fn kill(&mut self) {
        let _ = self.child.kill().await;
    }

    /// Get the last stderr line captured from ffmpeg.
    pub fn last_stderr_line(&self) -> Option<String> {
        self.stderr_lines
            .lock()
            .ok()
            .and_then(|buf| buf.back().cloned())
    }

    /// Get all captured stderr lines (up to STDERR_BUFFER_SIZE).
    pub fn stderr_lines(&self) -> Vec<String> {
        self.stderr_lines
            .lock()
            .ok()
            .map(|buf| buf.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get the captured stderr tail joined by newlines (up to
    /// STDERR_BUFFER_SIZE lines). Returns None when empty so callers can
    /// treat absence and emptiness identically.
    pub fn stderr_tail(&self) -> Option<String> {
        join_stderr_tail(self.stderr_lines())
    }

    pub fn service_type(&self) -> ServiceType {
        self.service_type
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

/// Pure helper extracted so the multi-line join logic can be unit-tested
/// without spawning ffmpeg.
pub(crate) fn join_stderr_tail(lines: Vec<String>) -> Option<String> {
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_stderr_tail_empty_returns_none() {
        assert_eq!(join_stderr_tail(vec![]), None);
    }

    #[test]
    fn join_stderr_tail_single_line_returns_that_line_verbatim() {
        let lines = vec!["only line".to_string()];
        assert_eq!(join_stderr_tail(lines), Some("only line".to_string()));
    }

    #[test]
    fn join_stderr_tail_multiple_lines_joined_with_newline_in_order() {
        let lines = vec![
            "first".to_string(),
            "second".to_string(),
            "third".to_string(),
        ];
        assert_eq!(
            join_stderr_tail(lines),
            Some("first\nsecond\nthird".to_string())
        );
    }

    #[test]
    fn join_stderr_tail_preserves_empty_inner_lines() {
        let lines = vec!["alpha".to_string(), String::new(), "gamma".to_string()];
        assert_eq!(join_stderr_tail(lines), Some("alpha\n\ngamma".to_string()));
    }

    #[test]
    fn service_type_display_roundtrip() {
        let types = [
            ServiceType::YtHls,
            ServiceType::Facebook,
            ServiceType::YtRtmp,
            ServiceType::Vimeo,
            ServiceType::Instagram,
            ServiceType::TestFile,
        ];
        for st in types {
            let s = st.to_string();
            let parsed: ServiceType = s.parse().unwrap();
            assert_eq!(parsed, st);
        }
    }

    #[test]
    fn service_type_serde_roundtrip() {
        let types = [
            ServiceType::YtHls,
            ServiceType::Facebook,
            ServiceType::YtRtmp,
            ServiceType::Vimeo,
            ServiceType::Instagram,
            ServiceType::TestFile,
        ];
        for st in types {
            let json = serde_json::to_string(&st).unwrap();
            let parsed: ServiceType = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, st);
        }
    }

    #[test]
    fn build_yt_hls_args_correct() {
        let args = build_ffmpeg_args(ServiceType::YtHls, "test-key", "YouTube");
        assert!(args.iter().any(|a| a.contains("a.upload.youtube.com")));
        assert!(args.iter().any(|a| a.contains("test-key")));
        assert!(args.contains(&"hls".to_string()));
        assert!(args.contains(&"PUT".to_string()));
        assert!(args.contains(&"1".to_string())); // reset_timestamps
        assert!(args.contains(&"+cgop".to_string()));
        // YT_HLS now uses FLV input
        let f_idx = args.iter().position(|a| a == "-f").unwrap();
        assert_eq!(args[f_idx + 1], "flv", "YT_HLS should use FLV input");
        // Should use -readrate + -readrate_initial_burst for drift-compensated pacing
        assert!(args.contains(&"-readrate".to_string()));
        assert!(args.contains(&"-readrate_initial_burst".to_string()));
    }

    #[test]
    fn build_facebook_args_correct() {
        let args = build_ffmpeg_args(ServiceType::Facebook, "fb-key", "Facebook");
        assert!(args.iter().any(|a| a.contains("live-api-s.facebook.com")));
        assert!(args.iter().any(|a| a.contains("fb-key")));
        assert!(args.contains(&"flv".to_string()));
        assert!(args.contains(&"copy".to_string()));
    }

    #[test]
    fn build_yt_rtmp_args_correct() {
        let args = build_ffmpeg_args(ServiceType::YtRtmp, "yt-key", "YT RTMP");
        assert!(args.iter().any(|a| a.contains("a.rtmp.youtube.com")));
        assert!(args.iter().any(|a| a.contains("yt-key")));
        assert!(args.contains(&"flv".to_string()));
        // Full passthrough (no audio re-encode)
        assert!(args.contains(&"-c".to_string()));
        assert!(args.contains(&"copy".to_string()));
        assert!(
            !args.contains(&"-c:a".to_string()),
            "should not have separate audio codec"
        );
        assert!(
            !args.contains(&"aac".to_string()),
            "should not re-encode audio"
        );
        // FLV path should NOT have genpts, copytb, bsf, avoid_negative_ts
        assert!(
            !args.contains(&"+genpts".to_string()),
            "FLV path should not have genpts"
        );
        assert!(
            !args.contains(&"-copytb".to_string()),
            "FLV path should not have copytb"
        );
        assert!(
            !args.contains(&"-bsf:a".to_string()),
            "FLV path should not have bsf"
        );
    }

    #[test]
    fn build_vimeo_args_correct() {
        let args = build_ffmpeg_args(ServiceType::Vimeo, "vimeo-key", "Vimeo");
        assert!(
            args.iter()
                .any(|a| a.contains("rtmp-global.cloud.vimeo.com"))
        );
        assert!(args.contains(&"flv".to_string()));
    }

    #[test]
    fn build_instagram_args_correct() {
        let args = build_ffmpeg_args(ServiceType::Instagram, "ig-key", "IG");
        assert!(args.iter().any(|a| a.contains("live-upload.instagram.com")));
        assert!(args.contains(&"flv".to_string()));
    }

    #[test]
    fn build_test_file_args_correct() {
        let args = build_ffmpeg_args(ServiceType::TestFile, "", "Test Stream");
        assert!(
            args.iter()
                .any(|a| a.contains("restreamer_test_Test_Stream.flv"))
        );
        assert!(args.contains(&"flv".to_string()));
        assert!(args.contains(&"copy".to_string()));
    }

    #[test]
    fn build_test_file_sanitizes_alias() {
        let args = build_ffmpeg_args(ServiceType::TestFile, "", "My/Bad Stream");
        assert!(args.iter().any(|a| a.contains("My_Bad_Stream")));
    }

    #[test]
    fn all_commands_have_pipe_input() {
        let types = [
            ServiceType::YtHls,
            ServiceType::Facebook,
            ServiceType::YtRtmp,
            ServiceType::Vimeo,
            ServiceType::Instagram,
            ServiceType::TestFile,
        ];
        for st in types {
            let args = build_ffmpeg_args(st, "key", "alias");
            assert!(
                args.contains(&"pipe:".to_string()),
                "{st} missing pipe: input"
            );
        }
    }

    #[test]
    fn all_commands_have_flv_input() {
        let types = [
            ServiceType::YtHls,
            ServiceType::Facebook,
            ServiceType::YtRtmp,
            ServiceType::Vimeo,
            ServiceType::Instagram,
            ServiceType::TestFile,
        ];
        for st in types {
            let args = build_ffmpeg_args(st, "key", "alias");
            let f_idx = args.iter().position(|a| a == "-f").unwrap();
            assert_eq!(args[f_idx + 1], "flv", "{st} missing flv input format");
        }
    }

    /// Live-stream endpoints use `-readrate 0.994 -readrate_initial_burst 10`
    /// to match measured producer FLV-tag-advance rate (see #135 Phase 2).
    /// TEST_FILE keeps `-re` for exact-duration output file length.
    #[test]
    fn live_paths_use_readrate_flag() {
        let types = [
            ServiceType::YtHls,
            ServiceType::Facebook,
            ServiceType::YtRtmp,
            ServiceType::Vimeo,
            ServiceType::Instagram,
        ];
        for st in types {
            let args = build_ffmpeg_args(st, "key", "alias");
            assert!(
                args.contains(&"-readrate".to_string()),
                "{st} must have -readrate for drift-compensated pacing"
            );
            let idx = args.iter().position(|a| a == "-readrate").unwrap();
            assert_eq!(
                args[idx + 1],
                CONSUMER_READRATE,
                "{st} readrate must equal CONSUMER_READRATE"
            );
            assert!(
                args.contains(&"-readrate_initial_burst".to_string()),
                "{st} must have -readrate_initial_burst for YouTube warmup"
            );
            let burst_idx = args
                .iter()
                .position(|a| a == "-readrate_initial_burst")
                .unwrap();
            assert_eq!(
                args[burst_idx + 1],
                CONSUMER_INITIAL_BURST_SECS,
                "{st} initial-burst secs must equal CONSUMER_INITIAL_BURST_SECS"
            );
            assert!(
                !args.contains(&"-re".to_string()),
                "{st} must NOT have bare -re (use -readrate instead)"
            );
        }
    }

    #[test]
    fn test_file_keeps_re_for_exact_duration() {
        let args = build_ffmpeg_args(ServiceType::TestFile, "", "alias");
        assert!(
            args.contains(&"-re".to_string()),
            "TEST_FILE must keep -re for exact duration match"
        );
        assert!(
            !args.contains(&"-readrate".to_string()),
            "TEST_FILE must not apply live-stream drift compensation"
        );
    }

    #[test]
    fn flv_rtmp_commands_have_no_ts_artifacts() {
        let types = [
            ServiceType::YtRtmp,
            ServiceType::Facebook,
            ServiceType::Vimeo,
            ServiceType::Instagram,
        ];
        for st in types {
            let args = build_ffmpeg_args(st, "key", "alias");
            // FLV path should NOT have genpts, copytb, bsf, avoid_negative_ts
            assert!(
                !args.contains(&"+genpts".to_string()),
                "{st} should not have genpts"
            );
            assert!(
                !args.contains(&"-copytb".to_string()),
                "{st} should not have copytb"
            );
            assert!(
                !args.contains(&"-bsf:a".to_string()),
                "{st} should not have bsf"
            );
        }
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn parse_ffmpeg_time_simple() {
        // Typical ffmpeg progress line:
        // "frame=  150 fps= 30 q=28.0 size=  1024kB time=00:00:05.00 bitrate=..."
        let ms = parse_ffmpeg_time_ms(
            "frame=  150 fps= 30 q=28.0 size=  1024kB time=00:00:05.00 bitrate=1024kbits/s",
        );
        assert_eq!(ms, Some(5_000));
    }

    #[test]
    fn parse_ffmpeg_time_hhmmss_fractional() {
        let ms = parse_ffmpeg_time_ms("time=01:23:45.67");
        // 1h*3600 + 23m*60 + 45 = 5025s; + 0.67s = 5025.67s = 5_025_670 ms
        assert_eq!(ms, Some(5_025_670));
    }

    #[test]
    fn parse_ffmpeg_time_none_when_missing() {
        let ms = parse_ffmpeg_time_ms("frame= 100 fps=30 bitrate=N/A");
        assert_eq!(ms, None);
    }

    #[test]
    fn parse_ffmpeg_time_tolerates_dot_comma() {
        // Some locales emit "time=00:00:05,00"
        let ms = parse_ffmpeg_time_ms("time=00:00:05,00");
        assert_eq!(ms, Some(5_000));
    }

    // I2: negative time values must be rejected.
    #[test]
    fn parse_ffmpeg_time_rejects_negative() {
        assert_eq!(parse_ffmpeg_time_ms("time=-00:00:05.00"), None);
    }

    // I3: "time=" must be a standalone whitespace-delimited token.
    #[test]
    fn parse_ffmpeg_time_rejects_substring_match() {
        assert_eq!(parse_ffmpeg_time_ms("xtime=00:00:05.00"), None);
        assert_eq!(parse_ffmpeg_time_ms("out_time=00:00:05.00"), None);
    }

    // I1: overflow must return None, not panic.
    #[test]
    fn parse_ffmpeg_time_rejects_overflow() {
        assert_eq!(parse_ffmpeg_time_ms("time=9999999999999:00:00.00"), None);
    }

    #[test]
    fn parse_ffmpeg_time_handles_empty_value() {
        assert_eq!(parse_ffmpeg_time_ms("time="), None);
        assert_eq!(parse_ffmpeg_time_ms("time=abc"), None);
    }

    // C1: channel-wiring tests — exercise drain_ffmpeg_stderr without ffmpeg.
    #[tokio::test]
    async fn drain_ffmpeg_stderr_emits_progress_on_time_line() {
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};
        use tokio::io::BufReader;

        let input = b"frame= 30 fps=30 time=00:00:01.00 bitrate=1000kbits/s\n\
                      Stream #0:0 info\n\
                      frame= 60 fps=30 time=00:00:02.00 bitrate=1000kbits/s\n";
        let reader = BufReader::new(&input[..]);
        let ring: Arc<Mutex<VecDeque<String>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_BUFFER_SIZE)));
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);

        drain_ffmpeg_stderr(reader, Arc::clone(&ring), Some(tx), "test_alias").await;

        // Ring buffer got all three lines.
        let buf = ring.lock().unwrap();
        assert_eq!(buf.len(), 3);
        drop(buf);

        // Two progress events (the info line between them has no time=).
        let first = rx.recv().await.expect("first event");
        assert_eq!(first.media_time_ms, 1_000);
        let second = rx.recv().await.expect("second event");
        assert_eq!(second.media_time_ms, 2_000);
        // Channel should be empty after drain finishes.
        assert!(rx.try_recv().is_err(), "no more events expected");
    }

    #[tokio::test]
    async fn drain_ffmpeg_stderr_does_not_emit_when_no_progress_tx() {
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};
        use tokio::io::BufReader;

        let input = b"frame= 30 fps=30 time=00:00:01.00\n";
        let reader = BufReader::new(&input[..]);
        let ring = Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_BUFFER_SIZE)));

        // None progress_tx — no panic, just ring buffering.
        drain_ffmpeg_stderr(reader, Arc::clone(&ring), None, "test_alias").await;
        assert_eq!(ring.lock().unwrap().len(), 1);
    }

    // C2: ffmpeg uses \r to overwrite progress stats in a terminal — multiple
    // stat updates accumulate in a single \n-terminated line separated by \r.
    // drain_ffmpeg_stderr MUST split on \r so every stat update emits its own
    // FfmpegProgress event, not just the first one in each burst.
    // This ensures Phase 4 consumer-rate samples are dense enough to measure
    // the 0.994 drift ratio accurately.
    #[tokio::test]
    async fn drain_ffmpeg_stderr_splits_cr_separated_stats_into_separate_events() {
        use std::collections::VecDeque;
        use std::sync::{Arc, Mutex};
        use tokio::io::BufReader;

        // A single \n-terminated line with three \r-separated stat updates —
        // exactly what ffmpeg produces when stdout is a terminal or a pipe.
        let input = b"frame=  0 fps=0.0 size=0kB time=00:00:00.00 bitrate=N/A\r\
                      frame=  6 fps=0.0 size= 6kB time=00:00:01.00 bitrate= 40kbits/s\r\
                      frame= 12 fps=6.0 size=12kB time=00:00:02.00 bitrate= 40kbits/s\n";
        let reader = BufReader::new(&input[..]);
        let ring: Arc<Mutex<VecDeque<String>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_BUFFER_SIZE)));
        let (tx, mut rx) = tokio::sync::mpsc::channel(64);

        drain_ffmpeg_stderr(reader, Arc::clone(&ring), Some(tx), "test_alias").await;

        // Ring buffer received the raw (unsplit) line — one entry for the \n.
        assert_eq!(ring.lock().unwrap().len(), 1);

        // Three separate FfmpegProgress events, one per \r-segment.
        let first = rx.recv().await.expect("first event from segment 1");
        assert_eq!(
            first.media_time_ms, 0,
            "first segment: time=00:00:00.00 → 0 ms"
        );
        let second = rx.recv().await.expect("second event from segment 2");
        assert_eq!(
            second.media_time_ms, 1_000,
            "second segment: time=00:00:01.00 → 1000 ms"
        );
        let third = rx.recv().await.expect("third event from segment 3");
        assert_eq!(
            third.media_time_ms, 2_000,
            "third segment: time=00:00:02.00 → 2000 ms"
        );
        // No further events.
        assert!(
            rx.try_recv().is_err(),
            "no more events expected after three segments"
        );
    }
}
