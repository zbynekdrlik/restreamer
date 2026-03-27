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
/// Uses -re for pacing (same as other FLV paths).
fn build_yt_hls_args(stream_key: &str) -> Vec<String> {
    let output_url = format!(
        "https://a.upload.youtube.com/http_upload_hls?cid={stream_key}&copy=0&file=out1248.ts"
    );
    vec![
        "-re".into(),
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
        // -re: read input at native frame rate (based on FLV timestamps).
        // This makes ffmpeg pace output to match the original stream timing.
        // Without it, ffmpeg would blast all buffered chunks instantly.
        "-re".into(),
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
const STDERR_BUFFER_SIZE: usize = 5;

/// Managed ffmpeg process with stdin pipe for writing data.
pub struct FfmpegProcess {
    child: Child,
    service_type: ServiceType,
    alias: String,
    stderr_lines: Arc<Mutex<VecDeque<String>>>,
}

impl FfmpegProcess {
    /// Spawn a new ffmpeg process for the given service type.
    pub fn spawn(
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
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
        let alias_clone = alias.to_string();
        let stderr_lines: Arc<Mutex<VecDeque<String>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(STDERR_BUFFER_SIZE)));
        let stderr_lines_clone = Arc::clone(&stderr_lines);
        tokio::spawn(async move {
            if let Some(stderr) = stderr {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Ok(mut buf) = stderr_lines_clone.lock() {
                        if buf.len() >= STDERR_BUFFER_SIZE {
                            buf.pop_front();
                        }
                        buf.push_back(line);
                    }
                }
                tracing::debug!(alias = %alias_clone, "ffmpeg stderr drain task finished");
            }
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

    pub fn service_type(&self) -> ServiceType {
        self.service_type
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // Should use -re for pacing
        assert!(args.contains(&"-re".to_string()));
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
