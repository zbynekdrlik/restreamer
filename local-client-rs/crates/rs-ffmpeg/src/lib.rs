/// FFmpeg process management for streaming endpoints.
///
/// Spawns and manages ffmpeg processes for different streaming service types.
/// Each service type (YouTube HLS, Facebook, etc.) has a specific ffmpeg
/// command configuration.
///
/// Ported from Python delivering-service endpoints.py ffmpeg construction.
use std::path::PathBuf;
use std::process::Stdio;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
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
pub fn build_ffmpeg_args(service_type: ServiceType, stream_key: &str, alias: &str) -> Vec<String> {
    match service_type {
        ServiceType::YtHls => build_yt_hls_args(stream_key),
        ServiceType::Facebook => build_rtmps_args(&format!(
            "rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}"
        )),
        ServiceType::YtRtmp => build_yt_rtmp_args(stream_key),
        ServiceType::Vimeo => build_rtmps_args(&format!(
            "rtmps://rtmp-global.cloud.vimeo.com:443/live/{stream_key}"
        )),
        ServiceType::Instagram => build_rtmps_args(&format!(
            "rtmps://live-upload.instagram.com:443/rtmp/{stream_key}"
        )),
        ServiceType::TestFile => build_test_file_args(alias),
    }
}

fn build_yt_hls_args(stream_key: &str) -> Vec<String> {
    let output_url = format!(
        "https://a.upload.youtube.com/http_upload_hls?cid={stream_key}&copy=0&file=out1248.ts"
    );
    vec![
        "-readrate".into(),
        "1.00".into(),
        "-f".into(),
        "mpegts".into(),
        "-loglevel".into(),
        "info".into(),
        "-fflags".into(),
        "+genpts+discardcorrupt".into(),
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

fn build_rtmps_args(url: &str) -> Vec<String> {
    vec![
        "-readrate".into(),
        "1.00".into(),
        "-f".into(),
        "mpegts".into(),
        "-loglevel".into(),
        "info".into(),
        "-fflags".into(),
        "+genpts+discardcorrupt".into(),
        "-i".into(),
        "pipe:".into(),
        "-f".into(),
        "flv".into(),
        "-c".into(),
        "copy".into(),
        url.to_string(),
    ]
}

fn build_yt_rtmp_args(stream_key: &str) -> Vec<String> {
    let url = format!("rtmp://a.rtmp.youtube.com/live2/{stream_key}");
    vec![
        "-readrate".into(),
        "1.00".into(),
        "-f".into(),
        "mpegts".into(),
        "-loglevel".into(),
        "info".into(),
        "-fflags".into(),
        "+genpts+discardcorrupt".into(),
        "-i".into(),
        "pipe:".into(),
        "-vf".into(),
        "yadif".into(),
        "-re".into(),
        "-f".into(),
        "flv".into(),
        "-vcodec".into(),
        "copy".into(),
        "-acodec".into(),
        "aac".into(),
        "-ab".into(),
        "160k".into(),
        "-ac".into(),
        "2".into(),
        "-ar".into(),
        "48000".into(),
        url,
    ]
}

fn build_test_file_args(alias: &str) -> Vec<String> {
    let output_dir = std::env::var("RESTREAMER_TEST_OUTPUT_DIR")
        .unwrap_or_else(|_| std::env::temp_dir().to_string_lossy().to_string());
    let safe_alias = alias.replace([' ', '/'], "_");
    let output_path = PathBuf::from(&output_dir)
        .join(format!("restreamer_test_{safe_alias}.ts"))
        .to_string_lossy()
        .to_string();
    vec![
        "-f".into(),
        "mpegts".into(),
        "-loglevel".into(),
        "info".into(),
        "-i".into(),
        "pipe:".into(),
        "-f".into(),
        "mpegts".into(),
        "-c".into(),
        "copy".into(),
        output_path,
    ]
}

/// Managed ffmpeg process with stdin pipe for writing data.
pub struct FfmpegProcess {
    child: Child,
    service_type: ServiceType,
    alias: String,
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

        let child = Command::new("ffmpeg")
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;

        Ok(Self {
            child,
            service_type,
            alias: alias.to_string(),
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
        assert!(args.contains(&"yadif".to_string()));
        assert!(args.contains(&"aac".to_string()));
        assert!(args.contains(&"160k".to_string()));
        assert!(args.contains(&"48000".to_string()));
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
                .any(|a| a.contains("restreamer_test_Test_Stream.ts"))
        );
        assert!(args.contains(&"mpegts".to_string()));
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
    fn all_commands_have_mpegts_input() {
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
            // Check -f mpegts appears as input format
            let f_idx = args.iter().position(|a| a == "-f").unwrap();
            assert_eq!(
                args[f_idx + 1],
                "mpegts",
                "{st} missing mpegts input format"
            );
        }
    }
}
