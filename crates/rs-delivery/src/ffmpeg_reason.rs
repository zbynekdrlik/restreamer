//! Parse ffmpeg stderr tail into a ReasonClass and decide reconnect backoff.

use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasonClass {
    YoutubeRtmpClosed,
    FacebookTlsInvalidated,
    RemoteBrokenPipe,
    NetworkTimeout,
    InvalidInput,
    S3FetchError,
    ProcessKilled,
    Unknown,
}

/// Classify the last portion of ffmpeg stderr into a reason class.
///
/// `service_type` is one of "YT_RTMP", "YT_HLS", "FB", "CUSTOM_RTMP", etc.
pub fn classify(service_type: &str, stderr_tail: &str) -> ReasonClass {
    // Cheap: look at the last 4 KB only.
    let start = stderr_tail.len().saturating_sub(4096);
    let tail = &stderr_tail[start..];

    if tail.contains("rs-delivery: killed") {
        return ReasonClass::ProcessKilled;
    }

    if tail.contains("TLS fatal alert") || tail.contains("session has been invalidated") {
        return ReasonClass::FacebookTlsInvalidated;
    }

    if tail.contains("Error submitting a packet to the muxer: Broken pipe")
        || tail.contains("IO error: Broken pipe")
        || tail.contains("Error writing trailer: Broken pipe")
    {
        return if service_type.starts_with("YT_") {
            ReasonClass::YoutubeRtmpClosed
        } else {
            ReasonClass::RemoteBrokenPipe
        };
    }

    if tail.contains("Connection timed out") {
        return ReasonClass::NetworkTimeout;
    }
    if tail.contains("Invalid data found") || tail.contains("No start code") {
        return ReasonClass::InvalidInput;
    }
    if tail.contains("rs-delivery: S3 fetch failed") {
        return ReasonClass::S3FetchError;
    }

    ReasonClass::Unknown
}

/// Minimum wait before next restart, given reason + consecutive count in this class.
pub fn reconnect_floor(class: ReasonClass, consecutive: u32) -> Duration {
    use ReasonClass::*;
    match class {
        // Never restart — caller suppresses.
        ProcessKilled => Duration::from_secs(u64::MAX),
        YoutubeRtmpClosed | FacebookTlsInvalidated | RemoteBrokenPipe => {
            // 30s * 2^consecutive, capped at 5 min.
            let base: u64 = 30;
            let mul = 2u64.saturating_pow(consecutive.min(10));
            Duration::from_secs(base.saturating_mul(mul).min(300))
        }
        NetworkTimeout => Duration::from_secs(10),
        InvalidInput => Duration::from_secs(1),
        S3FetchError => Duration::from_secs(5),
        Unknown => Duration::from_secs(15),
    }
}

/// Pick a single display-worthy line from stderr tail.
/// Skips progress lines (size=…, frame=…) and banner lines.
/// Returns the last line containing error-like keywords.
pub fn pick_last_error_line(stderr_tail: &str) -> Option<String> {
    stderr_tail
        .lines()
        .rev()
        .filter(|l| {
            let l = l.trim();
            !l.is_empty()
                && !l.starts_with("size=")
                && !l.starts_with("frame=")
                && !l.starts_with("ffmpeg version ")
                && !l.starts_with("  built with ")
                && !l.starts_with("  configuration: ")
                && !l.starts_with("  lib")
        })
        .find(|l| {
            let l = l.to_ascii_lowercase();
            l.contains("error")
                || l.contains("broken pipe")
                || l.contains("fatal")
                || l.contains("invalid")
                || l.contains("failed")
                || l.contains("timeout")
        })
        .map(|s| s.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_floor_remote_close_starts_at_30s() {
        assert_eq!(
            reconnect_floor(ReasonClass::YoutubeRtmpClosed, 0),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn reconnect_floor_remote_close_doubles_and_caps() {
        assert_eq!(
            reconnect_floor(ReasonClass::YoutubeRtmpClosed, 1),
            Duration::from_secs(60)
        );
        assert_eq!(
            reconnect_floor(ReasonClass::YoutubeRtmpClosed, 2),
            Duration::from_secs(120)
        );
        assert_eq!(
            reconnect_floor(ReasonClass::YoutubeRtmpClosed, 3),
            Duration::from_secs(240)
        );
        assert_eq!(
            reconnect_floor(ReasonClass::YoutubeRtmpClosed, 10),
            Duration::from_secs(300)
        );
        assert_eq!(
            reconnect_floor(ReasonClass::YoutubeRtmpClosed, 100),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn reconnect_floor_network_timeout_fixed_10s() {
        assert_eq!(
            reconnect_floor(ReasonClass::NetworkTimeout, 0),
            Duration::from_secs(10)
        );
        assert_eq!(
            reconnect_floor(ReasonClass::NetworkTimeout, 5),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn reconnect_floor_process_killed_infinite() {
        assert_eq!(
            reconnect_floor(ReasonClass::ProcessKilled, 0),
            Duration::from_secs(u64::MAX)
        );
    }

    #[test]
    fn pick_last_error_line_skips_progress() {
        let s = "size= 1234kB time=00:00:10 bitrate=1000kbits/s\n\
                 [aost#0:1/copy] Error submitting a packet to the muxer: Broken pipe\n\
                 size= 1235kB time=00:00:11 bitrate=999kbits/s";
        assert_eq!(
            pick_last_error_line(s).unwrap(),
            "[aost#0:1/copy] Error submitting a packet to the muxer: Broken pipe"
        );
    }

    #[test]
    fn pick_last_error_line_none_when_no_error() {
        let s = "size= 1kB time=00:00:01 bitrate=0";
        assert_eq!(pick_last_error_line(s), None);
    }

    #[test]
    fn classify_unknown_empty_stderr() {
        assert_eq!(classify("YT_RTMP", ""), ReasonClass::Unknown);
    }

    #[test]
    fn classify_process_killed_marker() {
        assert_eq!(
            classify(
                "YT_RTMP",
                "some random lines\nrs-delivery: killed\nlast line"
            ),
            ReasonClass::ProcessKilled
        );
    }

    #[test]
    fn classify_invalid_input() {
        assert_eq!(
            classify("YT_RTMP", "Invalid data found when processing input"),
            ReasonClass::InvalidInput
        );
    }
}
