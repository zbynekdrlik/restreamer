//! `build_rtmp_url` — plain RTMP/RTMPS upstream URL construction for the
//! Rust `RtmpPusher`. Extracted from `endpoint_task.rs` to keep that file
//! under the 1000-line CI cap (#232). Mirrors `rs_ffmpeg::build_ffmpeg_args`
//! URL construction so the pusher connects to the same upstream ffmpeg would.

use rs_ffmpeg::ServiceType;

/// Build the plain RTMP URL for a given service type and stream key.
pub(crate) fn build_rtmp_url(service_type: ServiceType, stream_key: &str) -> String {
    match service_type {
        ServiceType::YtRtmp => format!("rtmp://a.rtmp.youtube.com/live2/{stream_key}"),
        ServiceType::Facebook => {
            format!("rtmps://live-api-s.facebook.com:443/rtmp/{stream_key}")
        }
        ServiceType::Vimeo => {
            format!("rtmps://rtmp-global.cloud.vimeo.com:443/live/{stream_key}")
        }
        ServiceType::Instagram => {
            format!("rtmps://live-upload.instagram.com:443/rtmp/{stream_key}")
        }
        // TestFile has no upstream — use a local test address.
        ServiceType::TestFile => format!("rtmp://127.0.0.1:1935/live/{stream_key}"),
    }
}

/// Test-accessible re-export so unit tests can call `build_rtmp_url` without
/// making it part of the public API.
#[cfg(test)]
pub(crate) fn build_rtmp_url_pub(service_type: ServiceType, stream_key: &str) -> String {
    build_rtmp_url(service_type, stream_key)
}
