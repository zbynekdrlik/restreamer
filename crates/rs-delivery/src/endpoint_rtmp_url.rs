//! `build_rtmp_url` — plain RTMP/RTMPS upstream URL construction for the
//! Rust `RtmpPusher`. Extracted from `endpoint_task.rs` to keep that file
//! under the 1000-line CI cap (#232). Mirrors `rs_ffmpeg::build_ffmpeg_args`
//! URL construction so the pusher connects to the same upstream ffmpeg would.
//!
//! Compiled into BOTH the binary (`main.rs`) and the library (`lib.rs`)
//! targets: it depends only on `rs_ffmpeg` and the `test_file_sink` address
//! constant (also in both), and the library export lets the `tests/`
//! integration binaries dial the exact URL production dials (#192).

use rs_ffmpeg::ServiceType;

use crate::test_file_sink::TEST_FILE_SINK_ADDR;

/// Build the plain RTMP URL for a given service type and stream key.
pub fn build_rtmp_url(service_type: ServiceType, stream_key: &str) -> String {
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
        // TestFile has no upstream: the delivery binary's own loopback
        // accept-and-discard sink listens here (#192, `test_file_sink`).
        ServiceType::TestFile => format!("rtmp://{TEST_FILE_SINK_ADDR}/live/{stream_key}"),
    }
}
