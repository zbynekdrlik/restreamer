use super::rescue::*;

#[test]
fn build_rescue_ffmpeg_args_rtmp_endpoint() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/rescue.mp4",
        "rtmps://live-api-s.facebook.com:443/rtmp/key123",
        "flv",
        "FB-Test",
    );
    assert!(args.contains(&"-stream_loop".to_string()));
    assert!(args.contains(&"-1".to_string()));
    assert!(args.contains(&"-re".to_string()));
    let vf_idx = args.iter().position(|a| a == "-vf").unwrap();
    let vf_val = &args[vf_idx + 1];
    assert!(vf_val.contains("drawtext="));
    assert!(vf_val.contains("reload=1"));
    assert!(vf_val.contains("/tmp/rescue_FB-Test.txt"));
    assert!(args.last().unwrap().contains("facebook.com"));
}

#[test]
fn build_rescue_ffmpeg_args_hls_endpoint() {
    let args = build_rescue_ffmpeg_args(
        "https://s3.example.com/rescue.mp4",
        "https://a.upload.youtube.com/http_upload_hls?cid=key123&copy=0&file=out1248.ts",
        "hls",
        "YT-Test",
    );
    assert!(args.iter().any(|a| a == "hls"));
    assert!(args.iter().any(|a| a == "PUT"));
}

#[test]
fn format_countdown_warmup() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        95,
    );
    assert_eq!(text, "Stream starting ~ 1m 35s");
}

#[test]
fn format_countdown_buffer_empty() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::BufferEmpty,
        },
        30,
    );
    assert_eq!(text, "Stream recovering ~ 30s");
}

#[test]
fn format_countdown_zero() {
    let text = format_countdown_text(
        &DeliveryMode::Rescue {
            reason: RescueReason::Warmup,
        },
        0,
    );
    assert_eq!(text, "Stream starting soon");
}

#[test]
fn format_countdown_normal_mode_empty() {
    let text = format_countdown_text(&DeliveryMode::Normal, 120);
    assert_eq!(text, "");
}

#[test]
fn countdown_file_path_sanitizes() {
    assert_eq!(
        countdown_file_path("FB/Test Stream"),
        "/tmp/rescue_FB_Test_Stream.txt"
    );
}

#[test]
fn endpoint_url_youtube_hls() {
    let url = endpoint_url_for_service(rs_ffmpeg::ServiceType::YtHls, "test-key");
    assert!(url.contains("a.upload.youtube.com"));
    assert!(url.contains("test-key"));
}

#[test]
fn endpoint_url_facebook() {
    let url = endpoint_url_for_service(rs_ffmpeg::ServiceType::Facebook, "fb-key");
    assert!(url.contains("facebook.com"));
    assert!(url.contains("fb-key"));
}

#[test]
fn output_format_yt_hls_is_hls() {
    assert_eq!(
        output_format_for_service(rs_ffmpeg::ServiceType::YtHls),
        "hls"
    );
}

#[test]
fn output_format_facebook_is_flv() {
    assert_eq!(
        output_format_for_service(rs_ffmpeg::ServiceType::Facebook),
        "flv"
    );
}
