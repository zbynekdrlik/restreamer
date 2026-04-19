//! Fixture-driven classification tests using real stderr captures from the
//! 2026-04-19 live-event failure (instance 654, delivery_restart_log rows 14-29).

use rs_delivery::ffmpeg_reason::{ReasonClass, classify};
use std::fs;

fn read_fixture(name: &str) -> String {
    let path = format!("tests/ffmpeg_reason_fixtures/{name}");
    fs::read_to_string(&path).unwrap_or_else(|_| panic!("fixture not found: {path}"))
}

#[test]
fn classify_youtube_rtmp_broken_pipe_wave1_control_stream() {
    let stderr = read_fixture("14_Control_Stream_SNV.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_youtube_rtmp_broken_pipe_wave1_yt_nlch_4k() {
    let stderr = read_fixture("15_YT_NLCH_4K.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_youtube_rtmp_broken_pipe_wave1_yt_nlw_4k() {
    let stderr = read_fixture("16_YT_NLW_4k.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_youtube_rtmp_broken_pipe_wave2_yt_nlch_4k() {
    let stderr = read_fixture("17_YT_NLCH_4K.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_youtube_rtmp_broken_pipe_wave2_yt_nlw_4k() {
    let stderr = read_fixture("18_YT_NLW_4k.txt");
    assert_eq!(classify("YT_RTMP", &stderr), ReasonClass::YoutubeRtmpClosed);
}

#[test]
fn classify_facebook_tls_fatal_alert_fb_zbynek() {
    let stderr = read_fixture("19_FB_Zbynek.txt");
    assert_eq!(classify("FB", &stderr), ReasonClass::FacebookTlsInvalidated);
}

#[test]
fn classify_facebook_tls_fatal_alert_fb_newlevel() {
    let stderr = read_fixture("23_FB_NewLevel.txt");
    assert_eq!(classify("FB", &stderr), ReasonClass::FacebookTlsInvalidated);
}

#[test]
fn classify_remote_broken_pipe_for_non_yt_service() {
    // Same stderr content, but service_type is not YT_*: should be RemoteBrokenPipe.
    let stderr = read_fixture("14_Control_Stream_SNV.txt");
    assert_eq!(
        classify("CUSTOM_RTMP", &stderr),
        ReasonClass::RemoteBrokenPipe
    );
}
