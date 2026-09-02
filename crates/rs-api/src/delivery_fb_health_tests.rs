//! State-machine tests for `classify_fb_health` + the audit-transition helper.
//! Inputs are built from recorded, sanitised Graph API JSON (no tokens) so the
//! parser and the classifier are exercised together against real FB shapes.

use super::*;
use rs_facebook::live_video::LiveVideo;

/// Parse a `{ "data": [ ... ] }` fixture into the Vec the classifier consumes.
fn videos_from(json: &str) -> Vec<LiveVideo> {
    #[derive(serde::Deserialize)]
    struct Wrap {
        data: Vec<LiveVideo>,
    }
    serde_json::from_str::<Wrap>(json).unwrap().data
}

const LIVE_HEALTHY: &str = r#"{ "data": [{
  "id": "1", "status": "LIVE",
  "ingest_streams": { "data": [{
    "is_master": true,
    "stream_health": { "video_bitrate": 4500000.0, "video_framerate": 29.97,
                       "video_width": 1920, "video_height": 1080, "audio_bitrate": 128000.0 }
  }]}
}]}"#;

const LIVE_NO_MEDIA: &str = r#"{ "data": [{
  "id": "2", "status": "LIVE",
  "ingest_streams": { "data": [{ "is_master": true,
    "stream_health": { "video_bitrate": 0.0, "video_framerate": 0.0 } }]}
}]}"#;

const UNPUBLISHED: &str = r#"{ "data": [{ "id": "3", "status": "UNPUBLISHED" }]}"#;
const EMPTY_PAGE: &str = r#"{ "data": [] }"#;
const ONLY_VOD: &str = r#"{ "data": [{ "id": "4", "status": "VOD" }]}"#;

#[test]
fn live_with_measurable_ingest_is_good() {
    let h = classify_fb_health(&videos_from(LIVE_HEALTHY));
    assert_eq!(h.status, "LIVE");
    assert_eq!(h.health, "good");
    assert_eq!(h.video_bitrate_kbps, Some(4500));
    assert_eq!(h.resolution.as_deref(), Some("1920x1080"));
    assert_eq!(h.frame_rate.as_deref(), Some("29.97"));
    assert!(h.error.is_none());
}

#[test]
fn live_but_zero_media_is_bad() {
    let h = classify_fb_health(&videos_from(LIVE_NO_MEDIA));
    assert_eq!(h.status, "LIVE");
    assert_eq!(h.health, "bad");
    assert!(h.video_bitrate_kbps.is_none());
}

#[test]
fn unpublished_startup_is_nodata_not_alarming() {
    let h = classify_fb_health(&videos_from(UNPUBLISHED));
    assert_eq!(h.status, "UNPUBLISHED");
    assert_eq!(h.health, "noData");
}

#[test]
fn no_receiving_live_video_is_bad_red() {
    // The silent-discard case: we push bytes, FB has zero receiving live_video.
    let h = classify_fb_health(&videos_from(EMPTY_PAGE));
    assert_eq!(h.status, "NO_LIVE_VIDEO");
    assert_eq!(h.health, "bad");
}

#[test]
fn only_ended_or_vod_objects_is_also_no_live_video() {
    let h = classify_fb_health(&videos_from(ONLY_VOD));
    assert_eq!(h.status, "NO_LIVE_VIDEO");
    assert_eq!(h.health, "bad");
}

#[test]
fn live_wins_over_a_stale_unpublished_sibling() {
    let mut vids = videos_from(UNPUBLISHED);
    vids.extend(videos_from(LIVE_HEALTHY));
    let h = classify_fb_health(&vids);
    assert_eq!(h.health, "good");
}

#[test]
fn status_change_emits_only_on_transition() {
    assert!(status_changed_action(Some("good"), Some("good")).is_none());
    let (action, from, to) = status_changed_action(Some("good"), Some("bad")).unwrap();
    assert_eq!(action, rs_core::audit::Action::FacebookStatusChanged);
    assert_eq!(from.as_deref(), Some("good"));
    assert_eq!(to.as_deref(), Some("bad"));
    let (_, from, to) = status_changed_action(None, Some("noData")).unwrap();
    assert!(from.is_none());
    assert_eq!(to.as_deref(), Some("noData"));
}

#[test]
fn ttl_is_short_when_not_good() {
    let good = classify_fb_health(&videos_from(LIVE_HEALTHY));
    let bad = classify_fb_health(&videos_from(EMPTY_PAGE));
    assert_eq!(ttl_for_fb_health(&good), std::time::Duration::from_secs(60));
    assert_eq!(ttl_for_fb_health(&bad), std::time::Duration::from_secs(15));
}

#[tokio::test]
async fn attach_ships_dark_when_unconfigured() {
    let fb = rs_core::config::FacebookConfig::default(); // enabled=false
    let mut m = super::test_metrics();
    attach_fb_health(&fb, &mut m).await;
    let h = m.facebook_health.unwrap();
    assert_eq!(h.status, "unconfigured");
    assert_eq!(h.health, "unknown");
    assert_eq!(h.error.as_deref(), Some("fb_not_configured"));
}
