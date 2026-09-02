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
const PROCESSING: &str = r#"{ "data": [{ "id": "5", "status": "PROCESSING" }]}"#;
// A church page's ever-present "next Sunday" scheduled broadcast, with NO live
// object — must NOT mask the silent-discard RED.
const ONLY_SCHEDULED: &str = r#"{ "data": [{ "id": "6", "status": "SCHEDULED_UNPUBLISHED" }]}"#;
const SCHEDULED_LIVE_HEALTHY: &str = r#"{ "data": [{
  "id": "7", "status": "SCHEDULED_LIVE",
  "ingest_streams": { "data": [{ "is_master": true,
    "stream_health": { "video_bitrate": 3000000.0, "video_framerate": 25.0,
                       "video_width": 1280, "video_height": 720 } }]}
}]}"#;

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
fn unpublished_while_delivering_is_bad_not_masked() {
    // While we are pushing, a non-LIVE UNPUBLISHED draft means FB is not
    // ingesting our stream — RED, not a soothing grey.
    let h = classify_fb_health(&videos_from(UNPUBLISHED));
    assert_eq!(h.status, "NO_LIVE_VIDEO");
    assert_eq!(h.health, "bad");
}

#[test]
fn processing_is_nodata_transient() {
    let h = classify_fb_health(&videos_from(PROCESSING));
    assert_eq!(h.status, "PROCESSING");
    assert_eq!(h.health, "noData");
}

#[test]
fn scheduled_broadcast_alone_does_not_mask_red() {
    // The #166 review case: a church page's standing "next Sunday" scheduled
    // broadcast must not turn the silent-discard failure grey.
    let h = classify_fb_health(&videos_from(ONLY_SCHEDULED));
    assert_eq!(h.status, "NO_LIVE_VIDEO");
    assert_eq!(h.health, "bad");
}

#[test]
fn scheduled_live_with_ingest_is_good() {
    // SCHEDULED_LIVE = a scheduled broadcast that IS live now — inspect ingest.
    let h = classify_fb_health(&videos_from(SCHEDULED_LIVE_HEALTHY));
    assert_eq!(h.health, "good");
    assert_eq!(h.video_bitrate_kbps, Some(3000));
    assert_eq!(h.resolution.as_deref(), Some("1280x720"));
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
async fn attach_surfaces_unconfigured_when_token_or_page_empty() {
    // attach_fb_health is reached only when enabled; an empty token/page then is
    // a real misconfig → "unconfigured" (the call site keeps it dark otherwise).
    let fb = rs_core::config::FacebookConfig::default();
    let mut m = super::test_metrics();
    attach_fb_health(&fb, &mut m).await;
    let h = m.facebook_health.unwrap();
    assert_eq!(h.status, "unconfigured");
    assert_eq!(h.health, "unknown");
    assert_eq!(h.error.as_deref(), Some("fb_not_configured"));
}

// Wiremock-backed tests of the attach/TTL-cache/audit path (parity with
// yt_health_cache_tests). FB_GRAPH_API_BASE is process-global, so serialize.
static FB_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// Sets FB_GRAPH_API_BASE to the mock server and returns an enabled config.
fn fb_cfg(base: &str) -> rs_core::config::FacebookConfig {
    unsafe { std::env::set_var("FB_GRAPH_API_BASE", base) };
    rs_core::config::FacebookConfig {
        enabled: true,
        page_id: "163104934022649".into(),
        page_access_token: "tok".into(),
        api_version: "v21.0".into(),
    }
}

#[tokio::test]
async fn cached_probe_hits_graph_once_within_ttl() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let _guard = FB_ENV_LOCK.lock().await;
    super::clear_fb_health_cache_for_test();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v21.0/163104934022649/live_videos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "data": [] })))
        .expect(1) // second attach must be served from cache
        .mount(&server)
        .await;
    let fb = fb_cfg(&server.uri());

    let mut m1 = super::test_metrics();
    attach_fb_health_cached(&fb, 4242, "fb1", &mut m1, None).await;
    let mut m2 = super::test_metrics();
    attach_fb_health_cached(&fb, 4242, "fb1", &mut m2, None).await;
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    assert_eq!(m1.facebook_health.as_ref().unwrap().health, "bad");
    assert_eq!(m2.facebook_health.as_ref().unwrap().health, "bad");
    // wiremock verifies expect(1) on drop.
}

#[tokio::test]
async fn cached_probe_emits_audit_on_first_bad_observation() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let _guard = FB_ENV_LOCK.lock().await;
    super::clear_fb_health_cache_for_test();
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v21.0/163104934022649/live_videos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({ "data": [] })))
        .mount(&server)
        .await;
    let fb = fb_cfg(&server.uri());

    let (tx, mut rx) = tokio::sync::mpsc::channel(4);
    let mut m = super::test_metrics();
    attach_fb_health_cached(&fb, 4343, "fb-audit", &mut m, Some(&tx)).await;
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    let row = rx
        .try_recv()
        .expect("a None->bad transition must emit an audit row");
    assert_eq!(row.action, rs_core::audit::Action::FacebookStatusChanged);
    assert_eq!(row.endpoint.as_deref(), Some("fb-audit"));
}

#[tokio::test]
async fn expired_token_maps_to_oauth_invalid() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let _guard = FB_ENV_LOCK.lock().await;
    super::clear_fb_health_cache_for_test();
    let server = MockServer::start().await;
    // Graph returns HTTP 400 + code 190 for an expired token — the mapping must
    // key on the code, not the status.
    Mock::given(method("GET"))
        .and(path("/v21.0/163104934022649/live_videos"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": { "message": "Session has expired", "code": 190 }
        })))
        .mount(&server)
        .await;
    let fb = fb_cfg(&server.uri());

    let mut m = super::test_metrics();
    attach_fb_health(&fb, &mut m).await;
    unsafe { std::env::remove_var("FB_GRAPH_API_BASE") };

    let h = m.facebook_health.unwrap();
    assert_eq!(h.health, "unknown");
    assert_eq!(h.error.as_deref(), Some("oauth_invalid"));
}
