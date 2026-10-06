//! Manage-scope client (#357), against wiremock. Every test builds its own
//! client with explicit endpoints, so there is no process-global URL override
//! and no lock (`.claude/rules/mutation-killable-code.md`).

use super::*;
use wiremock::matchers::{
    body_partial_json, body_string_contains, header, method, path, query_param,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

const REFRESH: &str = "rt-fake-refresh-value";
const SECRET: &str = "cs-fake-client-secret";

fn creds() -> ManageCredentials {
    ManageCredentials {
        refresh_token: REFRESH.to_string(),
        client_id: "cid".to_string(),
        client_secret: SECRET.to_string(),
    }
}

async fn server_with_token(expires_in: u64, expect: u64) -> MockServer {
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains(format!("refresh_token={REFRESH}")))
        .and(body_string_contains("client_id=cid"))
        .and(body_string_contains(format!("client_secret={SECRET}")))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"access_token": "AT1", "expires_in": expires_in})),
        )
        .expect(expect)
        .mount(&s)
        .await;
    s
}

fn client(s: &MockServer) -> ManageClient {
    ManageClient::with_endpoints(
        creds(),
        &format!("{}/yt/", s.uri()),
        &format!("{}/token", s.uri()),
    )
}

/// Each call writes a NEW file, so several cases can coexist in one dir.
fn write_oauth(dir: &tempfile::TempDir, content: &str) -> std::path::PathBuf {
    let n = std::fs::read_dir(dir.path()).unwrap().count();
    let p = dir.path().join(format!("oauth-{n}.json"));
    std::fs::write(&p, content).unwrap();
    p
}

// ---- credentials ----------------------------------------------------------

#[test]
fn grants_manage_scope_needs_the_exact_scope_token() {
    assert!(grants_manage_scope(MANAGE_SCOPE));
    assert!(grants_manage_scope(&format!("openid {MANAGE_SCOPE} email")));
    assert!(!grants_manage_scope(
        "https://www.googleapis.com/auth/youtube.readonly"
    ));
    assert!(!grants_manage_scope(""));
}

#[test]
fn credentials_load_from_a_bom_prefixed_file() {
    let dir = tempfile::tempdir().unwrap();
    let p = write_oauth(
        &dir,
        &format!("\u{FEFF}{{\"refresh_token\":\" {REFRESH} \",\"scope\":\"{MANAGE_SCOPE}\"}}"),
    );
    let c = ManageCredentials::from_oauth_file(&p, "cid", SECRET).unwrap();
    assert_eq!(c.refresh_token, REFRESH);
    assert_eq!(c.client_id, "cid");
    assert_eq!(c.client_secret, SECRET);
}

#[test]
fn credentials_refuse_every_unusable_file_without_quoting_it() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("absent.json");
    let cases: Vec<(std::path::PathBuf, &str, &str, &str)> = vec![
        (missing, "cid", SECRET, "cannot read"),
        (
            write_oauth(&dir, &format!("not json {REFRESH}")),
            "cid",
            SECRET,
            "not valid JSON",
        ),
        (
            write_oauth(&dir, &format!("{{\"scope\":\"{MANAGE_SCOPE}\"}}")),
            "cid",
            SECRET,
            "no refresh_token",
        ),
        (
            write_oauth(
                &dir,
                &format!(
                    "{{\"refresh_token\":\"{REFRESH}\",\"scope\":\"https://www.googleapis.com/auth/youtube.readonly\"}}"
                ),
            ),
            "cid",
            SECRET,
            "lacks",
        ),
        (
            write_oauth(
                &dir,
                &format!("{{\"refresh_token\":\"{REFRESH}\",\"scope\":\"{MANAGE_SCOPE}\"}}"),
            ),
            "",
            SECRET,
            "not configured",
        ),
        (
            write_oauth(
                &dir,
                &format!("{{\"refresh_token\":\"{REFRESH}\",\"scope\":\"{MANAGE_SCOPE}\"}}"),
            ),
            "cid",
            "",
            "not configured",
        ),
    ];
    for (p, id, secret, expected) in cases {
        let err = ManageCredentials::from_oauth_file(&p, id, secret)
            .expect_err(expected)
            .to_string();
        assert!(
            err.contains(expected),
            "{err:?} should mention {expected:?}"
        );
        assert!(
            !err.contains(REFRESH),
            "error leaked the refresh token: {err}"
        );
    }
}

#[test]
fn credentials_debug_hides_the_secrets() {
    let shown = format!("{:?}", creds());
    assert!(shown.contains("cid"));
    assert!(!shown.contains(REFRESH));
    assert!(!shown.contains(SECRET));
}

// ---- pure helpers -----------------------------------------------------------

#[test]
fn api_error_message_prefers_reason_then_message() {
    let both =
        r#"{"error":{"message":"Invalid transition","errors":[{"reason":"invalidTransition"}]}}"#;
    assert_eq!(
        api_error_message(both),
        "invalidTransition: Invalid transition"
    );
    assert_eq!(api_error_message(r#"{"error":{"message":"Nope"}}"#), "Nope");
    assert_eq!(
        api_error_message(r#"{"error":{"errors":[{"reason":"quotaExceeded"}]}}"#),
        "quotaExceeded: "
    );
    assert_eq!(api_error_message("<html>"), "unparsable error body");
}

#[test]
fn redundant_transition_is_recognised_and_nothing_else() {
    let redundant = YouTubeError::Api {
        status: 403,
        message: "redundantTransition: already live".to_string(),
    };
    let invalid = YouTubeError::Api {
        status: 403,
        message: "invalidTransition: no".to_string(),
    };
    assert!(is_redundant_transition(&redundant));
    assert!(!is_redundant_transition(&invalid));
    assert!(!is_redundant_transition(&YouTubeError::Other(
        "redundantTransition".to_string()
    )));
}

#[test]
fn token_is_fresh_only_strictly_before_the_refresh_point() {
    let t = Instant::now();
    assert!(token_is_fresh(t, t + Duration::from_millis(1)));
    assert!(!token_is_fresh(t, t));
    assert!(!token_is_fresh(t + Duration::from_millis(1), t));
}

#[test]
fn transition_strings_match_the_api() {
    assert_eq!(BroadcastTransition::Live.as_str(), "live");
    assert_eq!(BroadcastTransition::Complete.as_str(), "complete");
}

// ---- token refresh ------------------------------------------------------------

#[tokio::test]
async fn the_access_token_is_refreshed_once_and_reused() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("GET"))
        .and(path("/yt/videos"))
        .and(header("authorization", "Bearer AT1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": []})))
        .expect(2)
        .mount(&s)
        .await;
    let c = client(&s);
    assert_eq!(c.vod_status("v").await.unwrap(), VodStatus::default());
    assert_eq!(c.vod_status("v").await.unwrap(), VodStatus::default());
}

#[tokio::test]
async fn a_token_shorter_lived_than_the_margin_is_refreshed_every_call() {
    let s = server_with_token(30, 2).await;
    Mock::given(method("GET"))
        .and(path("/yt/videos"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": []})))
        .mount(&s)
        .await;
    let c = client(&s);
    c.vod_status("v").await.unwrap();
    c.vod_status("v").await.unwrap();
}

#[tokio::test]
async fn a_failed_refresh_reports_only_the_error_code() {
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "invalid_grant",
            "error_description": format!("Bad token {REFRESH}")
        })))
        .mount(&s)
        .await;
    let err = client(&s).stream_status("x").await.unwrap_err();
    let text = err.to_string();
    assert!(matches!(err, YouTubeError::TokenExpired(_)), "{text}");
    assert!(text.contains("HTTP 400 invalid_grant"), "{text}");
    assert!(!text.contains(REFRESH), "{text}");
}

#[tokio::test]
async fn a_refresh_response_without_an_access_token_is_an_error() {
    let s = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"expires_in": 3600})))
        .mount(&s)
        .await;
    let err = client(&s).stream_status("x").await.unwrap_err();
    assert!(matches!(err, YouTubeError::OAuth(_)), "{err}");
}

#[tokio::test]
async fn a_401_refreshes_the_token_and_retries_once() {
    let s = server_with_token(3600, 2).await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .respond_with(ResponseTemplate::new(401))
        .up_to_n_times(1)
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"status": {"streamStatus": "active"}}]
        })))
        .mount(&s)
        .await;
    let c = client(&s);
    assert_eq!(
        c.stream_status("st").await.unwrap().as_deref(),
        Some("active")
    );
    assert_eq!(c.units_used(), 2, "both attempts are charged");
}

#[tokio::test]
async fn a_second_401_is_returned() {
    let s = server_with_token(3600, 2).await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({"error": {"message": "x"}})))
        .expect(2)
        .mount(&s)
        .await;
    let err = client(&s).stream_status("st").await.unwrap_err();
    assert!(
        matches!(err, YouTubeError::Api { status: 401, .. }),
        "{err}"
    );
}

// ---- Data API calls -----------------------------------------------------------

#[tokio::test]
async fn find_stream_by_title_pages_until_it_finds_the_exact_title() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .and(query_param("mine", "true"))
        .and(query_param("pageToken", "P2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": "s-other", "snippet": {"title": "e2e rtmp copy"}},
                {"id": "s-e2e", "snippet": {"title": "e2e rtmp"},
                 "contentDetails": {"isReusable": true},
                 "status": {"streamStatus": "inactive"}}
            ]
        })))
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .and(query_param("part", "id,snippet,contentDetails,status"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": "s-hls", "snippet": {"title": "e2e hls"}}],
            "nextPageToken": "P2"
        })))
        .mount(&s)
        .await;
    let c = client(&s);
    assert_eq!(
        c.find_stream_by_title("e2e rtmp").await.unwrap(),
        Some(ManagedStream {
            id: "s-e2e".to_string(),
            is_reusable: true,
            stream_status: "inactive".to_string(),
        })
    );
    assert_eq!(c.units_used(), 2);
}

#[tokio::test]
async fn find_stream_by_title_reports_absence_and_non_reusable_streams() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": "s1", "snippet": {"title": "one-off"},
                       "contentDetails": {"isReusable": false}}],
            "nextPageToken": ""
        })))
        .mount(&s)
        .await;
    let c = client(&s);
    assert_eq!(c.find_stream_by_title("e2e rtmp").await.unwrap(), None);
    assert_eq!(c.units_used(), 1, "an empty nextPageToken ends the lookup");
    let one_off = c.find_stream_by_title("one-off").await.unwrap().unwrap();
    assert!(!one_off.is_reusable);
    assert_eq!(one_off.stream_status, "");
}

#[tokio::test]
async fn insert_broadcast_creates_an_unlisted_manual_broadcast() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts"))
        .and(query_param("part", "id,snippet,status,contentDetails"))
        .and(body_partial_json(json!({
            "snippet": {"title": "A/V gate", "scheduledStartTime": "2026-10-06T10:00:00Z"},
            "status": {"privacyStatus": "unlisted"},
            "contentDetails": {
                "enableAutoStart": false,
                "enableAutoStop": false,
                "monitorStream": {"enableMonitorStream": false}
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "bc-1"})))
        .mount(&s)
        .await;
    let c = client(&s);
    assert_eq!(
        c.insert_broadcast("A/V gate", "2026-10-06T10:00:00Z")
            .await
            .unwrap(),
        "bc-1"
    );
    assert_eq!(c.units_used(), units::INSERT);
}

#[tokio::test]
async fn insert_broadcast_without_an_id_is_an_error() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": ""})))
        .mount(&s)
        .await;
    assert!(client(&s).insert_broadcast("t", "x").await.is_err());
}

#[tokio::test]
async fn bind_sends_both_ids() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/bind"))
        .and(query_param("id", "bc-1"))
        .and(query_param("streamId", "s-e2e"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "bc-1"})))
        .expect(1)
        .mount(&s)
        .await;
    let c = client(&s);
    c.bind_broadcast("bc-1", "s-e2e").await.unwrap();
    assert_eq!(c.units_used(), units::BIND);
}

#[tokio::test]
async fn transition_succeeds_also_when_already_in_that_state() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/transition"))
        .and(query_param("broadcastStatus", "live"))
        .and(query_param("id", "bc-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&s)
        .await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/transition"))
        .and(query_param("broadcastStatus", "complete"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"message": "Redundant", "errors": [{"reason": "redundantTransition"}]}
        })))
        .mount(&s)
        .await;
    let c = client(&s);
    c.transition_broadcast("bc-1", BroadcastTransition::Live)
        .await
        .unwrap();
    c.transition_broadcast("bc-1", BroadcastTransition::Complete)
        .await
        .unwrap();
    assert_eq!(c.units_used(), 2 * units::TRANSITION);
}

#[tokio::test]
async fn an_invalid_transition_is_an_api_error_with_its_reason() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/transition"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"message": "Invalid transition", "errors": [{"reason": "invalidTransition"}]}
        })))
        .mount(&s)
        .await;
    let err = client(&s)
        .transition_broadcast("bc-1", BroadcastTransition::Live)
        .await
        .unwrap_err();
    match err {
        YouTubeError::Api { status, message } => {
            assert_eq!(status, 403);
            assert_eq!(message, "invalidTransition: Invalid transition");
        }
        other => panic!("unexpected {other}"),
    }
}

#[tokio::test]
async fn status_reads_return_the_value_or_none() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("GET"))
        .and(path("/yt/liveBroadcasts"))
        .and(query_param("id", "bc-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"status": {"lifeCycleStatus": "live"}}]
        })))
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/liveBroadcasts"))
        .and(query_param("id", "gone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": []})))
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/videos"))
        .and(query_param("part", "processingDetails,status"))
        .and(query_param("id", "bc-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"processingDetails": {"processingStatus": "succeeded"},
                       "status": {"uploadStatus": "processed"}}]
        })))
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/liveStreams"))
        .and(query_param("id", "gone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": []})))
        .mount(&s)
        .await;
    let c = client(&s);
    assert_eq!(
        c.broadcast_life_cycle("bc-1").await.unwrap().as_deref(),
        Some("live")
    );
    assert_eq!(c.broadcast_life_cycle("gone").await.unwrap(), None);
    assert_eq!(
        c.vod_status("bc-1").await.unwrap(),
        VodStatus {
            processing: Some("succeeded".to_string()),
            upload: Some("processed".to_string()),
        }
    );
    assert_eq!(c.stream_status("gone").await.unwrap(), None);
    assert_eq!(c.units_used(), 4);
}

#[test]
fn new_targets_the_real_google_endpoints() {
    let c = ManageClient::new(creds());
    assert_eq!(c.api_base, DEFAULT_API_BASE);
    assert_eq!(c.token_uri, DEFAULT_TOKEN_URI);
    assert_eq!(c.units_used(), 0);
}

#[tokio::test]
async fn an_attached_quota_tracker_refuses_a_call_it_cannot_pay() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/bind"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&s)
        .await;
    static BUCKET: std::sync::OnceLock<crate::quota::QuotaTracker> = std::sync::OnceLock::new();
    let bucket = BUCKET.get_or_init(|| crate::quota::QuotaTracker::new(60));
    let c = client(&s).with_quota_tracker(bucket);
    c.bind_broadcast("bc-1", "st").await.unwrap();
    let err = c.bind_broadcast("bc-1", "st").await.unwrap_err();
    assert!(err.to_string().contains("quota exhausted"), "{err}");
    assert_eq!(c.units_used(), units::BIND, "a refused call is not charged");
    assert_eq!(bucket.remaining(), 10);
}

#[tokio::test]
async fn completing_a_broadcast_is_never_refused_by_an_empty_bucket() {
    let s = server_with_token(3600, 1).await;
    Mock::given(method("POST"))
        .and(path("/yt/liveBroadcasts/transition"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(1)
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/yt/liveBroadcasts"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": []})))
        .expect(1)
        .mount(&s)
        .await;
    static EMPTY: std::sync::OnceLock<crate::quota::QuotaTracker> = std::sync::OnceLock::new();
    let bucket = EMPTY.get_or_init(|| crate::quota::QuotaTracker::new(10));
    let c = client(&s).with_quota_tracker(bucket);
    let err = c
        .transition_broadcast("bc-1", BroadcastTransition::Live)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("quota exhausted"), "{err}");
    c.transition_broadcast("bc-1", BroadcastTransition::Complete)
        .await
        .unwrap();
    assert_eq!(
        c.broadcast_life_cycle_for_teardown("bc-1").await.unwrap(),
        None
    );
    assert!(
        c.broadcast_life_cycle("bc-1").await.is_err(),
        "a readiness poll is refused by an empty bucket"
    );
    assert_eq!(c.units_used(), units::TRANSITION + units::LIST);
    assert_eq!(bucket.remaining(), 0, "the forced calls put it into debt");
}

#[test]
fn costs_say_whether_the_bucket_may_refuse() {
    assert_eq!(
        admit(3),
        Cost {
            units: 3,
            forced: false
        }
    );
    assert_eq!(
        forced(4),
        Cost {
            units: 4,
            forced: true
        }
    );
}
