//! Device Code Flow contract: HTTP client against wiremock'd Google
//! endpoints + poll state machine.

use crate::device_flow::{
    PollDecision, PollResponse, poll_decision, poll_token, request_device_code,
};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn poll_decision_pending_continues() {
    let dec = poll_decision(&PollResponse::Pending);
    assert!(matches!(dec, PollDecision::Continue));
}

#[test]
fn poll_decision_slow_down_doubles_interval() {
    let dec = poll_decision(&PollResponse::SlowDown);
    assert!(matches!(dec, PollDecision::DoubleInterval));
}

#[test]
fn poll_decision_denied_is_terminal() {
    assert!(matches!(
        poll_decision(&PollResponse::Denied),
        PollDecision::TerminalDenied
    ));
}

#[test]
fn poll_decision_expired_is_terminal() {
    assert!(matches!(
        poll_decision(&PollResponse::Expired),
        PollDecision::TerminalExpired
    ));
}

#[test]
fn poll_decision_granted_is_terminal_with_tokens() {
    let dec = poll_decision(&PollResponse::Granted {
        access_token: "AT".into(),
        refresh_token: "RT".into(),
        expires_in: Some(3600),
        id_token: None,
    });
    match dec {
        PollDecision::TerminalGranted {
            access_token,
            refresh_token,
            ..
        } => {
            assert_eq!(access_token, "AT");
            assert_eq!(refresh_token, "RT");
        }
        other => panic!("expected TerminalGranted; got {:?}", other),
    }
}

#[tokio::test]
async fn request_device_code_parses_response() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/device/code"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"device_code":"D1","user_code":"USR-XYZ","verification_url":"https://www.google.com/device","expires_in":1800,"interval":5}"#
        ))
        .expect(1)
        .mount(&server)
        .await;
    let r = request_device_code(&server.uri(), "client_id", "scope1 scope2")
        .await
        .unwrap();
    assert_eq!(r.device_code, "D1");
    assert_eq!(r.user_code, "USR-XYZ");
    assert_eq!(r.verification_url, "https://www.google.com/device");
    assert_eq!(r.expires_in, 1800);
    assert_eq!(r.interval, 5);
}

#[tokio::test]
async fn poll_token_pending() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=urn"))
        .respond_with(
            ResponseTemplate::new(400).set_body_string(r#"{"error":"authorization_pending"}"#),
        )
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "client_id", "client_secret", "DEVICE")
        .await
        .unwrap();
    assert!(matches!(r, PollResponse::Pending));
}

#[tokio::test]
async fn poll_token_slow_down() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_string(r#"{"error":"slow_down"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    assert!(matches!(r, PollResponse::SlowDown));
}

#[tokio::test]
async fn poll_token_granted_parses_tokens() {
    let server = MockServer::start().await;
    Mock::given(method("POST")).and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(
            r#"{"access_token":"AT","refresh_token":"RT","expires_in":3600,"token_type":"Bearer","scope":"yt"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    match r {
        PollResponse::Granted {
            access_token,
            refresh_token,
            expires_in,
            ..
        } => {
            assert_eq!(access_token, "AT");
            assert_eq!(refresh_token, "RT");
            assert_eq!(expires_in, Some(3600));
        }
        other => panic!("expected Granted, got {:?}", other),
    }
}

#[tokio::test]
async fn poll_token_denied() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_string(r#"{"error":"access_denied"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    assert!(matches!(r, PollResponse::Denied));
}

#[tokio::test]
async fn poll_token_expired() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(400).set_body_string(r#"{"error":"expired_token"}"#))
        .mount(&server)
        .await;
    let r = poll_token(&server.uri(), "c", "s", "D").await.unwrap();
    assert!(matches!(r, PollResponse::Expired));
}
