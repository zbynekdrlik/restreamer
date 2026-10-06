//! Tests for `hetzner.rs` (moved out to keep the production file under the
//! 1000-line CI cap, #367).

use super::*;

#[test]
fn hetzner_client_new() {
    let client = HetznerClient::new("test-token");
    assert_eq!(client.api_token, "test-token");
    assert_eq!(client.base_url, API_BASE);
}

#[test]
fn hetzner_client_custom_base_url() {
    let client = HetznerClient::with_base_url("token", "http://localhost:8080");
    assert_eq!(client.base_url, "http://localhost:8080");
}

#[tokio::test]
async fn create_server_request_format() {
    // Test that the request body is properly constructed
    let req = CreateServerRequest {
        name: "test-server".to_string(),
        server_type: "cpx22".to_string(),
        location: "nbg1".to_string(),
        image: "ubuntu-22.04".to_string(),
        ssh_keys: vec!["restreamer".to_string()],
        user_data: "#cloud-config\n".to_string(),
        labels: [("app".to_string(), "restreamer".to_string())]
            .into_iter()
            .collect(),
    };
    let json = serde_json::to_value(&req).unwrap();
    assert_eq!(json["name"], "test-server");
    assert_eq!(json["server_type"], "cpx22");
    assert_eq!(json["location"], "nbg1");
    assert_eq!(json["ssh_keys"][0], "restreamer");
}

#[test]
fn server_response_deserialize() {
    let json = r#"{
        "server": {
            "id": 123,
            "name": "rs-delivery-1",
            "status": "running",
            "public_net": {"ipv4": {"ip": "1.2.3.4"}, "ipv6": {"ip": "::1"}},
            "server_type": {"name": "cx23", "description": "CX23"},
            "created": "2026-01-01T00:00:00+00:00"
        }
    }"#;
    let resp: ServerResponse = serde_json::from_str(json).unwrap();
    assert_eq!(resp.server.id, 123);
    assert_eq!(resp.server.name, "rs-delivery-1");
    assert_eq!(resp.server.public_net.ipv4.ip, "1.2.3.4");
}

#[test]
fn image_response_deserialize() {
    let json = r#"{
        "image": {
            "id": 456,
            "description": "rs-delivery snapshot",
            "status": "available",
            "created": "2026-01-01T00:00:00+00:00",
            "labels": {"app": "restreamer"}
        }
    }"#;
    let resp: ImageResponse = serde_json::from_str(json).unwrap();
    assert_eq!(resp.image.id, 456);
    assert_eq!(resp.image.description, "rs-delivery snapshot");
}

#[test]
fn ssh_key_response_deserialize() {
    let json = r#"{
        "ssh_keys": [
            {"id": 1, "name": "restreamer", "fingerprint": "aa:bb:cc"}
        ]
    }"#;
    let resp: SshKeysResponse = serde_json::from_str(json).unwrap();
    assert_eq!(resp.ssh_keys.len(), 1);
    assert_eq!(resp.ssh_keys[0].name, "restreamer");
}

#[test]
fn error_response_deserialize() {
    let json = r#"{"error": {"code": "not_found", "message": "Server not found"}}"#;
    let resp: ErrorResponse = serde_json::from_str(json).unwrap();
    assert_eq!(resp.error.code, "not_found");
}

// ----- create_server transient-error retry (#223) -----

/// A bare Hetzner server object (the shape inside `{"server": …}` and each
/// element of `{"servers": […]}`), with an explicit status and labels.
fn server_obj(id: i64, name: &str, status: &str, labels: &[(&str, &str)]) -> serde_json::Value {
    let lbls: serde_json::Map<String, serde_json::Value> = labels
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
        .collect();
    serde_json::json!({
        "id": id,
        "name": name,
        "status": status,
        "public_net": {"ipv4": {"ip": "1.2.3.4"}},
        "server_type": {"name": "cpx22"},
        "created": "2026-01-01T00:00:00+00:00",
        "labels": lbls
    })
}

fn ok_server_body(id: i64, name: &str) -> serde_json::Value {
    serde_json::json!({ "server": server_obj(id, name, "initializing", &[]) })
}

/// The labels `start_delivery` attaches to a delivery VPS — used both when
/// creating and (subset-matched) when deciding a found server is adoptable.
fn evt_labels(event_id: &str) -> std::collections::HashMap<String, String> {
    [
        ("app", "restreamer"),
        ("event_id", event_id),
        ("client_uuid", "inst-1"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect()
}

/// #223 RED: a transient 5xx from `POST /servers` must be retried
/// server-side, and the eventual 201 returns the created server. Before
/// the fix, `create_server` POSTs once and surfaces the 503 immediately.
#[tokio::test]
async fn create_server_retries_transient_5xx_then_succeeds() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;

    // First POST -> transient 503. Mounted FIRST and capped at one hit:
    // wiremock serves the first-registered mock that still has capacity,
    // so this answers POST #1, then `up_to_n_times(1)` exhausts it.
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": {"code": "unavailable", "message": "service temporarily unavailable"}
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    // Retry POST -> 201 success. Mounted SECOND, so once the 503 mock is
    // exhausted this one answers the retried request.
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(
            ResponseTemplate::new(201).set_body_json(ok_server_body(999, "rs-delivery-evt7")),
        )
        .expect(1)
        .mount(&server)
        .await;

    let client = HetznerClient::with_base_url("tok", &server.uri())
        .with_retry(4, std::time::Duration::from_millis(1));
    let got = client
        .create_server(
            "rs-delivery-evt7",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            std::collections::HashMap::new(),
        )
        .await
        .expect("create_server should retry the transient 503 and return the 201 server");
    assert_eq!(got.id, 999);
    assert_eq!(got.name, "rs-delivery-evt7");
}

/// #223: a permanent 4xx (e.g. malformed request) is NOT retried — it is
/// surfaced immediately after a single POST.
#[tokio::test]
async fn create_server_permanent_4xx_not_retried() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": {"code": "invalid_input", "message": "bad server_type"}
        })))
        .expect(1) // exactly one attempt, no retry
        .mount(&server)
        .await;

    let client = HetznerClient::with_base_url("tok", &server.uri())
        .with_retry(4, std::time::Duration::from_millis(1));
    let err = client
        .create_server(
            "rs-delivery-evt8",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            std::collections::HashMap::new(),
        )
        .await
        .expect_err("permanent 4xx must not be retried");
    match err {
        CloudError::Api { status, .. } => assert_eq!(status, 400),
        other => panic!("expected Api 400, got {other:?}"),
    }
}

/// #223 idempotency: a `409` name-conflict means a prior attempt already
/// created the VPS, so create_server looks it up by name and ADOPTS the
/// existing (label-matching, non-deleting) server instead of erroring or
/// POSTing a second VPS.
#[tokio::test]
async fn create_server_adopts_on_name_conflict_409() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;

    // The name lookup finds OUR server (matching labels, not deleting).
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": [server_obj(
                555, "rs-delivery-evt9", "initializing",
                &[("app", "restreamer"), ("event_id", "9"), ("client_uuid", "inst-1")]
            )]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Exactly ONE POST — it 409s (name taken); the code must adopt via the
    // lookup, never POST a second server.
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "error": {"code": "uniqueness_error", "message": "name already used"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = HetznerClient::with_base_url("tok", &server.uri())
        .with_retry(4, std::time::Duration::from_millis(1));
    let got = client
        .create_server(
            "rs-delivery-evt9",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            evt_labels("9"),
        )
        .await
        .expect("must adopt the already-created server");
    assert_eq!(got.id, 555, "adopted the existing VPS, not a new one");
}

/// #223 W4: a `409` whose only same-named server is the PREVIOUS VPS of
/// this event still `deleting` must NOT be adopted (it would point the DB
/// row at a VPS about to vanish, with a stale auth token). It is treated
/// as transient and eventually surfaced after the retry bound.
#[tokio::test]
async fn create_server_does_not_adopt_deleting_server() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;

    // Every name lookup returns the OLD server, mid-deletion.
    Mock::given(method("GET"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "servers": [server_obj(
                111, "rs-delivery-evt9", "deleting",
                &[("app", "restreamer"), ("event_id", "9"), ("client_uuid", "inst-1")]
            )]
        })))
        .mount(&server)
        .await;

    // POST always 409 — name still held by the deleting VPS.
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(409).set_body_json(serde_json::json!({
            "error": {"code": "uniqueness_error", "message": "name already used"}
        })))
        .expect(2) // with_retry(2, ..) => two attempts, neither adopts
        .mount(&server)
        .await;

    let client = HetznerClient::with_base_url("tok", &server.uri())
        .with_retry(2, std::time::Duration::from_millis(1));
    let err = client
        .create_server(
            "rs-delivery-evt9",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            evt_labels("9"),
        )
        .await
        .expect_err("a deleting same-named server must not be adopted");
    match err {
        CloudError::Api { status, .. } => assert_eq!(status, 409),
        other => panic!("expected Api 409 after exhaustion, got {other:?}"),
    }
}

/// #223 S2: the actual observed failure class — a transport-level error
/// (connection refused) — is transient and retried, then surfaced as
/// `CloudError::Http` once the bound is reached. Points at a closed port.
#[tokio::test]
async fn create_server_retries_transport_error_then_surfaces_http() {
    // 127.0.0.1:1 refuses connections — a connect-level reqwest error,
    // the send-level class the ticket's CI failure belongs to.
    let client = HetznerClient::with_base_url("tok", "http://127.0.0.1:1")
        .with_retry(2, std::time::Duration::from_millis(1));
    let err = client
        .create_server(
            "rs-delivery-evt11",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            std::collections::HashMap::new(),
        )
        .await
        .expect_err("transport error must surface after retries");
    assert!(
        matches!(err, CloudError::Http(_)),
        "expected CloudError::Http, got {err:?}"
    );
}

/// #223: a persistently-down API exhausts the retry bound and surfaces
/// the last transient error (no infinite loop).
#[tokio::test]
async fn create_server_exhausts_retries_then_errors() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    // POST always 503 (a 5xx does not trigger a name lookup — only a 409
    // does — so no GET mock is needed here).
    Mock::given(method("POST"))
        .and(path("/servers"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": {"code": "unavailable", "message": "still down"}
        })))
        .expect(2) // with_retry(2, ..) => exactly 2 attempts
        .mount(&server)
        .await;

    let client = HetznerClient::with_base_url("tok", &server.uri())
        .with_retry(2, std::time::Duration::from_millis(1));
    let err = client
        .create_server(
            "rs-delivery-evt10",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            std::collections::HashMap::new(),
        )
        .await
        .expect_err("exhausted retries must surface the transient error");
    match err {
        CloudError::Api { status, .. } => assert_eq!(status, 503),
        other => panic!("expected Api 503, got {other:?}"),
    }
}

// ----- #367: retry classification + backoff, pinned for cargo-mutants -----

/// 1 s, 3 s, 9 s, 27 s with the default 1 s base, then capped at 30 s, and a
/// huge attempt number saturates instead of overflowing (#223 S1).
#[test]
fn retry_backoff_is_base_times_three_to_the_attempt_capped() {
    let s = Duration::from_secs;
    assert_eq!(retry_backoff(s(1), 1), s(1));
    assert_eq!(retry_backoff(s(1), 2), s(3));
    assert_eq!(retry_backoff(s(1), 3), s(9));
    assert_eq!(retry_backoff(s(1), 4), s(27));
    assert_eq!(retry_backoff(s(1), 5), MAX_BACKOFF);
    assert_eq!(retry_backoff(s(1), u32::MAX), MAX_BACKOFF);
    assert_eq!(retry_backoff(s(1), 0), s(1), "attempt 0 cannot underflow");
    assert_eq!(
        retry_backoff(Duration::from_millis(10), 2),
        Duration::from_millis(30)
    );
}

/// Each of the four reqwest transport classes is retried on its own; an
/// error in none of them (a builder error, a redirect loop) is not.
#[test]
fn every_transport_error_class_is_transient_on_its_own() {
    let none = TransportClass::default();
    assert!(!none.is_transient());
    let only = |c: TransportClass| c.is_transient();
    assert!(
        only(TransportClass {
            timeout: true,
            ..none
        }),
        "timeout"
    );
    assert!(
        only(TransportClass {
            connect: true,
            ..none
        }),
        "connect"
    );
    assert!(
        only(TransportClass {
            request: true,
            ..none
        }),
        "request"
    );
    assert!(
        only(TransportClass {
            decode: true,
            ..none
        }),
        "decode"
    );
}

/// Only a 409 is the "name already taken" adoption signal; a 5xx or a
/// 4xx must never trigger a lookup-and-adopt.
#[test]
fn name_conflict_is_only_a_409() {
    let api = |status| CloudError::Api {
        status,
        message: String::new(),
    };
    assert!(is_name_conflict(&api(409)));
    assert!(!is_name_conflict(&api(503)));
    assert!(!is_name_conflict(&api(400)));
}

/// A raw HTTP endpoint on an ephemeral port that counts connections. Each
/// connection gets `reply` after its request was read, then is held open for
/// `hold` and closed.
async fn raw_endpoint(
    reply: &'static [u8],
    hold: Duration,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = accepted.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                let _ = sock.read(&mut buf).await;
                let _ = sock.write_all(reply).await;
                tokio::time::sleep(hold).await;
            });
        }
    });
    (format!("http://{addr}"), accepted)
}

async fn create_against(client: &HetznerClient) -> CloudError {
    tokio::time::timeout(
        Duration::from_secs(10),
        client.create_server(
            "rs-delivery-evt12",
            "cpx22",
            "fsn1",
            "ubuntu-24.04",
            &["restreamer".to_string()],
            "#cloud-config\n",
            std::collections::HashMap::new(),
        ),
    )
    .await
    .expect("create_server must return within its timeouts, never hang")
    .expect_err("the endpoint never answers a valid server")
}

/// #223: a connection the server closes without answering is a transport
/// error (`CloudError::Http`), so it is transient and retried.
#[tokio::test]
async fn create_server_retries_a_connection_closed_before_the_reply() {
    let (base, accepted) = raw_endpoint(b"", Duration::ZERO).await;
    let client = HetznerClient::with_base_url("tok", &base).with_retry(2, Duration::from_millis(1));
    let err = create_against(&client).await;
    assert!(matches!(err, CloudError::Http(_)), "got {err:?}");
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "a dropped connection is transient: the request is retried once"
    );
}

/// #223 W3: the request timeout covers the BODY. A server that sends the 201
/// headers and then stalls surfaces as a timeout instead of hanging, and is
/// retried.
#[tokio::test]
async fn create_server_times_out_and_retries_a_stalled_response_body() {
    let (base, accepted) = raw_endpoint(
        b"HTTP/1.1 201 Created\r\ncontent-type: application/json\r\ncontent-length: 4096\r\n\r\n{\"server\":",
        Duration::from_secs(30),
    )
    .await;
    let client = HetznerClient::with_base_url("tok", &base)
        .with_retry(2, Duration::from_millis(1))
        .with_timeouts(Duration::from_secs(5), Duration::from_millis(300));
    let err = create_against(&client).await;
    match &err {
        CloudError::Http(e) => assert!(e.is_timeout(), "expected a timeout, got {e:?}"),
        other => panic!("expected CloudError::Http, got {other:?}"),
    }
    assert_eq!(
        accepted.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "a timeout is transient: the request is retried once"
    );
}
