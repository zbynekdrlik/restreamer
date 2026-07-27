use super::*;
use axum::http::HeaderValue;
use jsonwebtoken::{EncodingKey, Header, encode};
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde::Serialize;
use std::sync::LazyLock;

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for (k, v) in pairs {
        h.insert(
            axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            HeaderValue::from_str(v).unwrap(),
        );
    }
    h
}

fn peer(s: &str) -> SocketAddr {
    s.parse().unwrap()
}

// -- classification ----------------------------------------------------

#[test]
fn loopback_without_forwarded_headers_is_local() {
    assert_eq!(
        classify(Some(&peer("127.0.0.1:5000")), &headers(&[])),
        Origin::Local
    );
    assert_eq!(
        classify(Some(&peer("[::1]:5000")), &headers(&[])),
        Origin::Local
    );
}

#[test]
fn rfc1918_lan_is_local() {
    // soak-mini.ps1 and the CI runner depend on this.
    for addr in ["10.77.9.204:8910", "192.168.1.5:5000", "172.16.4.9:5000"] {
        assert_eq!(
            classify(Some(&peer(addr)), &headers(&[])),
            Origin::Local,
            "{addr} must be Local"
        );
    }
}

#[test]
fn tailscale_cgnat_is_local() {
    assert_eq!(
        classify(Some(&peer("100.104.8.125:5000")), &headers(&[])),
        Origin::Local
    );
    // 100.128.x is NOT in 100.64.0.0/10.
    assert_eq!(
        classify(Some(&peer("100.128.0.1:5000")), &headers(&[])),
        Origin::Internet
    );
}

#[test]
fn public_peer_is_internet() {
    assert_eq!(
        classify(Some(&peer("8.8.8.8:5000")), &headers(&[])),
        Origin::Internet
    );
}

#[test]
fn each_forwarded_header_alone_marks_internet() {
    for h in PROXY_HEADERS {
        assert_eq!(
            classify(
                Some(&peer("127.0.0.1:5000")),
                &headers(&[(h, "203.0.113.7")])
            ),
            Origin::Internet,
            "{h} must betray the tunnel"
        );
    }
}

#[test]
fn a_lan_peer_that_sends_a_forwarded_header_is_treated_as_internet() {
    // Residual risk #3 in #273: a browser cannot set these, so this is a
    // deliberate, logged, fail-closed choice.
    assert_eq!(
        classify(
            Some(&peer("10.77.9.42:5000")),
            &headers(&[("x-forwarded-for", "203.0.113.7")])
        ),
        Origin::Internet
    );
}

#[test]
fn missing_peer_falls_back_to_the_header_half() {
    assert_eq!(classify(None, &headers(&[])), Origin::Local);
    assert_eq!(
        classify(None, &headers(&[("cf-connecting-ip", "203.0.113.7")])),
        Origin::Internet
    );
}

// -- loopback-only class (#337, inherited from #205) --------------------

#[test]
fn genuinely_local_requires_loopback_and_no_forwarded_headers() {
    assert!(is_genuinely_local(
        Some(&peer("127.0.0.1:1")),
        &headers(&[])
    ));
    assert!(is_genuinely_local(Some(&peer("[::1]:1")), &headers(&[])));
    assert!(!is_genuinely_local(
        Some(&peer("10.77.9.42:1")),
        &headers(&[])
    ));
    assert!(!is_genuinely_local(Some(&peer("8.8.8.8:1")), &headers(&[])));
    for h in PROXY_HEADERS {
        assert!(
            !is_genuinely_local(Some(&peer("127.0.0.1:1")), &headers(&[(h, "x")])),
            "{h} on a loopback peer is a tunneled request (#205)"
        );
    }
}

// -- CSRF (#339) -------------------------------------------------------

#[test]
fn cross_origin_post_is_a_violation() {
    let h = headers(&[
        ("origin", "https://evil.example"),
        ("host", "stream.lan:8910"),
    ]);
    assert!(csrf_violation(&h, &Method::POST).is_some());
    assert!(csrf_violation(&h, &Method::DELETE).is_some());
    assert!(csrf_violation(&h, &Method::PATCH).is_some());
}

#[test]
fn same_origin_post_is_fine() {
    let h = headers(&[
        ("origin", "http://stream.lan:8910"),
        ("host", "stream.lan:8910"),
    ]);
    assert!(csrf_violation(&h, &Method::POST).is_none());
}

#[test]
fn origin_matching_the_forwarded_host_is_fine() {
    // Through the tunnel the browser's Origin is the public hostname.
    let h = headers(&[
        ("origin", "https://streamsnv.newlevel.media"),
        ("host", "localhost:8910"),
        ("x-forwarded-host", "streamsnv.newlevel.media"),
    ]);
    assert!(csrf_violation(&h, &Method::POST).is_none());
}

#[test]
fn sec_fetch_site_cross_site_is_a_violation_even_with_no_origin() {
    let h = headers(&[("sec-fetch-site", "cross-site")]);
    assert!(csrf_violation(&h, &Method::POST).is_some());
    // same-origin / same-site / none are all legitimate.
    for site in ["same-origin", "same-site", "none"] {
        assert!(csrf_violation(&headers(&[("sec-fetch-site", site)]), &Method::POST).is_none());
    }
}

#[test]
fn reads_are_never_csrf() {
    let h = headers(&[
        ("origin", "https://evil.example"),
        ("host", "stream.lan:8910"),
        ("sec-fetch-site", "cross-site"),
    ]);
    assert!(csrf_violation(&h, &Method::GET).is_none());
    assert!(csrf_violation(&h, &Method::HEAD).is_none());
}

#[test]
fn no_origin_header_is_not_csrf() {
    // curl / Invoke-RestMethod / the CI runner.
    assert!(csrf_violation(&headers(&[("host", "127.0.0.1:8910")]), &Method::POST).is_none());
}

#[test]
fn null_origin_is_refused() {
    let h = headers(&[("origin", "null"), ("host", "stream.lan:8910")]);
    assert!(csrf_violation(&h, &Method::POST).is_some());
}

// -- token extraction --------------------------------------------------

#[test]
fn token_read_from_the_header() {
    assert_eq!(
        extract_token(&headers(&[(JWT_HEADER, "abc.def.ghi")])),
        Some("abc.def.ghi".to_string())
    );
}

#[test]
fn token_read_from_the_cookie() {
    assert_eq!(
        extract_token(&headers(&[(
            "cookie",
            "foo=bar; CF_Authorization=abc.def.ghi; baz=qux"
        )])),
        Some("abc.def.ghi".to_string())
    );
}

#[test]
fn no_token_at_all() {
    assert_eq!(extract_token(&headers(&[])), None);
    assert_eq!(extract_token(&headers(&[("cookie", "foo=bar")])), None);
}

// -- real RS256 verification ------------------------------------------
//
// A 2048-bit key pair is generated ONCE per test binary (ring refuses to
// sign with anything smaller). Nothing is committed: a private key in the
// repo would be a leak even as a fixture (#274).

struct TestKeys {
    encoding: EncodingKey,
    kid: String,
    decoding: DecodingKey,
}

static KEYS: LazyLock<TestKeys> = LazyLock::new(|| {
    let mut rng = rsa::rand_core::OsRng;
    let private = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("generate test RSA key");
    let der = private
        .to_pkcs1_der()
        .expect("encode test key")
        .as_bytes()
        .to_vec();
    let public = private.to_public_key();
    let n = base64_url(&public.n().to_bytes_be());
    let e = base64_url(&public.e().to_bytes_be());
    TestKeys {
        encoding: EncodingKey::from_rsa_der(&der),
        kid: "test-kid".to_string(),
        decoding: DecodingKey::from_rsa_components(&n, &e).expect("decoding key"),
    }
});

fn base64_url(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[derive(Serialize)]
struct TestClaims {
    aud: Vec<String>,
    iss: String,
    exp: i64,
    nbf: i64,
    email: String,
}

const AUD: &str = "3d69cb15e165fef384d065feebe37f94918e2f4730756bc6c0ba0c054ff42d26";
const ISS: &str = "https://newlevelchurch.cloudflareaccess.com";

fn mint(aud: &str, iss: &str, exp_offset: i64) -> String {
    let now = chrono::Utc::now().timestamp();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(KEYS.kid.clone());
    encode(
        &header,
        &TestClaims {
            aud: vec![aud.to_string()],
            iss: iss.to_string(),
            exp: now + exp_offset,
            nbf: now - 60,
            email: "drlik.marek@gmail.com".to_string(),
        },
        &KEYS.encoding,
    )
    .unwrap()
}

fn test_gate() -> Arc<AccessGate> {
    AccessGate::for_test(
        AccessMode::Enforce,
        &[AUD],
        ISS,
        vec![(KEYS.kid.clone(), KEYS.decoding.clone())],
    )
}

#[tokio::test]
async fn valid_token_is_accepted_and_carries_the_email() {
    let claims = test_gate().verify(&mint(AUD, ISS, 3600)).await.unwrap();
    assert_eq!(claims.identity(), "drlik.marek@gmail.com");
}

#[tokio::test]
async fn wrong_audience_is_rejected() {
    let err = test_gate()
        .verify(&mint("some-other-application-aud", ISS, 3600))
        .await
        .unwrap_err();
    assert!(err.contains("rejected"), "{err}");
}

#[tokio::test]
async fn expired_token_is_rejected() {
    let err = test_gate()
        .verify(&mint(AUD, ISS, -3600))
        .await
        .unwrap_err();
    assert!(err.contains("rejected"), "{err}");
}

#[tokio::test]
async fn wrong_issuer_is_rejected() {
    let err = test_gate()
        .verify(&mint(AUD, "https://attacker.cloudflareaccess.com", 3600))
        .await
        .unwrap_err();
    assert!(err.contains("rejected"), "{err}");
}

#[tokio::test]
async fn tampered_signature_is_rejected() {
    let token = mint(AUD, ISS, 3600);
    // Alter the first character of the signature segment. The mapping must
    // change the character for EVERY possible input — an earlier version
    // rewrote it to 'A' only when it was not already 'A', which left the token
    // untouched (and the test passing vacuously, then failing on the next run)
    // for the ~1-in-64 keys whose signature happens to start with 'A'.
    let mut parts: Vec<String> = token.split('.').map(|s| s.to_string()).collect();
    let sig = parts.pop().unwrap();
    let mut chars: Vec<char> = sig.chars().collect();
    chars[0] = if chars[0] == 'A' { 'B' } else { 'A' };
    let tampered: String = chars.into_iter().collect();
    assert_ne!(
        tampered, sig,
        "the tamper must actually change the signature"
    );
    parts.push(tampered);
    let err = test_gate().verify(&parts.join(".")).await.unwrap_err();
    assert!(err.contains("rejected"), "{err}");
}

#[tokio::test]
async fn unknown_kid_is_rejected_without_hanging() {
    // The gate's jwks_url points at a dead port, so this also proves a
    // failed refresh degrades to a refusal instead of an error page.
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("kid-we-never-saw".to_string());
    let now = chrono::Utc::now().timestamp();
    let token = encode(
        &header,
        &TestClaims {
            aud: vec![AUD.to_string()],
            iss: ISS.to_string(),
            exp: now + 3600,
            nbf: now - 60,
            email: "x@y.z".to_string(),
        },
        &KEYS.encoding,
    )
    .unwrap();
    let err = test_gate().verify(&token).await.unwrap_err();
    assert!(err.contains("no Access signing key"), "{err}");
}

#[tokio::test]
async fn garbage_token_is_rejected() {
    let err = test_gate().verify("not-a-jwt").await.unwrap_err();
    assert!(err.contains("malformed"), "{err}");
}

// -- policy ------------------------------------------------------------

#[tokio::test]
async fn internet_request_with_a_valid_token_is_allowed() {
    let token = mint(AUD, ISS, 3600);
    let h = headers(&[("cf-connecting-ip", "203.0.113.7"), (JWT_HEADER, &token)]);
    let d = decide(
        &test_gate(),
        Some(&peer("127.0.0.1:5000")),
        &h,
        &Method::GET,
        "/api/v1/status",
    )
    .await;
    match d {
        Decision::Allow { origin, identity } => {
            assert_eq!(origin, Origin::Internet);
            assert_eq!(identity.as_deref(), Some("drlik.marek@gmail.com"));
        }
        other => panic!("expected Allow, got {other:?}"),
    }
}

#[tokio::test]
async fn a_valid_token_still_cannot_reach_the_test_hooks() {
    let token = mint(AUD, ISS, 3600);
    let h = headers(&[("cf-connecting-ip", "203.0.113.7"), (JWT_HEADER, &token)]);
    let d = decide(
        &test_gate(),
        Some(&peer("127.0.0.1:5000")),
        &h,
        &Method::POST,
        "/api/v1/_test/s3-block",
    )
    .await;
    assert!(
        matches!(
            d,
            Decision::Deny {
                reason: "loopback_only",
                ..
            }
        ),
        "#337: a logged-in remote operator must not be able to kill the stream: {d:?}"
    );
}

#[tokio::test]
async fn lan_only_mode_refuses_even_a_valid_token() {
    let gate = AccessGate::for_test(
        AccessMode::LanOnly,
        &[AUD],
        ISS,
        vec![(KEYS.kid.clone(), KEYS.decoding.clone())],
    );
    let token = mint(AUD, ISS, 3600);
    let h = headers(&[("cf-connecting-ip", "203.0.113.7"), (JWT_HEADER, &token)]);
    let d = decide(
        &gate,
        Some(&peer("127.0.0.1:5000")),
        &h,
        &Method::GET,
        "/api/v1/status",
    )
    .await;
    assert!(
        matches!(
            d,
            Decision::Deny {
                reason: "lan_only",
                ..
            }
        ),
        "{d:?}"
    );
}

#[tokio::test]
async fn local_requests_never_touch_the_token_path() {
    // jwks_url points at a dead port; if the Local branch did any network
    // I/O this would be slow or fail. It must be instant.
    let d = decide(
        &test_gate(),
        Some(&peer("10.77.9.204:8910")),
        &headers(&[]),
        &Method::POST,
        "/api/v1/actions/toggle-delivering",
    )
    .await;
    assert!(
        matches!(
            d,
            Decision::Allow {
                origin: Origin::Local,
                ..
            }
        ),
        "{d:?}"
    );
}

#[test]
fn mode_parsing_defaults_to_enforce() {
    assert_eq!(AccessMode::parse("enforce"), AccessMode::Enforce);
    assert_eq!(AccessMode::parse("log_only"), AccessMode::LogOnly);
    assert_eq!(AccessMode::parse("lan_only"), AccessMode::LanOnly);
    assert_eq!(AccessMode::parse("LAN_ONLY"), AccessMode::LanOnly);
    assert_eq!(AccessMode::parse("nonsense"), AccessMode::Enforce);
    assert_eq!(AccessMode::parse(""), AccessMode::Enforce);
}

// -- review follow-ups: the properties the design CLAIMS, pinned ----------

#[test]
fn ipv4_mapped_loopback_is_still_genuinely_local() {
    // If `api.bind` were ever switched to `::`, every CI `_test` call (all of
    // which use 127.0.0.1) would arrive as ::ffff:127.0.0.1.
    assert!(is_genuinely_local(
        Some(&peer("[::ffff:127.0.0.1]:1")),
        &headers(&[])
    ));
    assert_eq!(
        classify(Some(&peer("[::ffff:10.77.9.42]:1")), &headers(&[])),
        Origin::Local
    );
    assert_eq!(
        classify(Some(&peer("[::ffff:8.8.8.8]:1")), &headers(&[])),
        Origin::Internet
    );
}

#[test]
fn dns_rebinding_is_refused_even_though_origin_matches_host() {
    // The attack Origin-vs-Host alone cannot see: the operator's browser is
    // pointed at evil.example, which has been rebound to the box's LAN
    // address, so Origin and Host agree perfectly — and both are the
    // attacker's choosing.
    let h = headers(&[
        ("origin", "http://evil.example:8910"),
        ("host", "evil.example:8910"),
        ("sec-fetch-site", "same-origin"),
    ]);
    let violation = csrf_violation(&h, &Method::POST);
    assert!(
        violation.is_some(),
        "a Host this box does not answer to must be refused for a mutating request"
    );
    assert!(violation.unwrap().contains("rebinding"));
}

#[test]
fn every_host_the_box_legitimately_answers_to_is_trusted() {
    for authority in [
        "127.0.0.1:8910",
        "10.77.9.204:8910",
        "192.168.1.7:8910",
        "[::1]:8910",
        "localhost:8910",
        "stream.lan:8910",
        "stream-pp", // single-label / MagicDNS short name
        "streamsnv.newlevel.media",
        "streampp.newlevel.media",
        "stream-snv.tailnet.ts.net",
    ] {
        assert!(
            is_trusted_authority(authority),
            "{authority} is a legitimate way to reach the box and must not 403"
        );
    }
    for authority in [
        "evil.example:8910",
        "attacker.co.uk",
        "newlevel.media.evil.com",
    ] {
        assert!(
            !is_trusted_authority(authority),
            "{authority} is attacker-registrable and must not be trusted"
        );
    }
}

#[test]
fn websocket_upgrade_gets_the_same_origin_check_as_a_mutating_request() {
    // Cross-site WebSocket hijacking: the handshake is a GET and WebSockets
    // are exempt from CORS, so without this a page on the internet could open
    // a socket to the LAN box and read live state.
    let h = headers(&[
        ("upgrade", "websocket"),
        ("origin", "https://evil.example"),
        ("host", "stream.lan:8910"),
    ]);
    assert!(csrf_violation(&h, &Method::GET).is_some());

    // The dashboard's own socket is same-origin and must still connect.
    let ok = headers(&[
        ("upgrade", "websocket"),
        ("origin", "http://stream.lan:8910"),
        ("host", "stream.lan:8910"),
    ]);
    assert!(csrf_violation(&ok, &Method::GET).is_none());

    // A non-browser client (the delivery VPS, a test harness) sends no Origin.
    let no_origin = headers(&[("upgrade", "websocket"), ("host", "127.0.0.1:8910")]);
    assert!(csrf_violation(&no_origin, &Method::GET).is_none());
}

#[tokio::test]
async fn a_token_without_aud_is_rejected() {
    // `set_audience` alone only validates an aud that is PRESENT — an absent
    // one used to sail straight through, defeating the application pin.
    #[derive(Serialize)]
    struct NoAud {
        iss: String,
        exp: i64,
        email: String,
    }
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(KEYS.kid.clone());
    let token = encode(
        &header,
        &NoAud {
            iss: ISS.to_string(),
            exp: chrono::Utc::now().timestamp() + 3600,
            email: "x@y.z".to_string(),
        },
        &KEYS.encoding,
    )
    .unwrap();
    let err = test_gate().verify(&token).await.unwrap_err();
    assert!(err.contains("rejected"), "{err}");
}

#[tokio::test]
async fn a_token_without_iss_is_rejected() {
    #[derive(Serialize)]
    struct NoIss {
        aud: Vec<String>,
        exp: i64,
    }
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(KEYS.kid.clone());
    let token = encode(
        &header,
        &NoIss {
            aud: vec![AUD.to_string()],
            exp: chrono::Utc::now().timestamp() + 3600,
        },
        &KEYS.encoding,
    )
    .unwrap();
    let err = test_gate().verify(&token).await.unwrap_err();
    assert!(err.contains("rejected"), "{err}");
}

#[tokio::test]
async fn a_non_rs256_token_is_rejected_before_any_key_lookup() {
    // Algorithm confusion: an HS256 token signed with the PUBLIC key material
    // must never be treated as a valid signature.
    let header = Header::new(Algorithm::HS256);
    let token = encode(
        &header,
        &serde_json::json!({"aud": [AUD], "iss": ISS, "exp": chrono::Utc::now().timestamp() + 3600}),
        &EncodingKey::from_secret(b"whatever"),
    )
    .unwrap();
    let err = test_gate().verify(&token).await.unwrap_err();
    assert!(err.contains("unexpected algorithm"), "{err}");
}

#[tokio::test]
async fn an_unknown_kid_does_not_refetch_the_jwks_per_request() {
    // Otherwise an unauthenticated attacker has a free amplifier: N requests
    // with N random kids = N fetches against the identity endpoint, each
    // holding a request open for the 5s timeout.
    let gate = test_gate();
    let started = std::time::Instant::now();
    for i in 0..5 {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(format!("random-kid-{i}"));
        let token = encode(
            &header,
            &TestClaims {
                aud: vec![AUD.to_string()],
                iss: ISS.to_string(),
                exp: chrono::Utc::now().timestamp() + 3600,
                nbf: chrono::Utc::now().timestamp() - 60,
                email: "x@y.z".to_string(),
            },
            &KEYS.encoding,
        )
        .unwrap();
        assert!(gate.verify(&token).await.is_err());
    }
    // The gate's jwks_url points at a dead port; without the rate limit each
    // miss would attempt a fresh connection.
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "5 unknown-kid requests took {:?} — the refresh rate limit is not working",
        started.elapsed()
    );
}
