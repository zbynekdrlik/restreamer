//! Route-level tests for origin-aware access control (#70, #273, #337, #339).
//!
//! These drive the REAL Axum router built by [`crate::router::build_router`],
//! with a synthetic peer address and headers, so they exercise exactly what a
//! request arriving through cloudflared (or from a malicious page) looks like.
//!
//! The three holes they pin, all reproducible on the code these tests were
//! written against:
//!
//! * **#337** — `/api/v1/_test/*` (s3-block, s3-unblock, oauth-device-grant)
//!   had no runtime gate at all, so `POST /api/v1/_test/s3-block` through the
//!   public tunnel could starve the delivery cache and kill the live stream.
//! * **#70 / #273** — every one of the ~60 `/api/v1/*` routes plus the SPA
//!   answered an internet-sourced request with no authentication whatsoever.
//! * **#339** — `allow_origin(Any)` plus bodyless POST actions made
//!   `POST /api/v1/actions/toggle-delivering` a CORS *simple* request: any web
//!   page the operator visits could fire it with no preflight.
//!
//! The invariant that must NOT change is equally covered here: a genuinely
//! local request (loopback) and an RFC1918 LAN request stay unauthenticated —
//! `scripts/soak-mini.ps1` and the self-hosted CI runner both call the box
//! that way, and the church operator must never face a login during a service.

use crate::router::build_router;
use crate::state::AppState;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use rs_core::config::Config;
use rs_core::models::WsEvent;
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tower::ServiceExt;

pub(crate) async fn test_state() -> AppState {
    let pool = rs_core::db::create_memory_pool().await.unwrap();
    rs_core::db::run_migrations(&pool).await.unwrap();
    let config = Config::for_testing();
    let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
    AppState::new_for_tests(pool, config, ws_tx)
}

/// Build a request with an explicit peer address and header set.
///
/// `peer` mirrors what `into_make_service_with_connect_info::<SocketAddr>()`
/// wires in production (`lib.rs::serve`).
pub(crate) fn req(method: &str, uri: &str, peer: &str, headers: &[(&str, &str)]) -> Request<Body> {
    let addr: SocketAddr = peer.parse().unwrap();
    let mut builder = Request::builder().method(method).uri(uri);
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let mut request = builder.body(Body::empty()).unwrap();
    request.extensions_mut().insert(ConnectInfo(addr));
    request
}

/// A request that reached us through the Cloudflare tunnel: cloudflared
/// connects from `127.0.0.1`, so the peer address alone looks local — the
/// forwarded header is the only thing that betrays it (#205).
fn tunneled(method: &str, uri: &str) -> Request<Body> {
    req(
        method,
        uri,
        "127.0.0.1:54321",
        &[("cf-connecting-ip", "203.0.113.7")],
    )
}

// ---------------------------------------------------------------------------
// #337 — CI-only /_test/* routes must be reachable ONLY from the same box.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_hook_s3_block_denied_through_the_tunnel() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(tunneled("POST", "/api/v1/_test/s3-block"))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#337: /_test/s3-block can starve the delivery cache and kill the live \
         stream — a tunneled request must never reach it"
    );
}

#[tokio::test]
async fn test_hook_s3_unblock_denied_through_the_tunnel() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(tunneled("POST", "/api/v1/_test/s3-unblock"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#337");
}

#[tokio::test]
async fn test_hook_oauth_device_grant_denied_through_the_tunnel() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(tunneled("POST", "/api/v1/_test/oauth-device-grant"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#337");
}

/// The `/_test/*` class is loopback-only, NOT merely "local": a LAN peer is
/// good enough for the dashboard but not for a hook that can kill the stream.
#[tokio::test]
async fn test_hook_denied_from_a_lan_peer() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/_test/s3-block",
            "10.77.9.42:54000",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#337: only the box itself (loopback, no forwarded headers) may call the test hooks"
    );
}

/// CI calls the hooks from the self-hosted runner on the box itself
/// (`http://127.0.0.1:8910`, ci.yml :4684 / :4823) — that must keep working.
#[tokio::test]
async fn test_hook_allowed_from_genuinely_local_ci() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/_test/s3-unblock",
            "127.0.0.1:54321",
            &[],
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the CI runner on the box is genuinely local and must keep reaching the test hooks"
    );
}

// ---------------------------------------------------------------------------
// #70 / #273 — default-deny for internet-sourced requests. The allowlist is
// deliberately EMPTY: status reads included.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn control_action_from_the_internet_is_denied() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(tunneled("POST", "/api/v1/actions/toggle-delivering"))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#273: an unauthenticated internet request must not be able to stop the stream"
    );
}

#[tokio::test]
async fn config_read_from_the_internet_is_denied() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(tunneled("GET", "/api/v1/config"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#273 default-deny");
}

#[tokio::test]
async fn status_read_from_the_internet_is_denied() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(tunneled("GET", "/api/v1/status"))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#273: the exception list is empty — even status reads require a valid Access JWT"
    );
}

/// A direct public peer with no proxy headers at all (someone port-forwards
/// 8910 on the router, bypassing the tunnel entirely).
#[tokio::test]
async fn direct_public_peer_is_denied() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req("GET", "/api/v1/status", "8.8.8.8:65432", &[]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#273");
}

#[tokio::test]
async fn x_forwarded_for_alone_marks_a_request_as_internet_sourced() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "GET",
            "/api/v1/status",
            "127.0.0.1:54321",
            &[("x-forwarded-for", "203.0.113.7")],
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#273");
}

#[tokio::test]
async fn x_forwarded_host_alone_marks_a_request_as_internet_sourced() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "GET",
            "/api/v1/status",
            "127.0.0.1:54321",
            &[("x-forwarded-host", "streamsnv.newlevel.media")],
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#273");
}

/// The websocket upgrade is a control-plane surface too — it streams live
/// state and must not be reachable unauthenticated from the internet.
#[tokio::test]
async fn websocket_upgrade_from_the_internet_is_denied() {
    let app = build_router(test_state().await);
    let resp = app.oneshot(tunneled("GET", "/api/v1/ws")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "#273");
}

// ---------------------------------------------------------------------------
// The LAN invariant — this must never regress.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn loopback_request_stays_unauthenticated() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req("GET", "/api/v1/status", "127.0.0.1:54321", &[]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the operator on the box must never see authentication"
    );
}

#[tokio::test]
async fn rfc1918_lan_request_stays_unauthenticated() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req("GET", "/api/v1/status", "10.77.9.42:54000", &[]))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "scripts/soak-mini.ps1 targets http://10.77.9.204:8910 unauthenticated — RFC1918 is Local"
    );
}

#[tokio::test]
async fn lan_control_action_stays_unauthenticated() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/_test/s3-unblock",
            "127.0.0.1:54321",
            &[],
        ))
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a local mutating request must not be gated"
    );
}

// ---------------------------------------------------------------------------
// #339 — CSRF. A bodyless cross-origin POST is a CORS *simple* request: the
// browser sends it with no preflight, so CORS never gets a veto. The only
// barrier is an Origin / Sec-Fetch-Site check on the request itself.
//
// Note these run with a LOOPBACK peer and no forwarded headers — i.e. the
// operator's own browser on the church LAN, visiting a malicious page. That is
// the exact scenario in #339, and it is NOT covered by internet-origin gating.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cross_origin_mutating_request_is_denied() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/_test/s3-unblock",
            "127.0.0.1:54321",
            &[
                ("origin", "https://evil.example"),
                ("host", "stream.lan:8910"),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#339: a POST whose Origin does not match the request Host is cross-site forgery"
    );
}

#[tokio::test]
async fn sec_fetch_site_cross_site_mutating_request_is_denied() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/_test/s3-unblock",
            "127.0.0.1:54321",
            &[("sec-fetch-site", "cross-site")],
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#339: Sec-Fetch-Site: cross-site is a browser telling us this is forgery"
    );
}

#[tokio::test]
async fn same_origin_mutating_request_is_allowed() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "POST",
            "/api/v1/_test/s3-unblock",
            "127.0.0.1:54321",
            &[
                ("origin", "http://stream.lan:8910"),
                ("host", "stream.lan:8910"),
                ("sec-fetch-site", "same-origin"),
            ],
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the dashboard's own fetch is same-origin and must pass"
    );
}

/// Cross-origin READS are not forgery (the browser blocks reading the response
/// via CORS), and `curl`/PowerShell send no Origin at all — neither may be
/// broken by the CSRF rule.
#[tokio::test]
async fn cross_origin_read_is_not_blocked_by_the_csrf_rule() {
    let app = build_router(test_state().await);
    let resp = app
        .oneshot(req(
            "GET",
            "/api/v1/status",
            "127.0.0.1:54321",
            &[("origin", "https://evil.example")],
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET is not a mutating method — the CSRF rule must not touch it"
    );
}

// ---------------------------------------------------------------------------
// The permanent guard against "we forgot route #3": enumerate every route
// registered in router.rs and assert each one denies an internet-sourced
// request that carries no Access JWT. A route added tomorrow is covered with
// no edit here — that is the whole point (#273).
// ---------------------------------------------------------------------------

/// Every path passed to `.route(...)` in `router.rs`, with axum path params
/// (`{id}`) filled in so the URI parses.
pub(crate) fn declared_routes() -> Vec<String> {
    let source = include_str!("router.rs");
    let mut out = Vec::new();
    for (idx, _) in source.match_indices(".route(") {
        let rest = &source[idx + ".route(".len()..];
        // The path literal is the first double-quoted token after `.route(`,
        // possibly on the next line for rustfmt-wrapped calls.
        let Some(start) = rest.find('"') else {
            continue;
        };
        // Guard against picking up a quote from a later statement.
        if rest[..start].contains(';') {
            continue;
        }
        let Some(len) = rest[start + 1..].find('"') else {
            continue;
        };
        let path = &rest[start + 1..start + 1 + len];
        if !path.starts_with('/') {
            continue;
        }
        let mut concrete = String::new();
        let mut skipping = false;
        for ch in path.chars() {
            match ch {
                '{' => {
                    skipping = true;
                    concrete.push('1');
                }
                '}' => skipping = false,
                c if !skipping => concrete.push(c),
                _ => {}
            }
        }
        out.push(format!("/api/v1{concrete}"));
    }
    out.sort();
    out.dedup();
    out
}

#[tokio::test]
async fn route_inventory_is_not_empty() {
    let routes = declared_routes();
    assert!(
        routes.len() > 40,
        "route scraper found only {} routes — it stopped matching router.rs, \
         which would make the coverage test below vacuous",
        routes.len()
    );
    assert!(routes.contains(&"/api/v1/status".to_string()));
    assert!(routes.contains(&"/api/v1/ws".to_string()));
    assert!(routes.contains(&"/api/v1/_test/s3-block".to_string()));
}

#[tokio::test]
async fn every_declared_route_denies_an_unauthenticated_internet_request() {
    let mut reachable = Vec::new();
    for path in declared_routes() {
        let app = build_router(test_state().await);
        let resp = app.oneshot(tunneled("GET", &path)).await.unwrap();
        if resp.status() != StatusCode::FORBIDDEN {
            reachable.push(format!("{path} -> {}", resp.status()));
        }
    }
    assert!(
        reachable.is_empty(),
        "#273: these routes answered an internet-sourced request with no Access JWT: {reachable:#?}"
    );
}

/// The SPA itself (served by `fallback_service`, registered AFTER the CORS
/// layer) is the route most easily left ungated — `Router::layer` only wraps
/// routes registered before it, so a layer attached where CORS sits would miss
/// the dashboard HTML/WASM entirely.
#[tokio::test]
async fn the_spa_fallback_is_gated_too() {
    let mut state = test_state().await;
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("index.html"), "<html>dashboard</html>").unwrap();
    state.www_dir = Some(dir.path().to_path_buf());
    let app = build_router(state);
    let resp = app.oneshot(tunneled("GET", "/")).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "#273: the dashboard SPA must be gated, not just /api/v1/*"
    );
}
