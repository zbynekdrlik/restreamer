//! Origin-aware access control — the single gate in front of every route
//! (#70, #273, #337, #339).
//!
//! # Why this exists
//!
//! `streamsnv.newlevel.media` proxies straight to `localhost:8910`, and until
//! this module every one of the ~60 `/api/v1/*` routes, the websocket, the CI
//! test hooks and the dashboard SPA answered an anonymous internet request with
//! a 200. Anyone who knew the hostname could stop the church live stream.
//!
//! # The model (decided in #273 — do not re-derive it)
//!
//! Two layers. Layer 1 is a **Cloudflare Access** application in front of the
//! tunnel: an unauthenticated request never reaches the box at all, and because
//! it covers the whole hostname, "we forgot to protect one route" is impossible
//! there. This module is layer 2: it re-verifies the signed Access assertion
//! *inside* the app, so a second ingress rule, a port-forward on the router, a
//! second `cloudflared`, or a revived tunnel on the other box cannot bypass the
//! edge.
//!
//! Every request is classified [`Origin::Local`] or [`Origin::Internet`]:
//!
//! | class | rule | members |
//! |---|---|---|
//! | loopback-only | loopback peer AND no forwarded header | `/api/v1/_test/*` |
//! | gated (default) | `Local`, OR a valid Access JWT | everything else, incl. the SPA, `/ws`, `/diag/dump` |
//! | allowlist | always allowed | *(empty — deliberately)* |
//!
//! The exception list is EMPTY, status reads included. A public read would need
//! a Bypass application at the edge plus a path list here plus an SPA that
//! works with half the API — i.e. exactly the "which paths are on the list"
//! maintenance that produced this bug. One model, one rule.
//!
//! # Two invariants that must never regress
//!
//! 1. **LAN is never authenticated.** When Cloudflare is down, identity is
//!    down, the JWKS is unreachable or the building's internet is out, the
//!    operator opens `http://stream.lan:8910` and everything works. The
//!    `Local` branch performs **no network I/O whatsoever** — there is nothing
//!    for it to hang on. Do not "improve" that by moving a fetch above the
//!    classification.
//! 2. **RFC1918 counts as Local.** `scripts/soak-mini.ps1` targets
//!    `http://10.77.9.204:8910` and the self-hosted CI runner lives on the box;
//!    neither carries a credential and neither ever should.
//!
//! # Why the origin test is not just "is the peer loopback?"
//!
//! `cloudflared` terminates the tunnel and connects to us from `127.0.0.1`, so
//! a request from the public internet passes a naive loopback check (#205). A
//! proxied request always carries at least one forwarded header
//! ([`PROXY_HEADERS`]) that a genuinely-local request never sets — so the peer
//! address and the headers are BOTH required. The peer half additionally
//! catches a direct port-forward, which carries no headers at all.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{ConnectInfo, Request, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use rs_core::config::AccessConfig;
use serde::Deserialize;
use tokio::sync::{Mutex, RwLock};

use crate::state::AppState;

/// Forwarded-header names set by reverse proxies (Cloudflare tunnel, nginx, …).
/// A genuinely-local request sets none of these. Issue #205.
pub const PROXY_HEADERS: [&str; 3] = ["cf-connecting-ip", "x-forwarded-for", "x-forwarded-host"];

/// Header Cloudflare Access injects with the signed assertion.
const JWT_HEADER: &str = "cf-access-jwt-assertion";
/// Cookie Access sets in the browser — also what rides along on the `/ws`
/// upgrade and on every same-origin `fetch` from the SPA.
const JWT_COOKIE: &str = "CF_Authorization";

/// Path prefix of the loopback-only class (#337).
const LOOPBACK_ONLY_PREFIX: &str = "/api/v1/_test/";

/// How long a fetched key set is served before a refresh is attempted. On a
/// failed refresh the last-good set keeps being served indefinitely — a
/// transient network blip must never lock the operator out.
const JWKS_MAX_AGE: Duration = Duration::from_secs(6 * 60 * 60);

/// Where a request came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// The box itself or the church LAN. Never authenticated.
    Local,
    /// Anything else — through the tunnel, or a direct public peer.
    Internet,
}

/// Emergency levers, settable in `api.access.mode` with no rebuild.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    /// Internet-sourced requests need a valid Access JWT.
    Enforce,
    /// Classify and log, allow everything. Behaviour identical to pre-#273.
    LogOnly,
    /// Reject every internet-sourced request, valid JWT or not.
    LanOnly,
}

impl AccessMode {
    fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "log_only" | "log-only" => Self::LogOnly,
            "lan_only" | "lan-only" => Self::LanOnly,
            "enforce" => Self::Enforce,
            other => {
                tracing::warn!(
                    "api.access.mode = {other:?} is not one of enforce|log_only|lan_only — \
                     falling back to enforce (default-deny)"
                );
                Self::Enforce
            }
        }
    }
}

/// True when the peer is on a network we treat as ours: loopback, RFC1918,
/// CGNAT, or link-local. Everything else is the public internet.
fn is_private_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                // 100.64.0.0/10 — CGNAT, which is also the Tailscale range the
                // operator's own machines use.
                || (v4.octets()[0] == 100 && (64..128).contains(&v4.octets()[1]))
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                // fc00::/7 unique-local and fe80::/10 link-local.
                || (v6.segments()[0] & 0xfe00) == 0xfc00
                || (v6.segments()[0] & 0xffc0) == 0xfe80
                // IPv4-mapped (::ffff:10.0.0.1) — classify by the inner v4.
                || v6.to_ipv4_mapped().map(|v4| is_private_ip(IpAddr::V4(v4))) == Some(true)
        }
    }
}

fn has_proxy_header(headers: &HeaderMap) -> bool {
    PROXY_HEADERS.iter().any(|h| headers.contains_key(*h))
}

/// Classify a request.
///
/// `peer` is `None` only in unit tests driving the router through
/// `tower::ServiceExt::oneshot` — production always wires it via
/// `into_make_service_with_connect_info::<SocketAddr>()` (`lib.rs::serve`, both
/// the HTTP and the HTTPS listener). A missing peer is treated as loopback
/// because it is NOT attacker-controllable: a client cannot strip an extension
/// the server itself inserts. The forwarded-header half of the rule still
/// applies, so a synthetic internet request is still classified correctly
/// without a peer address.
pub fn classify(peer: Option<&SocketAddr>, headers: &HeaderMap) -> Origin {
    if has_proxy_header(headers) {
        return Origin::Internet;
    }
    match peer {
        Some(addr) if !is_private_ip(addr.ip()) => Origin::Internet,
        _ => Origin::Local,
    }
}

/// The strict predicate for the loopback-only class: the request must come from
/// the box ITSELF. This is the rule #205/PR #314 proved in production for
/// `/diag/dump`, lifted here so there is exactly one copy of it.
///
/// `/_test/s3-block` can starve the delivery cache and kill the live stream, so
/// it must be unreachable even for a *logged-in* operator on a remote device —
/// only from the same machine. CI satisfies this: every `_test` call in ci.yml
/// goes to `http://127.0.0.1:8910`.
///
/// A `#[cfg(feature = "…")]` compile-out would be stronger, but the E2E jobs
/// run against the RELEASE binary CI deploys to stream.lan, so the routes must
/// exist in that binary — hence a runtime gate. Do not "simplify" this into a
/// `#[cfg]`.
pub fn is_genuinely_local(peer: Option<&SocketAddr>, headers: &HeaderMap) -> bool {
    if has_proxy_header(headers) {
        return false;
    }
    match peer {
        Some(addr) => addr.ip().is_loopback(),
        None => true,
    }
}

// ---------------------------------------------------------------------------
// CSRF (#339)
// ---------------------------------------------------------------------------

fn is_mutating(method: &Method) -> bool {
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

/// Strip the scheme from an `Origin` value, leaving `host[:port]`.
fn origin_authority(origin: &str) -> Option<&str> {
    let rest = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))?;
    if rest.is_empty() { None } else { Some(rest) }
}

/// The host this request believes it was addressed to. Behind the tunnel the
/// browser's `Origin` is the public hostname, so `X-Forwarded-Host` (which
/// cloudflared sets) is checked as well as `Host`.
fn request_authorities(headers: &HeaderMap) -> Vec<String> {
    let mut out = Vec::new();
    for name in ["host", "x-forwarded-host"] {
        if let Some(v) = headers.get(name).and_then(|v| v.to_str().ok()) {
            out.push(v.trim().to_ascii_lowercase());
        }
    }
    out
}

/// True when `origin` addresses the same host this request was sent to.
///
/// Used by the CORS layer so cross-origin READS are refused as well: with
/// `allow_origin(Any)` any page on the internet could read the dashboard's
/// state off a LAN box.
pub fn origin_is_same_site(origin: &str, headers: &HeaderMap) -> bool {
    match origin_authority(origin) {
        Some(a) => request_authorities(headers).contains(&a.to_ascii_lowercase()),
        None => false,
    }
}

/// Reject a cross-site mutating request.
///
/// A bodyless cross-origin `POST` is a CORS *simple* request: no preflight is
/// sent, the browser fires it and attaches cookies, and the attacker does not
/// need to read the response — the side effect (stream stopped) already
/// happened. CORS therefore cannot be the barrier; this check is.
///
/// It deliberately applies to LOCAL requests too. The scenario in #339 is the
/// operator's own browser, on the church LAN, visiting a malicious page — that
/// request has a LAN peer and no forwarded headers, i.e. it is `Local`. Gating
/// it only for `Internet` would leave the actual attack open.
///
/// Non-browser callers (curl, `Invoke-RestMethod`, the Playwright *request*
/// API, the CI runner) send neither header, so nothing in CI is affected.
fn csrf_violation(headers: &HeaderMap, method: &Method) -> Option<String> {
    if !is_mutating(method) {
        return None;
    }
    if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
        if site.eq_ignore_ascii_case("cross-site") {
            return Some("Sec-Fetch-Site: cross-site".to_string());
        }
    }
    // No Origin header at all: curl, Invoke-RestMethod, the CI runner, the
    // Playwright request API. Not a browser, so not forgery.
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok())?;
    let authorities = request_authorities(headers);
    // No Host at all (HTTP/1.0-style) — nothing to compare against; the
    // Sec-Fetch-Site check above and the origin gate are the remaining
    // barriers.
    if authorities.is_empty() {
        return None;
    }
    match origin_authority(origin) {
        Some(a) if authorities.contains(&a.to_ascii_lowercase()) => None,
        // `Origin: null` (sandboxed iframe, some redirects) lands here too, and
        // is correctly refused for a mutating request.
        _ => Some(format!("Origin {origin} does not match {authorities:?}")),
    }
}

// ---------------------------------------------------------------------------
// Access JWT verification
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    n: String,
    e: String,
}

/// The claims we care about. `aud`, `iss`, `exp` and `nbf` are validated by
/// `jsonwebtoken` itself against [`Validation`]; `email` is what we attribute
/// the audit row to.
#[derive(Debug, Deserialize)]
pub struct AccessClaims {
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub sub: Option<String>,
    /// Set instead of `email` when the caller authenticated with an Access
    /// service token.
    #[serde(default)]
    pub common_name: Option<String>,
}

impl AccessClaims {
    /// Human-readable identity for the audit log.
    pub fn identity(&self) -> String {
        self.email
            .clone()
            .or_else(|| self.common_name.clone())
            .or_else(|| self.sub.clone())
            .unwrap_or_else(|| "unknown".to_string())
    }
}

#[derive(Default)]
struct KeyCache {
    keys: HashMap<String, DecodingKey>,
    fetched_at: Option<Instant>,
}

/// Verifies Cloudflare Access assertions, caching the team's JWKS.
pub struct AccessGate {
    mode: AccessMode,
    audiences: Vec<String>,
    issuer: String,
    jwks_url: String,
    http: reqwest::Client,
    cache: RwLock<KeyCache>,
    /// Single-flight guard so a burst of internet requests triggers one fetch.
    fetching: Mutex<()>,
}

impl AccessGate {
    pub fn from_config(cfg: &AccessConfig) -> Arc<Self> {
        let team = cfg.team_domain.trim().trim_end_matches('/').to_string();
        Arc::new(Self {
            mode: AccessMode::parse(&cfg.mode),
            audiences: cfg.aud.iter().map(|a| a.trim().to_string()).collect(),
            issuer: format!("https://{team}"),
            jwks_url: format!("https://{team}/cdn-cgi/access/certs"),
            http: reqwest::Client::builder()
                // Never let a slow identity endpoint hold a request open.
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_default(),
            cache: RwLock::new(KeyCache::default()),
            fetching: Mutex::new(()),
        })
    }

    pub fn mode(&self) -> AccessMode {
        self.mode
    }

    /// Warm the key cache at startup and refresh it every [`JWKS_MAX_AGE`], so
    /// the first remote operator of the day does not pay the fetch. Failures
    /// are logged and retried; the last-good key set is never discarded.
    ///
    /// Called from `lib.rs::serve` only — unit tests build the router without
    /// it, which is why the request path can also refresh lazily.
    pub fn spawn_refresher(self: &Arc<Self>) {
        if self.mode == AccessMode::LanOnly || self.audiences.is_empty() {
            tracing::info!(
                "access: JWKS refresher not started (mode={:?}, {} audiences configured)",
                self.mode,
                self.audiences.len()
            );
            return;
        }
        let gate = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                gate.refresh_keys().await;
                tokio::time::sleep(JWKS_MAX_AGE).await;
            }
        });
    }

    async fn refresh_keys(&self) {
        let _guard = self.fetching.lock().await;
        let fetched = match self.http.get(&self.jwks_url).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(ok) => ok.json::<Jwks>().await,
                Err(e) => {
                    tracing::warn!("access: JWKS fetch {} returned {e}", self.jwks_url);
                    return;
                }
            },
            Err(e) => {
                tracing::warn!("access: JWKS fetch {} failed: {e}", self.jwks_url);
                return;
            }
        };
        let jwks = match fetched {
            Ok(j) => j,
            Err(e) => {
                tracing::warn!(
                    "access: JWKS body from {} did not parse: {e}",
                    self.jwks_url
                );
                return;
            }
        };
        let mut keys = HashMap::new();
        for jwk in &jwks.keys {
            match DecodingKey::from_rsa_components(&jwk.n, &jwk.e) {
                Ok(key) => {
                    keys.insert(jwk.kid.clone(), key);
                }
                Err(e) => tracing::warn!("access: JWKS key {} unusable: {e}", jwk.kid),
            }
        }
        if keys.is_empty() {
            // Keep the last-good set rather than blanking it.
            tracing::warn!(
                "access: JWKS from {} contained no usable keys",
                self.jwks_url
            );
            return;
        }
        let count = keys.len();
        let mut cache = self.cache.write().await;
        cache.keys = keys;
        cache.fetched_at = Some(Instant::now());
        tracing::info!(
            "access: cached {count} Access signing keys from {}",
            self.jwks_url
        );
    }

    /// Look up a signing key, refreshing once if the `kid` is unknown or the
    /// cache has aged out. On a failed refresh the last-good key set is served
    /// — a network blip must not lock a remote operator out.
    async fn key_for(&self, kid: &str) -> Option<DecodingKey> {
        {
            let cache = self.cache.read().await;
            let fresh = cache.fetched_at.is_some_and(|t| t.elapsed() < JWKS_MAX_AGE);
            if fresh {
                if let Some(key) = cache.keys.get(kid) {
                    return Some(key.clone());
                }
            }
        }
        self.refresh_keys().await;
        let cache = self.cache.read().await;
        cache.keys.get(kid).cloned()
    }

    /// Verify a Cloudflare Access assertion. Returns the claims, or a short
    /// reason suitable for a WARN log (never the token itself).
    pub async fn verify(&self, token: &str) -> Result<AccessClaims, String> {
        if self.audiences.is_empty() {
            return Err("no api.access.aud configured — cannot verify".to_string());
        }
        let header = decode_header(token).map_err(|e| format!("malformed token: {e}"))?;
        if header.alg != Algorithm::RS256 {
            return Err(format!("unexpected algorithm {:?}", header.alg));
        }
        let kid = header.kid.ok_or_else(|| "token has no kid".to_string())?;
        let key = self
            .key_for(&kid)
            .await
            .ok_or_else(|| format!("no Access signing key for kid {kid}"))?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&self.audiences);
        validation.set_issuer(&[&self.issuer]);
        validation.validate_exp = true;
        validation.validate_nbf = true;

        decode::<AccessClaims>(token, &key, &validation)
            .map(|data| data.claims)
            .map_err(|e| format!("rejected: {e}"))
    }

    /// Build a gate with a preloaded key set — test seam, so unit tests can
    /// exercise the real RS256 verification path without any network I/O.
    #[cfg(test)]
    pub fn for_test(
        mode: AccessMode,
        audiences: &[&str],
        issuer: &str,
        keys: Vec<(String, DecodingKey)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            mode,
            audiences: audiences.iter().map(|s| s.to_string()).collect(),
            issuer: issuer.to_string(),
            jwks_url: "http://127.0.0.1:1/never-fetched".to_string(),
            http: reqwest::Client::new(),
            cache: RwLock::new(KeyCache {
                keys: keys.into_iter().collect(),
                fetched_at: Some(Instant::now()),
            }),
            fetching: Mutex::new(()),
        })
    }
}

/// Pull the assertion out of the header, falling back to the cookie (which is
/// what a plain browser navigation and the `/ws` upgrade carry).
fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(JWT_HEADER).and_then(|v| v.to_str().ok()) {
        let v = v.trim();
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    let cookies = headers.get(header::COOKIE)?.to_str().ok()?;
    for pair in cookies.split(';') {
        let pair = pair.trim();
        if let Some(value) = pair
            .strip_prefix(JWT_COOKIE)
            .and_then(|r| r.strip_prefix('='))
        {
            if !value.is_empty() {
                return Some(value.to_string());
            }
        }
    }
    None
}

/// What the gate decided, before the mode lever is applied.
#[derive(Debug)]
pub enum Decision {
    Allow {
        origin: Origin,
        identity: Option<String>,
    },
    Deny {
        origin: Origin,
        reason: &'static str,
        detail: String,
    },
}

/// The whole policy, in one place, in evaluation order.
pub async fn decide(
    gate: &AccessGate,
    peer: Option<&SocketAddr>,
    headers: &HeaderMap,
    method: &Method,
    path: &str,
) -> Decision {
    let origin = classify(peer, headers);

    // 1. Loopback-only class (#337) — stricter than Local; a LAN peer or a
    //    logged-in remote operator is still refused.
    if path.starts_with(LOOPBACK_ONLY_PREFIX) && !is_genuinely_local(peer, headers) {
        return Decision::Deny {
            origin,
            reason: "loopback_only",
            detail: format!("{path} is reachable only from the box itself"),
        };
    }

    // 2. CSRF (#339) — applies to LOCAL requests too; see `csrf_violation`.
    if let Some(detail) = csrf_violation(headers, method) {
        return Decision::Deny {
            origin,
            reason: "csrf",
            detail,
        };
    }

    // 3. Local is done here. NO NETWORK I/O ON THIS PATH, EVER — that is what
    //    keeps the church LAN working when Cloudflare, the tunnel or the
    //    building's internet is down.
    if origin == Origin::Local {
        return Decision::Allow {
            origin,
            identity: None,
        };
    }

    if gate.mode() == AccessMode::LanOnly {
        return Decision::Deny {
            origin,
            reason: "lan_only",
            detail: "api.access.mode = lan_only rejects all internet-sourced requests".to_string(),
        };
    }

    let Some(token) = extract_token(headers) else {
        return Decision::Deny {
            origin,
            reason: "no_access_token",
            detail: format!(
                "internet-sourced request with neither {JWT_HEADER} nor a {JWT_COOKIE} cookie"
            ),
        };
    };

    match gate.verify(&token).await {
        Ok(claims) => Decision::Allow {
            origin,
            identity: Some(claims.identity()),
        },
        Err(detail) => Decision::Deny {
            origin,
            reason: "invalid_access_token",
            detail,
        },
    }
}

/// State threaded into the middleware: the gate plus the app state (for the
/// audit trail).
#[derive(Clone)]
pub struct AccessCtx {
    pub gate: Arc<AccessGate>,
    pub state: AppState,
}

fn forbidden(reason: &str) -> Response {
    (
        StatusCode::FORBIDDEN,
        [(header::CONTENT_TYPE, "application/json")],
        Body::from(format!(
            r#"{{"error":"forbidden","reason":"{reason}","hint":"open the dashboard from the church LAN, or sign in through Cloudflare Access"}}"#
        )),
    )
        .into_response()
}

/// The router-wide access middleware.
///
/// Attached at the very END of `build_router`, AFTER `fallback_service`:
/// `Router::layer` only wraps routes registered before it, so a layer placed
/// where the CORS layer sits would leave the dashboard SPA completely ungated
/// — which is the failure this whole PR exists to prevent.
pub async fn access_middleware(
    State(ctx): State<AccessCtx>,
    request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0);
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    let decision = decide(&ctx.gate, peer.as_ref(), request.headers(), &method, &path).await;

    match decision {
        Decision::Allow { origin, identity } => {
            if origin == Origin::Internet && is_mutating(&method) {
                let who = identity.clone().unwrap_or_else(|| "unknown".to_string());
                tracing::info!("access: remote {method} {path} by {who}");
                rs_core::audit::record(
                    &ctx.state.audit_tx,
                    rs_core::audit::AuditRow {
                        severity: rs_core::audit::Severity::Info,
                        source: rs_core::audit::Source::Operator,
                        event_id: None,
                        instance_id: None,
                        endpoint: None,
                        action: rs_core::audit::Action::RemoteControlAction,
                        detail: serde_json::json!({
                            "identity": who,
                            "method": method.as_str(),
                            "path": path,
                        }),
                        ts_override: None,
                    },
                );
            }
            next.run(request).await
        }
        Decision::Deny {
            origin,
            reason,
            detail,
        } => {
            // Every refusal is logged with its reason: residual risk #3 in
            // #273 is a LAN client that somehow sends a forwarded header and
            // gets a confusing 403 — this line makes that a 30-second
            // diagnosis instead of a mystery.
            tracing::warn!(
                "access: DENY {method} {path} peer={peer:?} origin={origin:?} reason={reason} ({detail})"
            );
            if ctx.gate.mode() == AccessMode::LogOnly {
                tracing::warn!("access: api.access.mode = log_only — allowing it anyway");
                return next.run(request).await;
            }
            forbidden(reason)
        }
    }
}

#[cfg(test)]
mod tests {
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
        // Flip the last character of the signature segment.
        let mut parts: Vec<String> = token.split('.').map(|s| s.to_string()).collect();
        let sig = parts.pop().unwrap();
        let flipped: String = sig
            .chars()
            .enumerate()
            .map(|(i, c)| if i == 0 && c != 'A' { 'A' } else { c })
            .collect();
        parts.push(flipped);
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
}
