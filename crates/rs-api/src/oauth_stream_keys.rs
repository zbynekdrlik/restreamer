//! `GET /endpoints/{id}/oauth-suggest` — server-side OAuth-grant auto-suggest
//! for the edit-endpoint dialog (#199).
//!
//! Given an endpoint, this tells the dashboard which authorized YouTube grant
//! UNIQUELY owns the endpoint's `stream_key` (so the dialog can pre-select it),
//! by matching the key against each grant's owned `liveStreams`
//! `cdn.ingestionInfo.streamName`. The matching runs ON THE SERVER (the browser
//! never receives the raw stream-key inventory), and the response is a tiny
//! verdict: `{ oauth_id, owners, probed_ok }`.
//!
//! Probing every grant's `liveStreams.list` burns YouTube quota, so the
//! per-grant map sits behind a short process-global TTL cache and every probe
//! is accounted against the shared `youtube_quota_tracker`.

use std::sync::{LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use rs_youtube::streams::LiveStream;
use serde::Serialize;

use crate::state::AppState;

/// One grant's owned ingestion stream names.
#[derive(Debug, Clone, PartialEq)]
pub struct OauthStreamKeys {
    pub oauth_id: i64,
    pub stream_names: Vec<String>,
}

/// The auto-suggest verdict returned to the dashboard.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct OauthSuggestResponse {
    /// The grant that UNIQUELY owns this endpoint's stream key, if exactly one
    /// does; `None` when zero or more than one grant owns it.
    pub oauth_id: Option<i64>,
    /// How many authorized grants own this stream key (0, 1, or more).
    pub owners: usize,
    /// `true` only when EVERY authorized grant was probed successfully. When
    /// `false`, a "no owner" verdict is unreliable (a probe failed), so the
    /// dashboard must not tell the operator to re-authorize.
    pub probed_ok: bool,
}

/// How long a probed grant→names map stays fresh. Short enough that a
/// freshly-authorized grant shows up within a minute, long enough that
/// repeated dialog opens do not re-probe every grant against YouTube.
const CACHE_TTL: Duration = Duration::from_secs(60);

/// `(probed_at, per-grant map, probed_ok)` — `None` until the first probe.
type CacheEntry = Option<(Instant, Vec<OauthStreamKeys>, bool)>;

static CACHE: LazyLock<Mutex<CacheEntry>> = LazyLock::new(|| Mutex::new(None));

fn cache_lock() -> std::sync::MutexGuard<'static, CacheEntry> {
    CACHE.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Extract the non-empty `cdn.ingestionInfo.streamName` values from a grant's
/// live streams. Pure — unit-tested.
pub fn stream_names_of(streams: &[LiveStream]) -> Vec<String> {
    streams
        .iter()
        .filter_map(|s| {
            s.cdn
                .as_ref()
                .and_then(|c| c.ingestion_info.as_ref())
                .and_then(|i| i.stream_name.clone())
        })
        .filter(|n| !n.is_empty())
        .collect()
}

/// Number of grants that own `key`. Pure — unit-tested.
pub fn count_owners(key: &str, grants: &[OauthStreamKeys]) -> usize {
    if key.is_empty() {
        return 0;
    }
    grants
        .iter()
        .filter(|g| g.stream_names.iter().any(|n| n == key))
        .count()
}

/// Return the oauth grant that UNIQUELY owns `key`, or `None` when zero or more
/// than one grant owns it (ambiguous => no auto-suggest). Pure — unit-tested,
/// and the SAME function the handler uses in production.
pub fn suggest_oauth_for_key(key: &str, grants: &[OauthStreamKeys]) -> Option<i64> {
    if key.is_empty() {
        return None;
    }
    let mut matches = grants
        .iter()
        .filter(|g| g.stream_names.iter().any(|n| n == key))
        .map(|g| g.oauth_id);
    let first = matches.next()?;
    match matches.next() {
        Some(_) => None, // ambiguous: >1 grant owns this key
        None => Some(first),
    }
}

/// Build the per-grant stream-key map by probing each authorized grant.
/// Returns `(map, probed_ok)`. A grant whose probe fails (quota exhausted or a
/// YouTube error) is OMITTED from the map — never counted as "owns nothing" —
/// and clears `probed_ok`, so a downstream "no owner" verdict stays honest.
async fn build_map(pool: &sqlx::SqlitePool) -> (Vec<OauthStreamKeys>, bool) {
    let oauths = match rs_core::db::youtube_oauth::list_oauths(pool).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("oauth_suggest: list_oauths failed: {e}");
            return (vec![], false);
        }
    };
    let mut out = Vec::new();
    let mut probed_ok = true;
    for o in oauths {
        // Skip the migration-seeded empty placeholder grant.
        if o.refresh_token.is_empty() {
            continue;
        }
        // Account each `liveStreams.list` against the shared daily quota.
        if crate::delivery_status::youtube_quota_tracker()
            .acquire(1)
            .is_err()
        {
            tracing::warn!(
                "oauth_suggest: quota exhausted, skipping grant '{}'",
                o.label
            );
            probed_ok = false;
            continue;
        }
        match rs_youtube::streams::list_streams_for_label(pool, &o.label).await {
            Ok(streams) => out.push(OauthStreamKeys {
                oauth_id: o.id,
                stream_names: stream_names_of(&streams),
            }),
            Err(e) => {
                tracing::warn!("oauth_suggest: probe failed for label '{}': {e}", o.label);
                probed_ok = false;
            }
        }
    }
    (out, probed_ok)
}

/// The grant→names map, TTL-cached to bound YouTube quota.
async fn cached_map(pool: &sqlx::SqlitePool) -> (Vec<OauthStreamKeys>, bool) {
    if let Some((at, map, ok)) = cache_lock().as_ref() {
        if at.elapsed() < CACHE_TTL {
            return (map.clone(), *ok);
        }
    }
    let (map, ok) = build_map(pool).await;
    *cache_lock() = Some((Instant::now(), map.clone(), ok));
    (map, ok)
}

/// `GET /endpoints/{id}/oauth-suggest`. Computes the auto-suggest verdict for
/// ONE endpoint's stream key server-side, so the raw stream-key inventory never
/// leaves the box. 404 when the endpoint does not exist.
pub async fn endpoint_oauth_suggest(
    State(state): State<AppState>,
    axum::extract::Path(id): axum::extract::Path<i64>,
) -> Result<Json<OauthSuggestResponse>, StatusCode> {
    let endpoint = rs_core::db::v2::get_endpoint_config(&state.pool, id)
        .await
        .map_err(|e| {
            tracing::error!("oauth_suggest: endpoint lookup failed: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    let (map, probed_ok) = cached_map(&state.pool).await;
    let key = &endpoint.stream_key;
    Ok(Json(OauthSuggestResponse {
        oauth_id: suggest_oauth_for_key(key, &map),
        owners: count_owners(key, &map),
        probed_ok,
    }))
}
