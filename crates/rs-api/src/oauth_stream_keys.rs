//! `GET /youtube/oauths/stream-keys` — per-grant owned ingestion stream names.
//!
//! Powers the edit-endpoint dialog's OAuth auto-suggest (#199): the dashboard
//! matches an endpoint's `stream_key` against each authorized grant's owned
//! `liveStreams` `cdn.ingestionInfo.streamName` values to pre-select the grant
//! that actually owns the stream key.
//!
//! Probing every grant's `liveStreams.list` on each dialog open would burn
//! YouTube quota, so results sit behind a short process-global TTL cache.

use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use rs_youtube::streams::LiveStream;
use serde::Serialize;

use crate::state::AppState;

/// One grant's owned ingestion stream names.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct OauthStreamKeys {
    pub oauth_id: i64,
    pub stream_names: Vec<String>,
}

/// How long a probed stream-key map stays fresh. Short enough that a
/// freshly-authorized grant shows up within a minute, long enough that
/// repeated dialog opens do not re-probe every grant against YouTube.
const CACHE_TTL: Duration = Duration::from_secs(60);

/// `(probed_at, per-grant map)` — `None` until the first probe.
type CacheEntry = Option<(Instant, Vec<OauthStreamKeys>)>;

static CACHE: LazyLock<Mutex<CacheEntry>> = LazyLock::new(|| Mutex::new(None));

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

/// Return the oauth grant that uniquely owns `key`, or `None` when zero or
/// more than one grant owns it (ambiguous => no auto-suggest). Pure —
/// unit-tested.
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
/// A grant whose probe fails contributes an empty list rather than aborting
/// the whole response — a partial map still lets the dialog auto-suggest.
async fn build_map(pool: &sqlx::SqlitePool) -> Vec<OauthStreamKeys> {
    let oauths = match rs_core::db::youtube_oauth::list_oauths(pool).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("oauth_stream_keys: list_oauths failed: {e}");
            return vec![];
        }
    };
    let mut out = Vec::with_capacity(oauths.len());
    for o in oauths {
        // Skip the migration-seeded empty placeholder grant.
        if o.refresh_token.is_empty() {
            continue;
        }
        let names = match rs_youtube::streams::list_streams_for_label(pool, &o.label).await {
            Ok(streams) => stream_names_of(&streams),
            Err(e) => {
                tracing::warn!(
                    "oauth_stream_keys: probe failed for label '{}': {e}",
                    o.label
                );
                Vec::new()
            }
        };
        out.push(OauthStreamKeys {
            oauth_id: o.id,
            stream_names: names,
        });
    }
    out
}

/// `GET /youtube/oauths/stream-keys`. TTL-cached to bound YouTube quota.
pub async fn list_oauth_stream_keys(State(state): State<AppState>) -> Json<Vec<OauthStreamKeys>> {
    if let Some((at, cached)) = CACHE.lock().unwrap().as_ref() {
        if at.elapsed() < CACHE_TTL {
            return Json(cached.clone());
        }
    }
    let fresh = build_map(&state.pool).await;
    *CACHE.lock().unwrap() = Some((Instant::now(), fresh.clone()));
    Json(fresh)
}
