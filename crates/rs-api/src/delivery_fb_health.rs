//! Facebook ingest-health extraction, audit emission, and the per-endpoint
//! attach/TTL-cache (#166). Structural parity with the YT health pipeline
//! (`delivery_yt_health.rs` + the `attach_yt_health*` helpers in
//! `delivery_status.rs`), kept self-contained here so `delivery_status.rs`
//! stays under the 1000-line CI cap — its loop only adds a 2-line call site.
//!
//! FB silently discards bytes pushed to a persistent key with no bound
//! `live_video`, so the badge is rendered ONLY while an event is delivering
//! (we ARE pushing). In that context "the page has no receiving live_video"
//! IS the failure the operator must see, so it maps to `bad` (red), NOT the
//! neutral grey `noData`. `noData` is reserved only for `PROCESSING` (FB is
//! transcoding an active ingest); UNPUBLISHED / SCHEDULED_* objects do NOT
//! count as "receiving" and fall through to `bad`.
//!
//! KNOWN LIMITATION (discovery approach, #166 review): this inspects the page's
//! currently-receiving `live_video` but does NOT correlate it with the specific
//! endpoint's persistent stream key (a persistent key `FB-<id>-0-<rand>` is not
//! the per-session `live_video` id, so it cannot be matched directly). If a
//! SECOND session (a phone Live Producer, the CI broadcast) is live on the same
//! page while our prod push is being discarded, the page reads `good` — a false
//! green. This is the documented trade-off of the discovery approach ("fragile
//! when multiple concurrent sessions exist on one page"); the deferred
//! create+bind-broadcast follow-up resolves it by polling the exact bound id.
//! For the church's single-session-per-page reality it is robust.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::config::FacebookConfig;
use rs_core::models::FacebookHealth;
use rs_facebook::live_video::LiveVideo;
use tokio::sync::mpsc::Sender;

/// Map the page's `live_videos` to a single `FacebookHealth`, parity with YT's
/// `good`/`bad`/`noData`/`unknown`. Pure — the unit-tested state machine.
pub fn classify_fb_health(videos: &[LiveVideo]) -> FacebookHealth {
    // 1. A currently-live object wins. Inspect its master ingest stream.
    if let Some(live) = videos.iter().find(|v| v.is_live()) {
        if let Some(health) = live.master_ingest().and_then(|i| i.stream_health.as_ref()) {
            let bitrate = health.video_bitrate.unwrap_or(0.0);
            let framerate = health.video_framerate.unwrap_or(0.0);
            if bitrate > 0.0 && framerate > 0.0 {
                return FacebookHealth {
                    status: "LIVE".into(),
                    health: "good".into(),
                    video_bitrate_kbps: Some((bitrate / 1000.0).round() as i64),
                    resolution: health.resolution(),
                    frame_rate: Some(format!("{framerate:.2}")),
                    age_secs: 0,
                    error: None,
                };
            }
        }
        // LIVE object exists but FB measures no media flowing → degraded (red).
        return FacebookHealth {
            status: "LIVE".into(),
            health: "bad".into(),
            video_bitrate_kbps: None,
            resolution: None,
            frame_rate: None,
            age_secs: 0,
            error: None,
        };
    }

    // 2. `PROCESSING` = FB is transcoding an active ingest → transient grey.
    // Deliberately NARROW (only PROCESSING): a church page almost always carries
    // a `SCHEDULED_*` "next Sunday" broadcast and may carry abandoned
    // `UNPUBLISHED` Live Producer drafts. Treating those as `noData` would mask
    // the silent-discard RED with grey while we are actively delivering (#166
    // review). Since the badge renders ONLY while delivering, a page that shows
    // no LIVE object and only scheduled/unpublished ones means FB is NOT
    // ingesting our push — that is the failure, so it falls through to `bad`.
    if videos
        .iter()
        .any(|v| v.status.as_deref() == Some("PROCESSING"))
    {
        return FacebookHealth {
            status: "PROCESSING".into(),
            health: "noData".into(),
            video_bitrate_kbps: None,
            resolution: None,
            frame_rate: None,
            age_secs: 0,
            error: None,
        };
    }

    // 3. No receiving live_video — the silent-discard case (incl. a page whose
    // only objects are scheduled/unpublished/VOD). RED.
    FacebookHealth {
        status: "NO_LIVE_VIDEO".into(),
        health: "bad".into(),
        video_bitrate_kbps: None,
        resolution: None,
        frame_rate: None,
        age_secs: 0,
        error: None,
    }
}

/// A health snapshot the probe could not run — parity with YT's error snapshots.
fn unknown_fb(status: &str, error: &str) -> FacebookHealth {
    FacebookHealth {
        status: status.to_string(),
        health: "unknown".into(),
        video_bitrate_kbps: None,
        resolution: None,
        frame_rate: None,
        age_secs: 0,
        error: Some(error.to_string()),
    }
}

/// Decide whether `FacebookStatusChanged` should fire, keyed on the mapped
/// `health` value (parity with `issue_changed_action`). `Some((Action,from,to))`
/// when `prior != current`.
pub fn status_changed_action(
    prior: Option<&str>,
    current: Option<&str>,
) -> Option<(Action, Option<String>, Option<String>)> {
    if prior == current {
        return None;
    }
    Some((
        Action::FacebookStatusChanged,
        prior.map(|s| s.to_string()),
        current.map(|s| s.to_string()),
    ))
}

/// Emit one `FacebookStatusChanged` row when `prior != current`. Best-effort
/// (drops silently if the audit channel is full). Returns `true` iff sent.
pub async fn record_and_maybe_emit_fb(
    prior: Option<&str>,
    current: Option<&str>,
    endpoint_alias: &str,
    audit_tx: &Sender<AuditRow>,
) -> bool {
    let Some((action, from, to)) = status_changed_action(prior, current) else {
        return false;
    };
    let row = AuditRow {
        severity: Severity::Info,
        source: Source::System,
        event_id: None,
        instance_id: None,
        endpoint: Some(endpoint_alias.to_string()),
        action,
        detail: serde_json::json!({ "from": from, "to": to }),
        ts_override: None,
    };
    audit_tx.send(row).await.is_ok()
}

/// Adaptive cache TTL: 60 s when healthy (good, no error), 15 s otherwise so a
/// degradation surfaces within one poll — parity with `ttl_for_health`.
pub fn ttl_for_fb_health(h: &FacebookHealth) -> Duration {
    if h.health == "good" && h.error.is_none() {
        Duration::from_secs(60)
    } else {
        Duration::from_secs(15)
    }
}

fn fb_health_cache() -> &'static dashmap::DashMap<i64, (Instant, FacebookHealth)> {
    static C: OnceLock<dashmap::DashMap<i64, (Instant, FacebookHealth)>> = OnceLock::new();
    C.get_or_init(dashmap::DashMap::new)
}

/// Probe FB Graph for the configured page's ingest health and attach it to
/// `metrics.facebook_health`. Errors are mapped to `FacebookHealth.error`
/// (never propagated) so the probe never breaks the surrounding loop. Ships
/// dark: when FB is not configured, surfaces `unconfigured`/`fb_not_configured`.
pub async fn attach_fb_health(
    fb: &FacebookConfig,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
) {
    // Reached only when FB monitoring is enabled (the call site gates on
    // `facebook.enabled`), so an empty token/page here is a real misconfig.
    if fb.page_access_token.trim().is_empty() || fb.page_id.trim().is_empty() {
        metrics.facebook_health = Some(unknown_fb("unconfigured", "fb_not_configured"));
        return;
    }
    match rs_facebook::live_video::fetch_page_live_videos(
        &fb.page_access_token,
        &fb.page_id,
        &fb.api_version,
    )
    .await
    {
        Ok(videos) => metrics.facebook_health = Some(classify_fb_health(&videos)),
        // Graph returns HTTP 400 for these; the meaning is in `error.code`, not
        // the HTTP status — so map on the code (#166 review).
        Err(rs_facebook::FacebookError::Api { code, .. }) => {
            let reason = match code {
                Some(190) => "oauth_invalid",
                Some(10) | Some(200..=299) => "permission",
                Some(4) | Some(17) | Some(32) | Some(613) => "rate_limited",
                _ => "fb_api_error",
            };
            metrics.facebook_health = Some(unknown_fb("unknown", reason));
        }
        Err(e) => {
            // `e` is already URL/token-stripped by rs_facebook, but the reason
            // string is a fixed label anyway.
            tracing::warn!(page_id = %fb.page_id, error = %e, "fb_health probe failed");
            metrics.facebook_health = Some(unknown_fb("unknown", "probe_error"));
        }
    }
}

/// Adaptive-TTL wrapper over `attach_fb_health`, keyed per endpoint id. Emits
/// one `FacebookStatusChanged` audit row on the slow path when the mapped
/// `health` value changed — parity with `attach_yt_health_cached`.
///
/// The cache is keyed per endpoint id (not per page) — with the single-page
/// config, N FB endpoints on the one page make N identical Graph calls per TTL.
/// That is acceptable because the church runs a single FB endpoint; a
/// multi-endpoint multi-page setup is the deferred create+bind follow-up.
pub async fn attach_fb_health_cached(
    fb: &FacebookConfig,
    endpoint_id: i64,
    endpoint_alias: &str,
    metrics: &mut rs_core::models::DeliveryEndpointMetrics,
    audit_tx: Option<&Sender<AuditRow>>,
) {
    // ONE lookup: capture the prior health AND serve a still-fresh snapshot.
    let prior_health: Option<String> = if let Some(entry) = fb_health_cache().get(&endpoint_id) {
        let (when, h) = entry.value().clone();
        let age = when.elapsed();
        if age < ttl_for_fb_health(&h) {
            let mut aged = h;
            aged.age_secs = age.as_secs() as i64;
            metrics.facebook_health = Some(aged);
            return;
        }
        Some(h.health)
    } else {
        None
    };

    attach_fb_health(fb, metrics).await;
    if let Some(h) = metrics.facebook_health.as_ref() {
        fb_health_cache().insert(endpoint_id, (Instant::now(), h.clone()));
        // Skip the audit row on a healthy cold start (prior None -> good): only
        // real transitions are operator-interesting (parity with YT keying on
        // top_issue, which is None when healthy). #166 review.
        let suppress_healthy_start = prior_health.is_none() && h.health == "good";
        if let (Some(tx), false) = (audit_tx, suppress_healthy_start) {
            let _ = record_and_maybe_emit_fb(
                prior_health.as_deref(),
                Some(h.health.as_str()),
                endpoint_alias,
                tx,
            )
            .await;
        }
    }
}

/// Test-only: clear the per-endpoint FB health cache (tests reuse endpoint id=1
/// across pools, mirroring `clear_yt_health_cache_for_test`).
#[cfg(test)]
pub fn clear_fb_health_cache_for_test() {
    fb_health_cache().clear();
}

/// Test-only: a minimal `DeliveryEndpointMetrics` for the attach tests.
#[cfg(test)]
fn test_metrics() -> rs_core::models::DeliveryEndpointMetrics {
    rs_core::models::DeliveryEndpointMetrics {
        alias: "fb1".into(),
        alive: true,
        current_chunk_id: 0,
        bytes_processed_total: 0,
        chunks_processed: 0,
        chunk_delay_secs: 0.0,
        stall_reason: None,
        ffmpeg_restart_count: 0,
        reconnect_count: 0,
        av_skew_ms: 0,
        fast_delay_target_secs: None,
        last_error: None,
        is_fast: false,
        delivery_mode: None,
        rescue_eta_secs: None,
        youtube_health: None,
        facebook_health: None,
        lifecycle: rs_core::models::EndpointLifecycle::Live,
    }
}

#[cfg(test)]
#[path = "delivery_fb_health_tests.rs"]
mod tests;
