pub mod audit_handlers;
pub mod clock_skew_probe;
pub mod delivery;
pub(crate) mod delivery_audit_mirror;
pub mod delivery_endpoints;
pub mod delivery_handlers;
pub(crate) mod delivery_helpers;
pub mod delivery_live_edge;
pub(crate) mod delivery_monitor;
#[cfg(test)]
mod delivery_reset_tests;
pub(crate) mod delivery_status;
#[cfg(test)]
mod delivery_tests;
mod delivery_youtube;
pub mod delivery_yt_health;
pub(crate) mod diag;
pub mod diagnostics_pacing;
pub mod endpoint_oauth;
pub mod handlers;
pub mod internet_probe;
pub mod metrics_handlers;
pub mod oauth_device;
pub mod obs;
#[cfg(test)]
mod on_vps_ready_tests;
pub mod rescue_video_handlers;
pub mod router;
#[cfg(test)]
mod router_tests;
pub mod s3_handlers;
pub mod state;
pub mod stream_handlers;
pub mod template_handlers;
pub mod uploads_endpoints;
pub mod websocket;
pub mod youtube;

#[cfg(test)]
mod yt_health_extract_tests;

#[cfg(test)]
mod yt_health_test_env;

#[cfg(test)]
mod delivery_status_yt_health_tests;

#[cfg(test)]
mod adaptive_ttl_tests;
#[cfg(test)]
mod multi_label_oauth_tests;
#[cfg(test)]
mod oauth_device_tests;
#[cfg(test)]
mod yt_health_cache_tests;

#[cfg(test)]
mod yt_health_audit_tests;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::info;

use rs_core::db;
use rs_core::models::{DeliveryEndpointMetrics, WsEvent};

use crate::state::AppState;

/// Returns the highest `current_chunk_id` among non-fast endpoints, or 0 if
/// none qualify. Fast (live-edge) endpoints are excluded because their chunk
/// position races ahead of the producer and would poison the global
/// `cache_duration_secs` reading. The cache bar tracks the buffer ahead of
/// the slowest non-fast consumer; that's what operators interpret during the
/// prefill window. Splitting this out as a pure function makes the policy
/// unit-testable.
fn max_non_fast_delivery_chunk(endpoints: &[DeliveryEndpointMetrics]) -> i64 {
    endpoints
        .iter()
        .filter(|m| !m.is_fast)
        .map(|m| m.current_chunk_id)
        .max()
        .unwrap_or(0)
}

/// Middleware that redirects HTTP requests for the configured domain to HTTPS.
/// Requests via IP address or with `x-forwarded-proto: https` pass through.
async fn https_redirect(
    req: axum::extract::Request,
    next: axum::middleware::Next,
    domain: String,
) -> axum::response::Response {
    use axum::response::IntoResponse;

    let forwarded_proto = req
        .headers()
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    if forwarded_proto == "https" {
        return next.run(req).await;
    }

    let host = req
        .headers()
        .get("host")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    let host_name = host.split(':').next().unwrap_or("");
    if host_name == domain {
        let path = req
            .uri()
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");
        let location = format!("https://{domain}{path}");
        axum::response::Redirect::permanent(&location).into_response()
    } else {
        next.run(req).await
    }
}

/// Start the API server on the given address.
/// Returns the actual bound address and a JoinHandle for shutdown coordination.
pub async fn serve(
    state: AppState,
    addr: SocketAddr,
) -> anyhow::Result<(SocketAddr, JoinHandle<()>)> {
    // Spawn delivery status broadcast loop if orchestrator is available
    if let Some(ref orch) = state.delivery_orchestrator {
        let orch = Arc::clone(orch);
        let pool = state.pool.clone();
        let ws_tx = state.ws_tx.clone();
        let cached = Arc::clone(&state.cached_delivery);
        let config = state.config.clone();
        let audit_tx = state.audit_tx.clone();
        tokio::spawn(async move {
            delivery_broadcast_loop(orch, pool, ws_tx, cached, config, audit_tx).await;
        });
    }

    let app = router::build_router(state.clone());

    // Wrap with HTTPS redirect middleware if TLS + domain configured
    let app = if state.config.api.tls {
        if let Some(ref domain) = state.config.api.https_domain {
            let domain = domain.clone();
            app.layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, domain.clone())
            }))
        } else {
            app
        }
    } else {
        app
    };

    // Spawn HTTPS listener if TLS enabled and cert/key files exist
    if state.config.api.tls {
        let cert_path = resolve_tls_path(&state.config.api.tls_cert);
        let key_path = resolve_tls_path(&state.config.api.tls_key);

        if cert_path.exists() && key_path.exists() {
            // Install rustls crypto provider (ring)
            let _ = rustls::crypto::ring::default_provider().install_default();

            let tls_config =
                axum_server::tls_rustls::RustlsConfig::from_pem_file(&cert_path, &key_path).await?;

            let https_addr = SocketAddr::from(([0, 0, 0, 0], state.config.api.https_port));
            info!("HTTPS server listening on {https_addr}");

            let https_app = app.clone();
            tokio::spawn(async move {
                if let Err(e) = axum_server::bind_rustls(https_addr, tls_config)
                    .serve(https_app.into_make_service())
                    .await
                {
                    tracing::error!("HTTPS server error: {e}");
                }
            });
        } else {
            tracing::warn!(
                "TLS enabled but cert/key files not found: {:?} / {:?}",
                cert_path,
                key_path
            );
        }
    }

    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!("API server listening on {local_addr}");

    let handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("API server error: {e}");
        }
    });

    Ok((local_addr, handle))
}

/// Resolve a TLS file path. If relative, resolve relative to the config directory.
fn resolve_tls_path(path: &str) -> std::path::PathBuf {
    let p = std::path::Path::new(path);
    if p.is_absolute() {
        p.to_path_buf()
    } else if let Some(parent) = rs_core::config::Config::default_path().parent() {
        parent.join(p)
    } else {
        p.to_path_buf()
    }
}

/// Background loop that polls delivery metrics every 2 seconds and broadcasts
/// WsEvent::DeliveryStatus to all connected WebSocket clients.
#[allow(clippy::too_many_arguments)]
async fn delivery_broadcast_loop(
    orch: Arc<delivery::DeliveryOrchestrator>,
    pool: sqlx::SqlitePool,
    ws_tx: tokio::sync::broadcast::Sender<WsEvent>,
    cached: std::sync::Arc<std::sync::RwLock<state::CachedDeliveryStatus>>,
    config: std::sync::Arc<rs_core::config::Config>,
    audit_tx: tokio::sync::mpsc::Sender<rs_core::audit::AuditRow>,
) {
    // Track previous endpoint alive state for ActivityFeed transitions
    let mut prev_alive: std::collections::HashMap<String, bool> = std::collections::HashMap::new();

    let mut last_event_id: Option<i64> = None;
    let mut last_event_name: Option<String> = None;
    let mut last_state_str = String::from("idle");

    // Track session start time for display in dashboard
    let mut session_start_time: Option<String> = None;

    // Tick counter — persist metrics every 3rd tick (every 6s at a 2s poll).
    let mut tick_counter: u64 = 0;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        // Find the active streaming event with delivering_activated
        let event = match db::get_streaming_event(&pool).await {
            Ok(Some(e)) if e.delivering_activated => e,
            _ => {
                // Broadcast "none" status when not delivering
                let none_status = state::CachedDeliveryStatus {
                    status: "none".to_string(),
                    ..Default::default()
                };
                if let Ok(mut c) = cached.write() {
                    *c = none_status;
                }
                let _ = ws_tx.send(WsEvent::DeliveryStatus {
                    instance_name: String::new(),
                    status: "none".to_string(),
                    server_ip: None,
                    endpoint_count: 0,
                    endpoints: Vec::new(),
                });
                let _ = ws_tx.send(WsEvent::PipelineState {
                    state: "idle".to_string(),
                    event_id: None,
                    event_name: None,
                    target_delay_secs: 0,
                    session_start: None,
                    local_buffer_chunks: 0,
                    s3_queue_chunks: 0,
                    cache_duration_secs: 0.0,
                });
                prev_alive.clear();
                session_start_time = None;
                continue;
            }
        };

        // Initialize session start time on first delivering tick
        if session_start_time.is_none() {
            session_start_time = Some(chrono::Utc::now().to_rfc3339());
        }

        // Mirror VPS audit rows into the host audit_log. Best-effort —
        // the VPS may be unreachable for reasons outside our control, and
        // the next tick retries. Rows are sent through the real audit_tx
        // so they reach the writer task and broadcast live via
        // `WsEvent::AuditAppended` — same pipeline as host-originated rows.
        if let Ok(Some(inst)) = db::get_delivery_instance_by_event(&pool, event.id).await {
            let _ = delivery::mirror_vps_audit(&pool, inst.id, &audit_tx).await;
        }

        match orch.poll_delivery_metrics(event.id).await {
            Ok((name, status, server_ip, _endpoint_count, endpoints)) => {
                // Supplement empty endpoints with configured placeholders
                let (final_endpoints, final_ep_count) = if endpoints.is_empty() {
                    let configured = db::get_event_endpoints(&pool, event.id)
                        .await
                        .unwrap_or_default();
                    let placeholders: Vec<DeliveryEndpointMetrics> = configured
                        .iter()
                        .map(|ep| DeliveryEndpointMetrics {
                            alias: ep.alias.clone(),
                            alive: false,
                            current_chunk_id: 0,
                            bytes_processed_total: 0,
                            chunks_processed: 0,
                            chunk_delay_secs: 0.0,
                            stall_reason: None,
                            ffmpeg_restart_count: 0,

                            reconnect_count: 0,
                            last_error: None,
                            is_fast: ep.is_fast,
                            delivery_mode: None,
                            rescue_eta_secs: None,
                            youtube_health: None,
                        })
                        .collect();
                    let count = placeholders.len() as u32;
                    (placeholders, count)
                } else {
                    let count = endpoints.len() as u32;
                    (endpoints.clone(), count)
                };

                // Cache for instant HTTP retrieval
                if let Ok(mut c) = cached.write() {
                    *c = state::CachedDeliveryStatus {
                        instance_name: name.clone(),
                        status: status.clone(),
                        server_ip: server_ip.clone(),
                        endpoint_count: final_ep_count,
                        endpoints: final_endpoints.clone(),
                    };
                }
                let _ = ws_tx.send(WsEvent::DeliveryStatus {
                    instance_name: name,
                    status,
                    server_ip,
                    endpoint_count: final_ep_count,
                    endpoints: final_endpoints.clone(),
                });

                // Compute and broadcast PipelineState
                let any_piping = final_endpoints
                    .iter()
                    .any(|m| m.alive && m.chunks_processed > 0);
                let state_str = if any_piping { "streaming" } else { "buffering" };

                let target_delay = event
                    .cache_delay_secs
                    .map(|s| s as u64)
                    .unwrap_or(config.delivery.delivery_delay_secs);

                last_event_id = Some(event.id);
                last_event_name = Some(event.name.clone());
                last_state_str = state_str.to_string();

                // Compute chunk pipeline breakdown
                let pending_chunks = db::get_pending_chunk_count_for_event(&pool, event.id)
                    .await
                    .unwrap_or(0);
                let sent_chunks = db::get_sent_chunk_count_for_event(&pool, event.id)
                    .await
                    .unwrap_or(0);
                let max_delivery_chunk = max_non_fast_delivery_chunk(&final_endpoints);
                let s3_queue = (sent_chunks - max_delivery_chunk).max(0);

                // Cap at 1.5x the per-event target. The raw value sums all
                // sent chunks above max_delivery_chunk; when no endpoint
                // is actively delivering yet (Stop+Start cycle, VPS spin-up,
                // pre-first-push prefill), max_delivery_chunk == 0 and the
                // raw value reflects the entire S3 backlog (e.g. 1726s for
                // an event that's been streaming 28 min). Operator sees
                // that as "1726s / 120s cache" which is nonsense (#187).
                // 1.5x leaves room to surface a genuine oversized cache
                // without flooding the dashboard with the historical sum.
                let cache_cap = (target_delay as f64) * 1.5;
                let cache_duration =
                    db::get_cache_duration_secs(&pool, event.id, max_delivery_chunk)
                        .await
                        .unwrap_or(0.0)
                        .min(cache_cap);

                let _ = ws_tx.send(WsEvent::PipelineState {
                    state: state_str.to_string(),
                    event_id: Some(event.id),
                    event_name: Some(event.name.clone()),
                    target_delay_secs: target_delay,
                    session_start: session_start_time.clone(),
                    local_buffer_chunks: pending_chunks,
                    s3_queue_chunks: s3_queue,
                    cache_duration_secs: cache_duration,
                });

                // Emit ActivityFeed for endpoint state transitions
                for ep in &final_endpoints {
                    let was_alive = prev_alive.get(&ep.alias).copied().unwrap_or(false);
                    if ep.alive && !was_alive {
                        let _ = ws_tx.send(WsEvent::ActivityFeed {
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            severity: "info".to_string(),
                            message: format!("Endpoint '{}' is now streaming", ep.alias),
                            source: "delivery".to_string(),
                        });
                    } else if !ep.alive && was_alive {
                        let reason = ep.stall_reason.as_deref().unwrap_or("unknown");
                        let _ = ws_tx.send(WsEvent::ActivityFeed {
                            timestamp: chrono::Utc::now().to_rfc3339(),
                            severity: "warning".to_string(),
                            message: format!("Endpoint '{}' stalled: {}", ep.alias, reason),
                            source: "delivery".to_string(),
                        });
                    }
                    prev_alive.insert(ep.alias.clone(), ep.alive);
                }

                // Persist per-endpoint metrics every 3rd tick (~6s) and broadcast
                // MetricsSample so dashboards can draw live time-series charts.
                tick_counter = tick_counter.wrapping_add(1);
                if tick_counter % 3 == 0 {
                    let ts_ms = chrono::Utc::now().timestamp_millis();
                    if let Ok(Some(inst)) =
                        db::get_delivery_instance_by_event(&pool, event.id).await
                    {
                        for m in &final_endpoints {
                            let _ = rs_core::db::metrics::insert(
                                &pool,
                                ts_ms,
                                inst.id,
                                event.id,
                                &m.alias,
                                m.alive,
                                m.current_chunk_id,
                                m.chunks_processed,
                                m.chunk_delay_secs,
                                m.bytes_processed_total,
                                m.ffmpeg_restart_count as i64,
                                m.delivery_mode.as_deref(),
                            )
                            .await;
                            let _ = ws_tx.send(WsEvent::MetricsSample {
                                ts_ms,
                                event_id: event.id,
                                instance_id: inst.id,
                                alias: m.alias.clone(),
                                chunk_delay_secs: m.chunk_delay_secs,
                                current_chunk_id: m.current_chunk_id,
                                chunks_processed: m.chunks_processed,
                                alive: m.alive,
                            });
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!("Delivery metrics poll failed: {e}");
                if let (Some(eid), Some(ename)) = (last_event_id, last_event_name.as_ref()) {
                    let pending = db::get_pending_chunk_count_for_event(&pool, eid)
                        .await
                        .unwrap_or(0);
                    let sent = db::get_sent_chunk_count_for_event(&pool, eid)
                        .await
                        .unwrap_or(0);
                    // Honor per-event cache_delay_secs override; only fall back
                    // to the global default when no override is set.
                    let fallback_target_delay = db::get_streaming_event_by_id(&pool, eid)
                        .await
                        .ok()
                        .flatten()
                        .and_then(|ev| ev.cache_delay_secs.map(|s| s as u64))
                        .unwrap_or(config.delivery.delivery_delay_secs);
                    // Cap at 1.5x target (#187) — see notes on the success
                    // path for the rationale.
                    let cache_cap = (fallback_target_delay as f64) * 1.5;
                    let cache_duration = db::get_cache_duration_secs(&pool, eid, 0)
                        .await
                        .unwrap_or(0.0)
                        .min(cache_cap);
                    let _ = ws_tx.send(WsEvent::PipelineState {
                        state: last_state_str.clone(),
                        event_id: Some(eid),
                        event_name: Some(ename.clone()),
                        target_delay_secs: fallback_target_delay,
                        session_start: session_start_time.clone(),
                        local_buffer_chunks: pending,
                        s3_queue_chunks: sent,
                        cache_duration_secs: cache_duration,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod max_non_fast_tests {
    use super::*;

    fn ep(alias: &str, current: i64, fast: bool) -> DeliveryEndpointMetrics {
        DeliveryEndpointMetrics {
            alias: alias.to_string(),
            alive: true,
            current_chunk_id: current,
            bytes_processed_total: 0,
            chunks_processed: 0,
            chunk_delay_secs: 0.0,
            stall_reason: None,
            ffmpeg_restart_count: 0,
            reconnect_count: 0,
            last_error: None,
            is_fast: fast,
            delivery_mode: None,
            rescue_eta_secs: None,
            youtube_health: None,
        }
    }

    #[test]
    fn empty_endpoints_returns_zero() {
        assert_eq!(max_non_fast_delivery_chunk(&[]), 0);
    }

    #[test]
    fn fast_endpoints_alone_returns_zero_during_prefill() {
        // Reproduces the bug: a fast endpoint races to chunk 240 while non-fast
        // endpoints sit at 0 during the prefill window. Without the filter,
        // max=240 → cache_duration_secs reads ~3s for 120s, then jumps to 120
        // when chunks_processed flips. With the filter, max=0 → cache_duration
        // ramps 0→target smoothly as the buffer fills.
        let eps = vec![ep("Kiko", 240, true)];
        assert_eq!(max_non_fast_delivery_chunk(&eps), 0);
    }

    #[test]
    fn fast_endpoint_excluded_when_mixed_with_non_fast() {
        let eps = vec![
            ep("Kiko", 240, true),
            ep("FB-Zbynek", 50, false),
            ep("YT", 60, false),
        ];
        // Excluding fast: max(50, 60) = 60.
        assert_eq!(max_non_fast_delivery_chunk(&eps), 60);
    }

    #[test]
    fn all_non_fast_returns_max() {
        let eps = vec![
            ep("FB-Zbynek", 50, false),
            ep("YT", 60, false),
            ep("FB-NewLevel", 55, false),
        ];
        assert_eq!(max_non_fast_delivery_chunk(&eps), 60);
    }
}

#[cfg(test)]
mod tls_tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    #[tokio::test]
    async fn https_redirect_skips_when_forwarded_proto_https() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, "streamsnv.newlevel.media".to_string())
            }));
        let req = Request::builder()
            .uri("/test")
            .header("host", "streamsnv.newlevel.media")
            .header("x-forwarded-proto", "https")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn https_redirect_redirects_http_domain_request() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, "streamsnv.newlevel.media".to_string())
            }));
        let req = Request::builder()
            .uri("/test")
            .header("host", "streamsnv.newlevel.media")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PERMANENT_REDIRECT);
        assert_eq!(
            resp.headers().get("location").unwrap(),
            "https://streamsnv.newlevel.media/test"
        );
    }

    #[tokio::test]
    async fn https_redirect_passes_through_ip_requests() {
        let app = axum::Router::new()
            .route("/test", axum::routing::get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |req, next| {
                https_redirect(req, next, "streamsnv.newlevel.media".to_string())
            }));
        let req = Request::builder()
            .uri("/test")
            .header("host", "10.77.9.204:8910")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
