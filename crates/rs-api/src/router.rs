use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::request::Parts;
use axum::http::{HeaderValue, Method, header};
use axum::routing::{get, post};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::access;
use crate::delivery_handlers;
use crate::handlers;
use crate::rescue_video_handlers;
use crate::s3_handlers;
use crate::state::AppState;
use crate::stream_handlers;
use crate::template_handlers;
use crate::uploads_endpoints;
use crate::websocket;
use crate::youtube;

/// Build the Axum router with all API routes.
///
/// Creates its own [`access::AccessGate`] from the config. Production uses
/// [`build_router_with_gate`] instead so `lib.rs::serve` can hold the same gate
/// and warm/refresh its JWKS cache in the background.
pub fn build_router(state: AppState) -> Router {
    let gate = access::AccessGate::from_config(&state.config.api.access);
    build_router_with_gate(state, gate)
}

/// Inner fallback for the nested `/api/v1` router: an unmatched API path is a
/// 404, never the SPA HTML (#248).
async fn api_not_found() -> axum::http::StatusCode {
    axum::http::StatusCode::NOT_FOUND
}

/// Build the router around an existing access gate.
pub fn build_router_with_gate(state: AppState, gate: std::sync::Arc<access::AccessGate>) -> Router {
    let api = Router::new()
        // Core status/health
        .route("/health", get(handlers::health))
        .route("/status", get(handlers::get_status))
        .route(
            "/streaming-event",
            get(handlers::get_streaming_event).delete(handlers::delete_streaming_event),
        )
        .route(
            "/chunks",
            get(handlers::get_chunks).delete(handlers::delete_chunks),
        )
        .route("/chunks/stats", get(handlers::get_chunk_stats))
        // Actions
        .route(
            "/actions/restart-inpoint",
            post(handlers::action_restart_inpoint),
        )
        .route(
            "/actions/restart-endpoint",
            post(handlers::action_restart_endpoint),
        )
        .route(
            "/actions/toggle-receiving",
            post(handlers::action_toggle_receiving),
        )
        .route(
            "/actions/toggle-delivering",
            post(handlers::action_toggle_delivering),
        )
        // Config
        .route(
            "/config",
            get(handlers::get_config).patch(handlers::patch_config),
        )
        // Logs
        .route("/logs/inpoint", get(handlers::get_logs_inpoint))
        .route("/logs/endpoint", get(handlers::get_logs_endpoint))
        // Audit log
        .route("/audit", get(crate::audit_handlers::list))
        .route("/audit/{id}", get(crate::audit_handlers::get_one))
        // WebSocket
        .route("/ws", get(websocket::ws_handler))
        // Events CRUD
        .route(
            "/events",
            get(handlers::list_events).post(handlers::create_event),
        )
        .route(
            "/events/{id}",
            get(handlers::get_event_by_id)
                .delete(handlers::delete_event_by_id)
                .patch(stream_handlers::update_event),
        )
        .route(
            "/events/{id}/clear-s3",
            post(s3_handlers::clear_event_s3_chunks),
        )
        .route("/s3/usage", get(s3_handlers::get_s3_usage))
        .route(
            "/rescue-video/upload",
            post(rescue_video_handlers::upload_rescue_video)
                // Hard body-size limit matches the handler's MAX_INPUT_BYTES
                // (200 MiB). Without this Axum buffers the entire multipart
                // payload into memory before the handler sees a single byte,
                // so a client could OOM the Tauri process by POSTing 10 GiB.
                // The handler transcodes the input down to a much smaller
                // FLV (capped at 50 MiB via MAX_FLV_BYTES); this layer just
                // bounds the pre-transcode raw upload.
                .layer(DefaultBodyLimit::max(209_715_200)),
        )
        .route("/events/{id}/activate", post(handlers::activate_event))
        .route(
            "/events/{id}/start-delivering",
            post(handlers::start_delivering),
        )
        .route("/events/{id}/deactivate", post(handlers::deactivate_event))
        .route(
            "/events/{id}/start-stream",
            post(stream_handlers::start_stream),
        )
        .route(
            "/events/{id}/stop-stream",
            post(stream_handlers::stop_stream),
        )
        .route("/events/{id}/endpoints", get(handlers::get_event_endpoints))
        .route(
            "/events/{event_id}/endpoints/{endpoint_id}",
            post(handlers::attach_endpoint_to_event).delete(handlers::detach_endpoint_from_event),
        )
        // Endpoint Configs CRUD
        .route(
            "/endpoints",
            get(handlers::list_endpoints).post(handlers::create_endpoint),
        )
        .route(
            "/endpoints/{id}",
            get(handlers::get_endpoint_by_id)
                .put(handlers::update_endpoint)
                .delete(handlers::delete_endpoint),
        )
        .route(
            "/endpoints/{id}/link-oauth",
            post(crate::endpoint_oauth::link_endpoint_oauth),
        )
        // Template CRUD
        .route(
            "/templates",
            get(template_handlers::list_templates).post(template_handlers::create_template),
        )
        .route(
            "/templates/{id}",
            get(template_handlers::get_template)
                .patch(template_handlers::update_template)
                .delete(template_handlers::delete_template),
        )
        .route(
            "/templates/{id}/endpoints",
            get(template_handlers::get_template_endpoints),
        )
        .route(
            "/templates/{template_id}/endpoints/{endpoint_id}",
            post(template_handlers::attach_endpoint_to_template)
                .delete(template_handlers::detach_endpoint_from_template),
        )
        // Delivery orchestration
        .route("/delivery/start", post(delivery_handlers::delivery_start))
        .route("/delivery/status", get(delivery_handlers::delivery_status))
        .route("/delivery/logs", get(delivery_handlers::delivery_logs))
        .route(
            "/delivery/status/cached",
            get(delivery_handlers::delivery_status_cached),
        )
        .route("/delivery/stop", post(delivery_handlers::delivery_stop))
        .route(
            "/delivery/instances",
            get(delivery_handlers::list_delivery_instances),
        )
        .route(
            "/delivery/last-destroy",
            get(delivery_handlers::delivery_last_destroy),
        )
        .route(
            "/delivery/endpoints/add",
            post(delivery_handlers::delivery_add_endpoint),
        )
        .route(
            "/delivery/endpoints/remove",
            post(delivery_handlers::delivery_remove_endpoint),
        )
        .route("/delivery/metrics", get(crate::metrics_handlers::list))
        // OBS WebSocket
        .route("/obs/status", get(handlers::obs_status))
        .route("/obs/start-stream", post(handlers::obs_start_stream))
        .route("/obs/stop-stream", post(handlers::obs_stop_stream))
        // YouTube
        .route("/youtube/status", get(youtube::youtube_status))
        .route("/youtube/oauths", get(youtube::list_oauths))
        .route(
            "/youtube/oauth/device-start",
            post(crate::oauth_device::device_start),
        )
        .route(
            "/youtube/oauth/device-status",
            get(crate::oauth_device::device_status),
        )
        .route("/youtube/oauth/seed", post(youtube::youtube_oauth_seed))
        // Facebook config seed (CI-only — see crates/rs-api/src/facebook.rs)
        .route(
            "/facebook/config/seed",
            post(crate::facebook::facebook_config_seed),
        )
        // Upload telemetry
        .route("/uploads/stats", get(uploads_endpoints::get_uploads_stats))
        .route(
            "/uploads/recent",
            get(uploads_endpoints::get_recent_uploads),
        )
        // Diagnostics
        .route(
            "/diagnostics/pacing",
            get(crate::diagnostics_pacing::get_pacing),
        )
        .route("/diag/dump", post(crate::diag::diag_dump_handler))
        // Test hooks for CI E2E testing
        .route("/_test/s3-block", post(handlers::test_s3_block))
        .route("/_test/s3-unblock", post(handlers::test_s3_unblock))
        .route(
            "/_test/oauth-device-grant",
            post(crate::oauth_device::test_grant_now),
        )
        // Unknown `/api/v1/*` paths must 404 as an API, NOT fall through to the
        // SPA fallback below (#248): the embedded/on-disk frontend serves
        // index.html for any unmatched route, which is correct for client-side
        // routes but wrong for a typo'd API endpoint. An explicit inner
        // fallback stops the nested API delegating unmatched paths to the outer
        // SPA fallback.
        .fallback(api_not_found);

    // Same-origin only (#339). The dashboard is served by this very process
    // (`compute_api_base()` returns `window.location.origin + '/api/v1'`), so
    // every legitimate browser call is same-origin — via the LAN IP, via
    // `stream.lan`, or via the public hostname behind the tunnel, each of which
    // matches its own Host. `allow_origin(Any)` used to hand every page on the
    // internet a read of the dashboard's state; it is gone.
    //
    // CORS is NOT the CSRF barrier — a bodyless cross-origin POST is a simple
    // request and is never preflighted. That is handled in
    // `access::csrf_violation`.
    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(
            |origin: &HeaderValue, parts: &Parts| {
                origin
                    .to_str()
                    .ok()
                    .map(|o| access::origin_is_same_site(o, &parts.headers))
                    .unwrap_or(false)
            },
        ))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::PATCH,
            Method::PUT,
        ])
        .allow_headers([header::CONTENT_TYPE, header::ACCEPT]);

    let mut router = Router::new()
        .nest("/api/v1", api)
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    // Serve the WASM dashboard. Production serves it EMBEDDED in the binary
    // (`rs_webui`, #248/#107) so a fresh NSIS install has a working dashboard
    // with no on-disk `www/` and the whole www-drift class dies by
    // construction; the embedded handler sets per-asset cache-control
    // (index.html no-cache, hashed assets immutable). An on-disk `www_dir`
    // override stays ONLY for tests / dev E2E (injected via `with_www_dir`),
    // where cache-control is irrelevant.
    if let Some(www_dir) = &state.www_dir {
        use tower_http::services::{ServeDir, ServeFile};
        let index = www_dir.join("index.html");
        let serve = ServeDir::new(www_dir).fallback(ServeFile::new(index));
        router = router.fallback_service(serve);
    } else {
        router = router.fallback(rs_webui::serve_embedded);
    }

    // Origin-aware access control (#70/#273/#337/#339) goes on LAST, AFTER
    // `fallback_service`, because `Router::layer` only wraps routes registered
    // BEFORE it — attaching it up where the CORS layer used to sit would have
    // left the dashboard SPA (HTML + WASM) completely ungated. `access_tests`
    // pins that with `the_spa_fallback_is_gated_too`.
    //
    // CORS stays OUTERMOST (applied after, so it runs first) so that an
    // `OPTIONS` preflight is answered by the CORS layer rather than being
    // refused by the gate.
    let ctx = access::AccessCtx {
        gate,
        state: state.clone(),
    };
    router
        .layer(axum::middleware::from_fn_with_state(
            ctx,
            access::access_middleware,
        ))
        .layer(cors)
}

// Tests split across three files, all to stay under the 1000-line-per-file cap:
// these (`#[path]`, so still a CHILD module of `router` reaching private items
// via `super::*`), the sibling `router_tests.rs`, and `access_tests.rs`.
#[cfg(test)]
#[path = "router_inline_tests.rs"]
mod tests;
