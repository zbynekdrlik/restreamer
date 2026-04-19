/// Delivery API routes: /api/health, /api/init, /api/status, /api/stop, /api/logs
use crate::{AppState, EndpointHandle};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    routing::{get, post},
};
use rs_core::log_buffer::LogEntry;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    // Health endpoint is public (used for readiness probes)
    let public = Router::new().route("/api/health", get(health));

    // All other endpoints require bearer token authentication
    let protected = Router::new()
        .route("/api/init", post(init_endpoints))
        .route("/api/status", get(endpoint_status))
        .route("/api/stop", post(stop_endpoints))
        .route("/api/endpoints/add", post(add_endpoint))
        .route("/api/endpoints/remove", post(remove_endpoint))
        .route("/api/logs", get(get_logs))
        .layer(middleware::from_fn_with_state(state.clone(), require_auth));

    public.merge(protected).with_state(state)
}

/// Middleware that checks for a valid bearer token on protected endpoints.
async fn require_auth(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    let token = state.auth_token.read().await;
    // If no token is set yet, reject all requests
    let expected = match token.as_deref() {
        Some(t) if !t.is_empty() => t,
        _ => return Err(StatusCode::UNAUTHORIZED),
    };

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if let Some(bearer) = auth_header.strip_prefix("Bearer ") {
        if bearer == expected {
            return Ok(next.run(req).await);
        }
    }

    Err(StatusCode::UNAUTHORIZED)
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: state.version.to_string(),
    })
}

#[derive(Debug, Deserialize)]
pub struct InitRequest {
    pub endpoints: Vec<EndpointConfig>,
    pub s3_config: S3Config,
    pub event_identifier: String,
    pub start_chunk_id: i64,
    /// Optional auth token — if provided, sets the bearer token for future requests.
    #[serde(default)]
    pub auth_token: Option<String>,
    #[serde(default)]
    pub delivery_delay_ms: u64,
    /// Optional URL to a rescue video played when buffer is empty during warmup or outage.
    #[serde(default)]
    pub rescue_video_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EndpointConfig {
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
    #[serde(default)]
    pub is_fast: bool,
    /// Chunk storage format. Only "flv" is supported.
    #[serde(default = "default_chunk_format")]
    pub chunk_format: String,
    /// Per-endpoint start chunk ID. Overrides the top-level `start_chunk_id` if set.
    #[serde(default)]
    pub start_chunk_id: Option<i64>,
}

fn default_chunk_format() -> String {
    "flv".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl S3Config {
    /// Override fields with environment variables when available.
    /// This allows S3 credentials to be passed securely via cloud-init
    /// env file rather than over plaintext HTTP.
    pub fn with_env_overrides(mut self) -> Self {
        if let Ok(v) = std::env::var("DELIVERY_S3_BUCKET") {
            self.bucket = v;
        }
        if let Ok(v) = std::env::var("DELIVERY_S3_REGION") {
            self.region = v;
        }
        if let Ok(v) = std::env::var("DELIVERY_S3_ENDPOINT") {
            self.endpoint = v;
        }
        if let Ok(v) = std::env::var("DELIVERY_S3_ACCESS_KEY_ID") {
            self.access_key_id = v;
        }
        if let Ok(v) = std::env::var("DELIVERY_S3_SECRET_ACCESS_KEY") {
            self.secret_access_key = v;
        }
        self
    }
}

#[derive(Serialize)]
struct InitResponse {
    status: String,
    endpoints_started: usize,
}

async fn init_endpoints(
    State(state): State<Arc<AppState>>,
    Json(req): Json<InitRequest>,
) -> Result<Json<InitResponse>, StatusCode> {
    // If an auth token is provided, store it for future request authentication
    if let Some(token) = &req.auth_token {
        if !token.is_empty() {
            *state.auth_token.write().await = Some(token.clone());
        }
    }

    // Apply environment variable overrides to S3 config (credentials come via cloud-init)
    let s3_config = req.s3_config.clone().with_env_overrides();

    // Store S3 config and event identifier for later use by /api/endpoints/add
    *state.s3_config.write().await = Some(s3_config.clone());
    *state.event_identifier.write().await = Some(req.event_identifier.clone());
    *state.delivery_delay_ms.write().await = req.delivery_delay_ms;
    *state.rescue_video_url.write().await = req.rescue_video_url.clone();

    let mut endpoints = state.endpoints.write().await;
    let mut started = 0usize;

    for ep_cfg in &req.endpoints {
        if endpoints.contains_key(&ep_cfg.alias) {
            continue;
        }

        // Use per-endpoint start_chunk_id if set, otherwise fall back to top-level
        let start_id = ep_cfg.start_chunk_id.unwrap_or(req.start_chunk_id);

        let handle = EndpointHandle::spawn(
            ep_cfg.clone(),
            s3_config.clone(),
            req.event_identifier.clone(),
            start_id,
            req.delivery_delay_ms,
            req.rescue_video_url.clone(),
        );

        endpoints.insert(ep_cfg.alias.clone(), handle);
        started += 1;
    }

    tracing::info!(
        requested = req.endpoints.len(),
        started,
        "Initialized endpoints"
    );

    Ok(Json(InitResponse {
        status: "ok".to_string(),
        endpoints_started: started,
    }))
}

#[derive(Serialize)]
struct StatusResponse {
    status: String,
    endpoint_count: usize,
    endpoints: Vec<EndpointStatusEntry>,
    /// Audit rows since the requested cursor (up to AUDIT_RING_CAP). The
    /// host-side poller uses `next_audit_cursor` to mirror rows into the
    /// host `audit_log` without polling twice.
    #[serde(default)]
    recent_audit: Vec<crate::audit_ring::RingRow>,
    #[serde(default)]
    next_audit_cursor: i64,
}

#[derive(Debug, Deserialize, Default)]
pub struct StatusQuery {
    #[serde(default)]
    pub since: Option<i64>,
}

#[derive(Serialize)]
struct EndpointStatusEntry {
    alias: String,
    alive: bool,
    current_chunk_id: i64,
    bytes_processed_total: u64,
    chunks_processed: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    stall_reason: Option<String>,
    ffmpeg_restart_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ffmpeg_last_stderr: Option<String>,
    consecutive_chunk_misses: u32,
    consecutive_ffmpeg_failures: u32,
    /// Per-endpoint audit log of recent ffmpeg restarts (capped at
    /// RESTART_HISTORY_CAP). Each entry records timestamp, chunk_id at
    /// the moment of death, lifetime, reason, stderr tail, and the
    /// backoff applied before the next spawn.
    restart_history: Vec<crate::endpoint_task::FfmpegRestartRecord>,
    /// Current delivery mode: "normal", "warmup", "rescue", "recovering".
    delivery_mode: String,
    /// ETA in seconds until rescue mode ends (warmup or buffer refill).
    #[serde(skip_serializing_if = "Option::is_none")]
    rescue_eta_secs: Option<u64>,
}

async fn endpoint_status(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<StatusQuery>,
) -> Json<StatusResponse> {
    let endpoints = state.endpoints.read().await;
    let mut entries = Vec::new();

    for (alias, handle) in endpoints.iter() {
        let stats = handle.stats().await;
        entries.push(EndpointStatusEntry {
            alias: alias.clone(),
            alive: handle.is_alive(),
            current_chunk_id: stats.current_chunk_id,
            bytes_processed_total: stats.bytes_processed_total,
            chunks_processed: stats.chunks_processed,
            stall_reason: stats.stall_reason,
            ffmpeg_restart_count: stats.ffmpeg_restart_count,
            last_error: stats.last_error,
            ffmpeg_last_stderr: stats.ffmpeg_last_stderr,
            consecutive_chunk_misses: stats.consecutive_chunk_misses,
            consecutive_ffmpeg_failures: stats.consecutive_ffmpeg_failures,
            restart_history: stats.restart_history.into_iter().collect(),
            delivery_mode: stats.delivery_mode.clone(),
            rescue_eta_secs: stats.rescue_eta_secs,
        });
    }

    let (recent_audit, next_audit_cursor) = state.audit_ring.since(q.since.unwrap_or(0));

    Json(StatusResponse {
        status: "ok".to_string(),
        endpoint_count: entries.len(),
        endpoints: entries,
        recent_audit,
        next_audit_cursor,
    })
}

#[derive(Debug, Deserialize)]
struct StopRequest {
    alias: Option<String>,
}

#[derive(Serialize)]
struct StopResponse {
    status: String,
    message: String,
}

async fn stop_endpoints(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StopRequest>,
) -> Json<StopResponse> {
    let mut endpoints = state.endpoints.write().await;

    let message = match req.alias {
        Some(alias) => {
            if let Some(handle) = endpoints.remove(&alias) {
                handle.stop().await;
                format!("Stopped endpoint: {alias}")
            } else {
                format!("Endpoint not found: {alias}")
            }
        }
        None => {
            let count = endpoints.len();
            for (_, handle) in endpoints.drain() {
                handle.stop().await;
            }
            format!("Stopped all {count} endpoints")
        }
    };

    Json(StopResponse {
        status: "ok".to_string(),
        message,
    })
}

// --- Mid-stream endpoint add/remove ---

#[derive(Debug, Deserialize)]
struct AddEndpointRequest {
    endpoint: EndpointConfig,
}

#[derive(Serialize)]
struct AddEndpointResponse {
    status: String,
    alias: String,
    started: bool,
}

async fn add_endpoint(
    State(state): State<Arc<AppState>>,
    Json(req): Json<AddEndpointRequest>,
) -> Result<Json<AddEndpointResponse>, (StatusCode, String)> {
    // Read stored S3 config and event identifier (set during /api/init)
    let s3_config = state.s3_config.read().await.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "Delivery not initialized — call /api/init first".to_string(),
        )
    })?;
    let event_identifier = state.event_identifier.read().await.clone().ok_or_else(|| {
        (
            StatusCode::CONFLICT,
            "Delivery not initialized — call /api/init first".to_string(),
        )
    })?;
    let delivery_delay_ms = *state.delivery_delay_ms.read().await;
    let rescue_video_url = state.rescue_video_url.read().await.clone();

    let mut endpoints = state.endpoints.write().await;

    // Check for duplicate alias
    if endpoints.contains_key(&req.endpoint.alias) {
        return Ok(Json(AddEndpointResponse {
            status: "ok".to_string(),
            alias: req.endpoint.alias.clone(),
            started: false,
        }));
    }

    let start_id = req.endpoint.start_chunk_id.unwrap_or(0);

    let handle = EndpointHandle::spawn(
        req.endpoint.clone(),
        s3_config,
        event_identifier,
        start_id,
        delivery_delay_ms,
        rescue_video_url,
    );

    let alias = req.endpoint.alias.clone();
    endpoints.insert(alias.clone(), handle);

    tracing::info!(alias = %alias, start_chunk_id = start_id, "Added endpoint mid-stream");

    Ok(Json(AddEndpointResponse {
        status: "ok".to_string(),
        alias,
        started: true,
    }))
}

#[derive(Debug, Deserialize)]
struct RemoveEndpointRequest {
    alias: String,
}

#[derive(Serialize)]
struct RemoveEndpointResponse {
    status: String,
    alias: String,
    removed: bool,
}

async fn remove_endpoint(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RemoveEndpointRequest>,
) -> Result<Json<RemoveEndpointResponse>, StatusCode> {
    let mut endpoints = state.endpoints.write().await;

    if let Some(handle) = endpoints.remove(&req.alias) {
        handle.stop().await;
        tracing::info!(alias = %req.alias, "Removed endpoint mid-stream");
        Ok(Json(RemoveEndpointResponse {
            status: "ok".to_string(),
            alias: req.alias,
            removed: true,
        }))
    } else {
        Ok(Json(RemoveEndpointResponse {
            status: "ok".to_string(),
            alias: req.alias,
            removed: false,
        }))
    }
}

// --- Log retrieval ---

#[derive(Deserialize)]
struct LogQueryParams {
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize, Deserialize)]
pub struct LogsResponse {
    pub entries: Vec<LogEntry>,
}

const MAX_LOG_ENTRIES: usize = 1000;

async fn get_logs(
    State(state): State<Arc<AppState>>,
    axum::extract::Query(params): axum::extract::Query<LogQueryParams>,
) -> Json<LogsResponse> {
    let limit = params.limit.unwrap_or(100).min(MAX_LOG_ENTRIES);
    let entries = state.log_buffer.recent("", limit);
    Json(LogsResponse { entries })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    const TEST_TOKEN: &str = "test-secret-token";

    async fn test_state() -> Arc<AppState> {
        let mut state = AppState::new().await;
        state.auth_token = RwLock::new(Some(TEST_TOKEN.to_string()));
        Arc::new(state)
    }

    #[tokio::test]
    async fn health_endpoint() {
        let app = router(test_state().await);
        let req = Request::builder()
            .uri("/api/health")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert!(json["version"].is_string());
    }

    #[tokio::test]
    async fn status_endpoint_requires_auth() {
        let app = router(test_state().await);
        // Request without auth header should be rejected
        let req = Request::builder()
            .uri("/api/status")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn status_endpoint_empty() {
        let app = router(test_state().await);
        let req = Request::builder()
            .uri("/api/status")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["endpoint_count"], 0);
    }

    #[tokio::test]
    async fn status_response_includes_diagnostic_fields() {
        let app = router(test_state().await);
        let req = Request::builder()
            .uri("/api/status")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // When there are no endpoints, the array is empty but the shape is correct
        assert!(json["endpoints"].is_array());
    }

    #[tokio::test]
    async fn stop_endpoint_empty() {
        let app = router(test_state().await);
        let req = Request::builder()
            .method("POST")
            .uri("/api/stop")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::from(r#"{"alias": null}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn add_endpoint_returns_conflict_before_init() {
        let app = router(test_state().await);
        let body = serde_json::json!({
            "endpoint": {
                "alias": "test-yt",
                "service_type": "YT_HLS",
                "stream_key": "fake-key"
            }
        });
        let req = Request::builder()
            .method("POST")
            .uri("/api/endpoints/add")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn remove_endpoint_not_found() {
        let app = router(test_state().await);
        let body = serde_json::json!({ "alias": "nonexistent" });
        let req = Request::builder()
            .method("POST")
            .uri("/api/endpoints/remove")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {TEST_TOKEN}"))
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["removed"], false);
    }

    #[tokio::test]
    async fn get_logs_requires_auth() {
        let state = Arc::new(AppState::new().await);
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/logs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn get_logs_returns_entries() {
        let state = Arc::new(AppState::new().await);
        state.log_buffer.push(rs_core::log_buffer::LogEntry {
            level: "WARN".into(),
            target: "rs_delivery::endpoint_task".into(),
            message: "ffmpeg died".into(),
        });
        *state.auth_token.write().await = Some("test-token".to_string());
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/logs?limit=10")
                    .header("Authorization", "Bearer test-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let logs: LogsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(logs.entries.len(), 1);
        assert!(logs.entries[0].message.contains("ffmpeg died"));
    }
}
