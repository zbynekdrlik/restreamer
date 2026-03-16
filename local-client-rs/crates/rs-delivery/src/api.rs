/// Delivery API routes: /api/health, /api/init, /api/status, /api/stop
use crate::{AppState, EndpointHandle};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    middleware,
    routing::{get, post},
};
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
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EndpointConfig {
    pub alias: String,
    pub service_type: String,
    pub stream_key: String,
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

    let mut endpoints = state.endpoints.write().await;
    let mut started = 0usize;

    for ep_cfg in &req.endpoints {
        if endpoints.contains_key(&ep_cfg.alias) {
            continue;
        }

        let handle = EndpointHandle::spawn(
            ep_cfg.clone(),
            s3_config.clone(),
            req.event_identifier.clone(),
            req.start_chunk_id,
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
}

#[derive(Serialize)]
struct EndpointStatusEntry {
    alias: String,
    alive: bool,
    buff_size_bytes: u64,
    current_chunk_id: i64,
    bytes_processed_total: u64,
}

async fn endpoint_status(State(state): State<Arc<AppState>>) -> Json<StatusResponse> {
    let endpoints = state.endpoints.read().await;
    let mut entries = Vec::new();

    for (alias, handle) in endpoints.iter() {
        let stats = handle.stats().await;
        entries.push(EndpointStatusEntry {
            alias: alias.clone(),
            alive: handle.is_alive(),
            buff_size_bytes: stats.0,
            current_chunk_id: stats.1,
            bytes_processed_total: stats.0,
        });
    }

    Json(StatusResponse {
        status: "ok".to_string(),
        endpoint_count: entries.len(),
        endpoints: entries,
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tokio::sync::RwLock;
    use tower::util::ServiceExt;

    const TEST_TOKEN: &str = "test-secret-token";

    fn test_state() -> Arc<AppState> {
        let mut state = AppState::default();
        // Pre-set auth token for tests
        state.auth_token = RwLock::new(Some(TEST_TOKEN.to_string()));
        Arc::new(state)
    }

    #[tokio::test]
    async fn health_endpoint() {
        let app = router(test_state());
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
        let app = router(test_state());
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
        let app = router(test_state());
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
    async fn stop_endpoint_empty() {
        let app = router(test_state());
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
}
