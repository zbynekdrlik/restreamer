/// Delivery API routes: /api/health, /api/init, /api/status, /api/stop

use crate::{AppState, EndpointHandle};
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/init", post(init_endpoints))
        .route("/api/status", get(endpoint_status))
        .route("/api/stop", post(stop_endpoints))
        .with_state(state)
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

#[derive(Serialize)]
struct InitResponse {
    status: String,
    endpoints_started: usize,
}

async fn init_endpoints(
    State(state): State<Arc<AppState>>,
    Json(req): Json<InitRequest>,
) -> Result<Json<InitResponse>, StatusCode> {
    let mut endpoints = state.endpoints.write().await;
    let count = req.endpoints.len();

    for ep_cfg in &req.endpoints {
        if endpoints.contains_key(&ep_cfg.alias) {
            continue;
        }

        let handle = EndpointHandle::spawn(
            ep_cfg.clone(),
            req.s3_config.clone(),
            req.event_identifier.clone(),
            req.start_chunk_id,
        );

        endpoints.insert(ep_cfg.alias.clone(), handle);
    }

    tracing::info!(count, "Initialized endpoints");

    Ok(Json(InitResponse {
        status: "ok".to_string(),
        endpoints_started: count,
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
    use tower::util::ServiceExt;

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState::new())
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
    async fn status_endpoint_empty() {
        let app = router(test_state());
        let req = Request::builder()
            .uri("/api/status")
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
            .body(Body::from(r#"{"alias": null}"#))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
