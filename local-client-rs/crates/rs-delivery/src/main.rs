/// Delivery binary — runs on Hetzner VPS to pull S3 chunks and pipe to ffmpeg.
///
/// Provides a minimal Axum API on :8000 for health, init, status, and stop.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

mod api;
mod endpoint_task;
mod s3_fetch;

pub use endpoint_task::EndpointHandle;

/// Application state shared across API handlers.
pub struct AppState {
    pub endpoints: RwLock<HashMap<String, EndpointHandle>>,
    pub version: &'static str,
    pub ready: RwLock<bool>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            endpoints: RwLock::new(HashMap::new()),
            version: env!("CARGO_PKG_VERSION"),
            ready: RwLock::new(true),
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let state = Arc::new(AppState::new());
    let app = api::router(state);

    let addr = "0.0.0.0:8000";
    tracing::info!("Delivery service starting on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
