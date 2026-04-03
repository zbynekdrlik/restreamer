/// Delivery binary — runs on Hetzner VPS to pull S3 chunks and pipe to ffmpeg.
///
/// Provides a minimal Axum API on :8000 for health, init, status, and stop.
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use rs_core::log_buffer::LogBuffer;
use tracing_subscriber::prelude::*;

mod api;
mod endpoint_task;
mod s3_fetch;

pub use endpoint_task::EndpointHandle;

/// Application state shared across API handlers.
pub struct AppState {
    pub endpoints: RwLock<HashMap<String, EndpointHandle>>,
    pub version: &'static str,
    pub ready: RwLock<bool>,
    /// Bearer token for authenticating API requests. Set via DELIVERY_AUTH_TOKEN
    /// env var or via the /api/init endpoint.
    pub auth_token: RwLock<Option<String>>,
    /// S3 config stored after /api/init for use by /api/endpoints/add.
    pub s3_config: RwLock<Option<api::S3Config>>,
    /// Event identifier stored after /api/init for use by /api/endpoints/add.
    pub event_identifier: RwLock<Option<String>>,
    /// Delivery delay in chunks, stored after /api/init.
    pub delivery_delay_chunks: RwLock<i64>,
    /// In-memory log buffer for /api/logs endpoint.
    pub log_buffer: LogBuffer,
}

impl Default for AppState {
    fn default() -> Self {
        let auth_token = std::env::var("DELIVERY_AUTH_TOKEN").ok();
        Self {
            endpoints: RwLock::new(HashMap::new()),
            version: env!("CARGO_PKG_VERSION"),
            ready: RwLock::new(true),
            auth_token: RwLock::new(auth_token),
            s3_config: RwLock::new(None),
            event_identifier: RwLock::new(None),
            delivery_delay_chunks: RwLock::new(0),
            log_buffer: LogBuffer::new(5000),
        }
    }
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState::default());

    let capture_layer = rs_core::log_capture::LogCaptureLayer::new(state.log_buffer.clone());
    let fmt_layer = tracing_subscriber::fmt::layer().with_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    );
    tracing_subscriber::registry()
        .with(capture_layer)
        .with(fmt_layer)
        .init();

    let app = api::router(state);

    let addr = "0.0.0.0:8000";
    tracing::info!("Delivery service starting on {addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind to 0.0.0.0:8000");
    axum::serve(listener, app)
        .await
        .expect("delivery server error");
}
