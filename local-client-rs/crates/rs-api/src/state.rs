use std::path::PathBuf;
use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::{broadcast, mpsc};

use rs_core::config::Config;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::WsEvent;

/// Shared application state for all Axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<Config>,
    pub ws_tx: broadcast::Sender<WsEvent>,
    pub config_path: Option<PathBuf>,
    pub log_buffer: LogBuffer,
    pub inpoint_restart_tx: Option<mpsc::Sender<()>>,
    pub endpoint_restart_tx: Option<mpsc::Sender<()>>,
}

impl AppState {
    pub fn new(pool: SqlitePool, config: Config, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        Self {
            pool,
            config: Arc::new(config),
            ws_tx,
            config_path: None,
            log_buffer: LogBuffer::new(100),
            inpoint_restart_tx: None,
            endpoint_restart_tx: None,
        }
    }

    pub fn with_config_path(mut self, path: PathBuf) -> Self {
        self.config_path = Some(path);
        self
    }

    pub fn with_log_buffer(mut self, buffer: LogBuffer) -> Self {
        self.log_buffer = buffer;
        self
    }

    pub fn with_restart_channels(
        mut self,
        inpoint_tx: mpsc::Sender<()>,
        endpoint_tx: mpsc::Sender<()>,
    ) -> Self {
        self.inpoint_restart_tx = Some(inpoint_tx);
        self.endpoint_restart_tx = Some(endpoint_tx);
        self
    }
}
