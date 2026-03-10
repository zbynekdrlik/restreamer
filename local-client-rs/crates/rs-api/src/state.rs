use std::path::PathBuf;
use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::{broadcast, mpsc};

use rs_core::config::Config;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::{InpointState, WsEvent};

use crate::delivery::DeliveryOrchestrator;

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
    /// Directory containing the WASM frontend (index.html + assets).
    /// When set, Axum serves these files so LAN browsers can access the dashboard.
    pub www_dir: Option<PathBuf>,
    /// Shared RTMP connection state, set by MediaReceiver, read by API handlers.
    pub inpoint_state: InpointState,
    /// Delivery orchestrator for Hetzner VPS management.
    /// Only present when Hetzner API token is configured.
    pub delivery_orchestrator: Option<Arc<DeliveryOrchestrator>>,
}

impl AppState {
    pub fn new(pool: SqlitePool, config: Config, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        let delivery = DeliveryOrchestrator::new(pool.clone(), config.clone());
        Self {
            pool,
            config: Arc::new(config),
            ws_tx,
            config_path: None,
            log_buffer: LogBuffer::new(100),
            inpoint_restart_tx: None,
            endpoint_restart_tx: None,
            www_dir: None,
            inpoint_state: InpointState::new(),
            delivery_orchestrator: delivery.map(Arc::new),
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

    pub fn with_www_dir(mut self, dir: PathBuf) -> Self {
        self.www_dir = Some(dir);
        self
    }

    pub fn with_inpoint_state(mut self, state: InpointState) -> Self {
        self.inpoint_state = state;
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

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::db;

    #[tokio::test]
    async fn new_defaults() {
        let pool = db::create_memory_pool().await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, Config::for_testing(), ws_tx);

        assert!(state.config_path.is_none());
        assert!(state.inpoint_restart_tx.is_none());
        assert!(state.endpoint_restart_tx.is_none());
        // No Hetzner token in test config, so delivery is None
        assert!(state.delivery_orchestrator.is_none());
    }

    #[tokio::test]
    async fn with_config_path_sets_path() {
        let pool = db::create_memory_pool().await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let state = AppState::new(pool, Config::for_testing(), ws_tx)
            .with_config_path(PathBuf::from("/tmp/test.json"));

        assert_eq!(state.config_path, Some(PathBuf::from("/tmp/test.json")));
    }

    #[tokio::test]
    async fn with_log_buffer_replaces_default() {
        let pool = db::create_memory_pool().await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let buffer = LogBuffer::new(500);
        buffer.push(rs_core::log_buffer::LogEntry {
            level: "INFO".into(),
            target: "test".into(),
            message: "hello".into(),
        });

        let state = AppState::new(pool, Config::for_testing(), ws_tx).with_log_buffer(buffer);

        let entries = state.log_buffer.recent("test", 10);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "hello");
    }

    #[tokio::test]
    async fn with_restart_channels_sets_both() {
        let pool = db::create_memory_pool().await.unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);
        let (inpoint_tx, _) = mpsc::channel(1);
        let (endpoint_tx, _) = mpsc::channel(1);

        let state = AppState::new(pool, Config::for_testing(), ws_tx)
            .with_restart_channels(inpoint_tx, endpoint_tx);

        assert!(state.inpoint_restart_tx.is_some());
        assert!(state.endpoint_restart_tx.is_some());
    }
}
