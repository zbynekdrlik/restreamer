use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::broadcast;

use rs_core::config::Config;
use rs_core::models::WsEvent;

/// Shared application state for all Axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<Config>,
    pub ws_tx: broadcast::Sender<WsEvent>,
}

impl AppState {
    pub fn new(pool: SqlitePool, config: Config, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        Self {
            pool,
            config: Arc::new(config),
            ws_tx,
        }
    }
}
