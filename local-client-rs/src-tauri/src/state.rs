//! Application state management for the embedded service.

use std::path::PathBuf;
use std::sync::Arc;

use sqlx::SqlitePool;
use tokio::sync::{broadcast, oneshot, RwLock};

use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::{ChunkStats, StreamingEvent, WsEvent};

/// Shared application state that holds the embedded service resources.
///
/// This replaces the HTTP-based communication with direct state access.
pub struct AppState {
    /// SQLite connection pool for direct database queries
    pool: SqlitePool,
    /// Configuration
    config: Config,
    /// Log buffer for viewing logs
    log_buffer: LogBuffer,
    /// WebSocket event broadcast channel
    ws_tx: broadcast::Sender<WsEvent>,
    /// Shutdown signal sender (used when app exits)
    shutdown_tx: Arc<RwLock<Option<oneshot::Sender<()>>>>,
}

impl AppState {
    /// Create a new AppState with the given resources.
    pub fn new(
        pool: SqlitePool,
        config: Config,
        log_buffer: LogBuffer,
        ws_tx: broadcast::Sender<WsEvent>,
        shutdown_tx: oneshot::Sender<()>,
    ) -> Self {
        Self {
            pool,
            config,
            log_buffer,
            ws_tx,
            shutdown_tx: Arc::new(RwLock::new(Some(shutdown_tx))),
        }
    }

    /// Get chunk statistics directly from the database.
    pub async fn get_chunk_stats(&self) -> Result<ChunkStats, String> {
        let chunk_duration_ms = self.config.inpoint.chunk_duration_ms;
        db::get_chunk_stats(&self.pool, chunk_duration_ms)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get the current streaming event.
    pub async fn get_streaming_event(&self) -> Result<Option<StreamingEvent>, String> {
        db::get_streaming_event(&self.pool)
            .await
            .map_err(|e| e.to_string())
    }

    /// Get recent log entries for a component.
    pub fn get_logs(&self, component: &str, limit: usize) -> Vec<rs_core::log_buffer::LogEntry> {
        self.log_buffer.recent(component, limit)
    }

    /// Subscribe to WebSocket events.
    pub fn subscribe_events(&self) -> broadcast::Receiver<WsEvent> {
        self.ws_tx.subscribe()
    }

    /// Get the configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the database path.
    pub fn db_path(&self) -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\ProgramData\Restreamer\restreamer.db")
        } else {
            PathBuf::from("/var/lib/restreamer/restreamer.db")
        }
    }

    /// Trigger graceful shutdown of the embedded service.
    pub async fn shutdown(&self) {
        let mut guard = self.shutdown_tx.write().await;
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
    }
}
