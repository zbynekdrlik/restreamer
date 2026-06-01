//! Application state management for the embedded service.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use sqlx::SqlitePool;
use tokio::sync::{broadcast, oneshot, Mutex, RwLock};

use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::{ChunkStats, InpointState, StreamingEvent, WsEvent};

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
    /// Shared RTMP connection state
    inpoint_state: InpointState,
    /// Shared disk-pressure level atomic (0=ok, 1=warn, 2=critical). Same
    /// Arc the embedded `rs_api::AppState` reads in its `get_status`
    /// handler, written by the disk monitor inside ServiceCore. #234.
    disk_pressure_level: Arc<std::sync::atomic::AtomicU8>,
    /// Shared "RTMP publisher stable since" timestamp. Same Arc the
    /// embedded `rs_api::AppState` reads in its `get_status` handler.
    /// #234.
    rtmp_stable_since: Arc<Mutex<Option<Instant>>>,
}

impl AppState {
    /// Create a new AppState with the given resources.
    pub fn new(
        pool: SqlitePool,
        config: Config,
        log_buffer: LogBuffer,
        ws_tx: broadcast::Sender<WsEvent>,
        shutdown_tx: oneshot::Sender<()>,
        inpoint_state: InpointState,
        disk_pressure_level: Arc<std::sync::atomic::AtomicU8>,
        rtmp_stable_since: Arc<Mutex<Option<Instant>>>,
    ) -> Self {
        Self {
            pool,
            config,
            log_buffer,
            ws_tx,
            shutdown_tx: Arc::new(RwLock::new(Some(shutdown_tx))),
            inpoint_state,
            disk_pressure_level,
            rtmp_stable_since,
        }
    }

    /// Read the current disk-pressure level as a UI-ready string
    /// (`"ok"` / `"warn"` / `"critical"`). Mirrors the HTTP `/api/v1/status`
    /// path so the tray banner renders identically to the LAN dashboard.
    /// Mapping intentionally mirrors `rs_endpoint::disk_pressure::DiskPressure::as_str`.
    pub fn disk_pressure(&self) -> String {
        match self
            .disk_pressure_level
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            2 => "critical",
            1 => "warn",
            _ => "ok",
        }
        .to_string()
    }

    /// Seconds since the RTMP publisher has been continuously stable.
    /// Zero when no publisher is currently connected.
    pub async fn rtmp_stable_secs(&self) -> u64 {
        self.rtmp_stable_since
            .lock()
            .await
            .map(|t| t.elapsed().as_secs())
            .unwrap_or(0)
    }

    /// Check if RTMP publisher is connected.
    pub fn is_inpoint_connected(&self) -> bool {
        self.inpoint_state.is_connected()
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

    /// Delete all chunk records from the database and remove orphaned `.bin` files.
    pub async fn clear_all_chunks(&self) -> Result<u64, String> {
        let count = db::delete_all_chunks(&self.pool)
            .await
            .map_err(|e| e.to_string())?;

        let chunk_dir = if cfg!(windows) {
            std::path::PathBuf::from(r"C:\ProgramData\Restreamer\chunks")
        } else {
            std::path::PathBuf::from("/var/lib/restreamer/chunks")
        };

        cleanup_chunk_files(&chunk_dir).await;

        Ok(count)
    }

    /// Trigger graceful shutdown of the embedded service.
    pub async fn shutdown(&self) {
        let mut guard = self.shutdown_tx.write().await;
        if let Some(tx) = guard.take() {
            let _ = tx.send(());
        }
    }
}

/// Remove all `.bin` files from the given directory.
///
/// Non-`.bin` files are left intact. Errors on individual file deletions
/// are logged but do not stop processing remaining files.
pub async fn cleanup_chunk_files(dir: &Path) {
    if let Ok(mut entries) = tokio::fs::read_dir(dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            if entry.path().extension().is_some_and(|ext| ext == "bin") {
                if let Err(e) = tokio::fs::remove_file(entry.path()).await {
                    tracing::warn!("Failed to remove chunk file {}: {e}", entry.path().display());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cleanup_chunk_files_removes_bin_preserves_others() {
        let tmp = tempfile::tempdir().unwrap();
        let bin1 = tmp.path().join("chunk1.bin");
        let bin2 = tmp.path().join("chunk2.bin");
        let keep = tmp.path().join("config.json");
        tokio::fs::write(&bin1, b"data").await.unwrap();
        tokio::fs::write(&bin2, b"data").await.unwrap();
        tokio::fs::write(&keep, b"data").await.unwrap();

        cleanup_chunk_files(tmp.path()).await;

        assert!(!bin1.exists(), "chunk1.bin should be deleted");
        assert!(!bin2.exists(), "chunk2.bin should be deleted");
        assert!(keep.exists(), "config.json should be preserved");
    }

    #[tokio::test]
    async fn cleanup_chunk_files_handles_missing_dir() {
        // Should not panic on a non-existent directory
        cleanup_chunk_files(Path::new("/tmp/nonexistent-restreamer-test-dir")).await;
    }
}
