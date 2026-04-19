use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use sqlx::SqlitePool;
use tokio::sync::{Mutex, broadcast, mpsc};

use rs_core::audit::AuditRow;
use rs_core::config::{Config, ObsConfig};
use rs_core::log_buffer::LogBuffer;
use rs_core::models::{InpointState, WsEvent};
use rs_endpoint::metrics::UploadMetrics;

use crate::delivery::DeliveryOrchestrator;
use crate::obs::ObsClient;

/// Cached delivery metrics from the last broadcast loop poll.
/// Updated every 2 seconds by the delivery broadcast loop.
#[derive(Clone, Default, serde::Serialize)]
pub struct CachedDeliveryStatus {
    pub instance_name: String,
    pub status: String,
    pub server_ip: Option<String>,
    pub endpoint_count: u32,
    pub endpoints: Vec<rs_core::models::DeliveryEndpointMetrics>,
}

/// Shared application state for all Axum handlers.
#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<Config>,
    /// Mutable config reference that can be swapped by patch_config.
    /// Handlers that need the latest config after a patch should read from this.
    pub config_live: Arc<std::sync::RwLock<Arc<Config>>>,
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
    /// Cached delivery status from the last broadcast loop poll.
    /// Allows instant initial load without hitting the VPS.
    pub cached_delivery: Arc<std::sync::RwLock<CachedDeliveryStatus>>,
    /// OBS WebSocket client. Wrapped in RwLock to allow dynamic restart on config change.
    pub obs_client: Arc<tokio::sync::RwLock<Option<Arc<ObsClient>>>>,
    /// Test hook: when true, S3 uploads are paused (simulates network outage).
    pub s3_upload_blocked: Arc<std::sync::atomic::AtomicBool>,
    /// Serializes S3 mutation operations (delete-event and clear-s3) so
    /// two concurrent deletes cannot overlap their S3 LIST+DELETE scans
    /// and double the load on the S3 endpoint. Also provides a stable
    /// happens-before boundary between concurrent delete requests.
    pub s3_mutation_lock: Arc<tokio::sync::Mutex<()>>,
    /// Upload metrics shared with ChunkUploader for /uploads/stats API.
    pub upload_metrics: Arc<UploadMetrics>,
    /// When the RTMP publisher last became "connected". Used by the
    /// `POST /delivery/start` handler to gate creation of a VPS until the
    /// ingest has been stable for `RTMP_STABLE_REQUIRED_SECS` seconds.
    /// `None` means no publisher is currently connected. Wire-up to the
    /// inpoint MediaReceiver lands in Task 18; for now the field exists so
    /// the handler and its tests can exercise the gate directly.
    pub rtmp_stable_since: Arc<Mutex<Option<Instant>>>,
    /// Fire-and-forget sender for audit rows. Handlers push `AuditRow` via
    /// `rs_core::audit::record(&state.audit_tx, row)` and the audit writer
    /// task batches INSERTs + broadcasts `WsEvent::AuditAppended`.
    /// The default constructor creates a throwaway channel whose receiver
    /// is dropped immediately — real wiring (spawning `audit_writer_task`
    /// against the receiver) lands in Task 27.
    pub audit_tx: mpsc::Sender<AuditRow>,
}

impl AppState {
    pub fn new(pool: SqlitePool, config: Config, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        let delivery = DeliveryOrchestrator::new(pool.clone(), config.clone());
        let obs_client = if config.obs.enabled {
            Some(Arc::new(ObsClient::spawn(
                config.obs.clone(),
                ws_tx.clone(),
            )))
        } else {
            None
        };
        let config = Arc::new(config);
        // Throwaway audit channel; real wire-up is Task 27.
        let (audit_tx, _audit_rx) = mpsc::channel::<AuditRow>(1024);
        Self {
            pool,
            config_live: Arc::new(std::sync::RwLock::new(config.clone())),
            config,
            ws_tx,
            config_path: None,
            log_buffer: LogBuffer::new(100),
            inpoint_restart_tx: None,
            endpoint_restart_tx: None,
            www_dir: None,
            inpoint_state: InpointState::new(),
            delivery_orchestrator: delivery.map(Arc::new),
            cached_delivery: Arc::new(std::sync::RwLock::new(CachedDeliveryStatus::default())),
            obs_client: Arc::new(tokio::sync::RwLock::new(obs_client)),
            s3_upload_blocked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            s3_mutation_lock: Arc::new(tokio::sync::Mutex::new(())),
            upload_metrics: Arc::new(UploadMetrics::default()),
            rtmp_stable_since: Arc::new(Mutex::new(None)),
            audit_tx,
        }
    }

    /// Replace the audit channel with a shared sender (used when the
    /// runtime spawns the `audit_writer_task` and wants handlers to feed
    /// into the real writer).
    pub fn with_audit_tx(mut self, tx: mpsc::Sender<AuditRow>) -> Self {
        self.audit_tx = tx;
        self
    }

    /// Replace the upload metrics with a shared instance (set before ChunkUploader is spawned).
    pub fn with_upload_metrics(mut self, m: Arc<UploadMetrics>) -> Self {
        self.upload_metrics = m;
        self
    }

    /// Set the S3 upload blocked flag (shared with ChunkUploader).
    pub fn with_s3_upload_blocked(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.s3_upload_blocked = flag;
        self
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

    /// Restart or stop the OBS WebSocket client based on new config.
    /// Dropping the old client closes the command channel, which causes
    /// the connection loop to exit cleanly.
    pub async fn restart_obs_client(&self, obs_config: &ObsConfig) {
        let mut guard = self.obs_client.write().await;
        // Drop old client first (closes cmd channel → loop exits)
        *guard = None;
        if obs_config.enabled {
            *guard = Some(Arc::new(ObsClient::spawn(
                obs_config.clone(),
                self.ws_tx.clone(),
            )));
        }
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
