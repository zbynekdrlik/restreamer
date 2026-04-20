use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::SqlitePool;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use rs_api::state::AppState;
use rs_core::audit::AuditRow;
use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::{InpointState, WsEvent};
use rs_endpoint::metrics::UploadMetrics;
use rs_endpoint::s3::S3Client;
use rs_endpoint::uploader::ChunkUploader;
use rs_inpoint::flv_chunker::FlvChunkSink;
use rs_inpoint::rtmp_server::RtmpServer;

use crate::shutdown::ShutdownCoordinator;

/// Main service orchestrator that starts all components.
///
/// This is the core runtime that can be embedded in both:
/// - The standalone `restreamer-service` binary (Windows Service / console mode)
/// - The unified Tauri application with embedded service
pub struct ServiceCore {
    config: Config,
    config_path: PathBuf,
    log_buffer: LogBuffer,
    db_path: PathBuf,
    chunk_dir: PathBuf,
    inpoint_state: InpointState,
    /// Externally provided database pool (used in GUI mode to avoid duplicate pools)
    provided_pool: Option<SqlitePool>,
}

impl ServiceCore {
    /// Create a new ServiceCore with the given configuration.
    pub fn new(config: Config, config_path: PathBuf, log_buffer: LogBuffer) -> Self {
        Self::with_inpoint_state(config, config_path, log_buffer, InpointState::new())
    }

    pub fn with_inpoint_state(
        config: Config,
        config_path: PathBuf,
        log_buffer: LogBuffer,
        inpoint_state: InpointState,
    ) -> Self {
        let data_dir = if cfg!(windows) {
            PathBuf::from(r"C:\ProgramData\Restreamer")
        } else {
            PathBuf::from("/var/lib/restreamer")
        };

        Self {
            db_path: data_dir.join("restreamer.db"),
            chunk_dir: data_dir.join("chunks"),
            config,
            config_path,
            log_buffer,
            inpoint_state,
            provided_pool: None,
        }
    }

    /// Provide an externally created database pool.
    ///
    /// When set, `run_with_signal()` will use this pool instead of creating a new one.
    /// This is essential for GUI mode where the Tauri app already created a pool for AppState.
    ///
    /// # Precondition
    ///
    /// The caller **must** run `db::run_migrations(&pool)` before passing the pool,
    /// because `run_with_signal()` skips migrations for externally provided pools.
    pub fn with_pool(mut self, pool: SqlitePool) -> Self {
        self.provided_pool = Some(pool);
        self
    }

    /// Start all service components and wait for shutdown via Ctrl+C.
    pub async fn run(self) -> anyhow::Result<()> {
        self.run_with_signal(async {
            info!("Press Ctrl+C to stop.");
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
    }

    /// Start all service components and wait for the given shutdown signal.
    pub async fn run_with_signal(
        mut self,
        shutdown_signal: impl Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let shutdown = ShutdownCoordinator::new();

        // Database: use provided pool or create a new one. For audit
        // purposes we capture the schema version before and after
        // running migrations so a `MigrationsApplied` row (emitted below,
        // once the audit channel exists) records the actual delta.
        let (pool, migration_from, migration_to) = match self.provided_pool.take() {
            Some(pool) => {
                info!("Using externally provided database pool");
                let v = db::current_schema_version(&pool).await.unwrap_or(0);
                (pool, v, v)
            }
            None => {
                let pool = db::create_pool(&self.db_path)
                    .await
                    .context("failed to create database pool")?;
                let from_v = db::current_schema_version(&pool).await.unwrap_or(0);
                db::run_migrations(&pool)
                    .await
                    .context("failed to run database migrations")?;
                let to_v = db::current_schema_version(&pool).await.unwrap_or(from_v);
                db::seed_templates_from_events(&pool)
                    .await
                    .context("failed to seed templates from events")?;
                info!("Database initialized at {}", self.db_path.display());
                (pool, from_v, to_v)
            }
        };

        // Set up client profile
        if let Err(e) = db::upsert_client_profile(&pool, &self.config.client_uuid).await {
            tracing::error!("Failed to set client profile: {e:?}");
            tracing::error!("client_uuid was: {:?}", self.config.client_uuid);
            return Err(e).context("failed to set client profile");
        }

        // WebSocket broadcast channel
        let (ws_tx, _) = broadcast::channel::<WsEvent>(256);

        // Restart channels for inpoint and endpoint
        let (inpoint_restart_tx, inpoint_restart_rx) = mpsc::channel::<()>(1);
        let (endpoint_restart_tx, endpoint_restart_rx) = mpsc::channel::<()>(1);

        // Shared RTMP connection state
        let inpoint_state = self.inpoint_state.clone();

        // Shared S3 upload blocked flag (test hook for simulating outages)
        let s3_upload_blocked = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Shared upload metrics (exposed via /api/v1/uploads/stats)
        let upload_metrics = Arc::new(UploadMetrics::default());

        // API server
        let api_addr: SocketAddr =
            format!("{}:{}", self.config.api.bind, self.config.api.port).parse()?;

        // Create the audit channel BEFORE `AppState::new` so the real
        // sender is wired into the `DeliveryOrchestrator` at construction
        // (it was previously a throwaway sender and all VPS-lifecycle
        // audit rows were silently dropped — see the 2026-04-19 post-mortem).
        let (audit_tx, audit_rx) = mpsc::channel::<AuditRow>(1024);

        let mut api_state = AppState::new(
            pool.clone(),
            self.config.clone(),
            ws_tx.clone(),
            audit_tx.clone(),
        )
        .with_config_path(self.config_path)
        .with_log_buffer(self.log_buffer)
        .with_inpoint_state(inpoint_state.clone())
        .with_restart_channels(inpoint_restart_tx, endpoint_restart_tx)
        .with_s3_upload_blocked(Arc::clone(&s3_upload_blocked))
        .with_upload_metrics(Arc::clone(&upload_metrics));

        // Spawn the audit writer that drains the receiver and batches
        // INSERTs + WS broadcasts, and schedule nightly rotation of the
        // audit_log and metrics tables.
        {
            let pool = pool.clone();
            let ws_tx = ws_tx.clone();
            tokio::spawn(async move {
                rs_core::audit::audit_writer_task(pool, ws_tx, audit_rx).await;
            });
        }
        {
            let pool = pool.clone();
            tokio::spawn(async move {
                loop {
                    let now = chrono::Utc::now();
                    // Next 02:00 UTC.
                    let mut next = now.date_naive().and_hms_opt(2, 0, 0).unwrap().and_utc();
                    if next <= now {
                        next += chrono::Duration::hours(24);
                    }
                    let sleep_secs = (next - now).num_seconds().max(60) as u64;
                    tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
                    let _ = rs_core::db::audit::rotate(&pool, 90).await;
                    let _ = rs_core::db::metrics::rotate(&pool, 7).await;
                }
            });
        }

        // Share the AppState's audit_tx with downstream components so they
        // feed into the same audit pipeline. The sender now flows to the
        // real writer task spawned above.
        let uploader_audit_tx = api_state.audit_tx.clone();

        // Re-wire the inpoint_state so the MediaReceiver can write the
        // shared `rtmp_stable_since` cell read by `POST /delivery/start`,
        // and emit RtmpConnected/Disconnected audit rows.
        let wired_inpoint = api_state
            .inpoint_state
            .clone()
            .with_audit_tx(api_state.audit_tx.clone())
            .with_stable_since(Arc::clone(&api_state.rtmp_stable_since));
        api_state = api_state.with_inpoint_state(wired_inpoint.clone());
        let inpoint_state = wired_inpoint;

        // Serve the WASM frontend from a "www" directory next to the binary,
        // so LAN browsers can access the dashboard at http://<host>:8910/
        if let Ok(exe) = std::env::current_exe() {
            if let Some(exe_dir) = exe.parent() {
                let www = exe_dir.join("www");
                if www.is_dir() {
                    info!("Serving frontend from {}", www.display());
                    api_state = api_state.with_www_dir(www);
                }
            }
        }
        let (actual_addr, api_handle) = rs_api::serve(api_state, api_addr).await?;
        info!("API server running on {actual_addr}");

        // Chunk directory
        tokio::fs::create_dir_all(&self.chunk_dir).await?;

        // RTMP Inpoint server (FLV-only)
        let flv_chunk_sink = Arc::new(FlvChunkSink::new(
            self.chunk_dir.clone(),
            Duration::from_millis(self.config.inpoint.chunk_duration_ms),
        ));

        // Forward chunk events to the database.
        let mut chunk_rx = flv_chunk_sink.subscribe();
        let chunk_pool = pool.clone();
        let chunk_ws_tx = ws_tx.clone();
        let chunk_task = tokio::spawn(async move {
            loop {
                match chunk_rx.recv().await {
                    Ok(chunk_info) => {
                        // Get current streaming event for the chunk
                        match db::get_streaming_event(&chunk_pool).await {
                            Ok(Some(event)) => {
                                let path_str = chunk_info.path.to_string_lossy().to_string();
                                match db::insert_chunk(
                                    &chunk_pool,
                                    event.id,
                                    &path_str,
                                    chunk_info.size as i64,
                                    &chunk_info.md5,
                                    chunk_info.duration_ms as i64,
                                )
                                .await
                                {
                                    Ok(id) => {
                                        if let Err(e) = db::update_received_bytes(
                                            &chunk_pool,
                                            event.id,
                                            chunk_info.size as i64,
                                        )
                                        .await
                                        {
                                            tracing::error!("Failed to update received bytes: {e}");
                                        }
                                        if let Err(e) = chunk_ws_tx.send(WsEvent::ChunkReceived {
                                            id,
                                            data_size: chunk_info.size as i64,
                                            md5: chunk_info.md5,
                                        }) {
                                            tracing::debug!(
                                                "No WS subscribers for ChunkReceived: {e}"
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        tracing::error!("Failed to insert chunk record: {e}");
                                    }
                                }
                            }
                            Ok(None) => {
                                warn!("Chunk received but no active streaming event");
                            }
                            Err(e) => {
                                tracing::error!("Failed to query streaming event: {e}");
                            }
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::error!("Chunk broadcast lagged, LOST {n} chunks from DB tracking");
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        info!("Chunk event channel closed");
                        break;
                    }
                }
            }
        });

        // Inpoint restart loop
        let inpoint_shutdown_rx = shutdown.subscribe();
        let inpoint_bind = self.config.inpoint.rtmp_bind.clone();
        let inpoint_port = self.config.inpoint.rtmp_port;
        let inpoint_flv_sink = Arc::clone(&flv_chunk_sink);
        let inpoint_state_clone = inpoint_state.clone();
        let inpoint_task = tokio::spawn(async move {
            run_inpoint_loop(
                inpoint_bind,
                inpoint_port,
                inpoint_flv_sink,
                inpoint_state_clone,
                inpoint_restart_rx,
                inpoint_shutdown_rx,
            )
            .await;
        });

        // Endpoint restart loop (S3 upload only — no manager notification)
        let endpoint_shutdown_rx = shutdown.subscribe();
        let s3_config = self.config.s3.clone();
        let endpoint_pool = pool.clone();
        let endpoint_ws_tx = ws_tx.clone();
        let endpoint_client_uuid = self.config.client_uuid.clone();
        let endpoint_audit_tx = uploader_audit_tx.clone();
        let endpoint_task = tokio::spawn(async move {
            run_endpoint_loop(
                endpoint_pool,
                s3_config,
                endpoint_ws_tx,
                endpoint_restart_rx,
                endpoint_shutdown_rx,
                s3_upload_blocked,
                upload_metrics,
                endpoint_client_uuid,
                endpoint_audit_tx,
            )
            .await;
        });

        // Periodic status broadcast for live dashboard updates
        let status_pool = pool.clone();
        let status_ws_tx = ws_tx.clone();
        let status_inpoint = inpoint_state.clone();
        let status_chunk_ms = self.config.inpoint.chunk_duration_ms;
        let status_shutdown_rx = shutdown.subscribe();
        let status_task = tokio::spawn(async move {
            run_status_broadcast(
                status_pool,
                status_ws_tx,
                status_inpoint,
                status_chunk_ms,
                status_shutdown_rx,
            )
            .await;
        });

        // Audit: system-source startup rows. Emitted once all components
        // have spawned so MigrationsApplied / RestreamerStarted land AFTER
        // any error-during-migration would have bailed out of this fn.
        if migration_to > migration_from {
            rs_core::audit::record(
                &uploader_audit_tx,
                rs_core::audit::AuditRow {
                    severity: rs_core::audit::Severity::Info,
                    source: rs_core::audit::Source::System,
                    event_id: None,
                    instance_id: None,
                    endpoint: None,
                    action: rs_core::audit::Action::MigrationsApplied,
                    detail: serde_json::json!({
                        "from_version": migration_from,
                        "to_version": migration_to,
                    }),
                    ts_override: None,
                },
            );
        }
        rs_core::audit::record(
            &uploader_audit_tx,
            rs_core::audit::AuditRow {
                severity: rs_core::audit::Severity::Info,
                source: rs_core::audit::Source::System,
                event_id: None,
                instance_id: None,
                endpoint: None,
                action: rs_core::audit::Action::RestreamerStarted,
                detail: serde_json::json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "db_path": self.db_path.display().to_string(),
                }),
                ts_override: None,
            },
        );

        // Wait for shutdown signal
        info!("All services started.");
        shutdown_signal.await;
        info!("Shutdown signal received");

        // Trigger shutdown
        shutdown.trigger();

        // Flush remaining chunks before uploader stops
        flv_chunk_sink.flush().await;

        // Wait for all tasks
        match inpoint_task.await {
            Ok(()) => info!("Inpoint stopped cleanly"),
            Err(e) => tracing::error!("Inpoint task panicked: {e}"),
        }
        match endpoint_task.await {
            Ok(()) => info!("Endpoint stopped cleanly"),
            Err(e) => tracing::error!("Endpoint task panicked: {e}"),
        }
        // Drop the chunk channel so the chunk task can exit
        drop(flv_chunk_sink);
        match chunk_task.await {
            Ok(()) => info!("Chunk forwarder stopped cleanly"),
            Err(e) => tracing::error!("Chunk forwarder panicked: {e}"),
        }
        // Stop periodic status broadcast
        status_task.abort();
        info!("Status broadcast stopped");

        // Abort the API server (it has no shutdown signal)
        api_handle.abort();
        info!("API server stopped");

        info!("Service stopped");
        Ok(())
    }
}

/// Run the RTMP inpoint server with restart support.
///
/// Auto-restarts on crash with exponential backoff (2s, 4s, 8s, 16s, max 30s).
/// Crash counter resets when a publisher connects. Gives up after 10 consecutive
/// crashes without any successful connection.
async fn run_inpoint_loop(
    bind: String,
    port: u16,
    flv_chunk_sink: Arc<FlvChunkSink>,
    inpoint_state: InpointState,
    mut restart_rx: mpsc::Receiver<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut consecutive_crashes: u32 = 0;
    let mut last_connected = false;
    const MAX_CONSECUTIVE_CRASHES: u32 = 10;

    loop {
        let server = RtmpServer::new(&bind, port);
        let rtmp_shutdown = server.shutdown_handle();
        let flv_sink = Arc::clone(&flv_chunk_sink);
        let state = inpoint_state.clone();
        let mut handle = tokio::spawn(async move { server.run(flv_sink, state).await });

        info!("Inpoint RTMP server started on {bind}:{port}");

        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(60));
        heartbeat.tick().await; // consume the immediate first tick

        let restart = loop {
            tokio::select! {
                result = &mut handle => {
                    match result {
                        Ok(Ok(())) => {
                            info!("RTMP server stopped cleanly");
                            break false; // Clean stop, don't restart
                        }
                        Ok(Err(e)) => {
                            tracing::error!("RTMP server error: {e}");
                            consecutive_crashes += 1;
                        }
                        Err(e) => {
                            tracing::error!("RTMP task panicked: {e}");
                            consecutive_crashes += 1;
                        }
                    }
                    if consecutive_crashes >= MAX_CONSECUTIVE_CRASHES {
                        tracing::error!(
                            crashes = consecutive_crashes,
                            "RTMP server exceeded max consecutive crashes, giving up"
                        );
                        break false;
                    }
                    let backoff = (1u64 << consecutive_crashes.min(4)).min(30);
                    tracing::warn!(
                        crashes = consecutive_crashes,
                        backoff_secs = backoff,
                        "RTMP server crashed, auto-restarting in {backoff}s"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(backoff)).await;
                    break true; // Restart
                }
                msg = restart_rx.recv() => {
                    if msg.is_some() {
                        info!("Inpoint restart requested");
                        let _ = rtmp_shutdown.send(());
                        let _ = handle.await;
                        break true;
                    } else {
                        break false; // channel closed
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Inpoint shutting down");
                    let _ = rtmp_shutdown.send(());
                    let _ = handle.await;
                    break false;
                }
                _ = heartbeat.tick() => {
                    let connected = inpoint_state.is_connected();
                    if connected && !last_connected {
                        consecutive_crashes = 0;
                        info!("RTMP publisher connected, crash counter reset");
                    }
                    last_connected = connected;
                    info!(
                        rtmp_connected = connected,
                        "Inpoint heartbeat: RTMP server alive"
                    );
                }
            }
        };

        flv_chunk_sink.flush().await;

        if !restart {
            break;
        }

        info!("Restarting inpoint RTMP server...");
    }
}

/// Run the endpoint uploader with restart support.
#[allow(clippy::too_many_arguments)]
async fn run_endpoint_loop(
    pool: SqlitePool,
    s3_config: rs_core::config::S3Config,
    ws_tx: broadcast::Sender<WsEvent>,
    mut restart_rx: mpsc::Receiver<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
    s3_upload_blocked: Arc<std::sync::atomic::AtomicBool>,
    upload_metrics: Arc<UploadMetrics>,
    client_uuid: String,
    audit_tx: mpsc::Sender<AuditRow>,
) {
    loop {
        let s3 = match S3Client::new(&s3_config) {
            Ok(s3) => s3,
            Err(e) => {
                tracing::error!("Failed to create S3 client: {e}");
                break;
            }
        };

        let (component_shutdown_tx, _) = broadcast::channel::<()>(1);
        let component_rx = component_shutdown_tx.subscribe();

        let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx.clone(), client_uuid.clone())
            .with_upload_blocked(Arc::clone(&s3_upload_blocked))
            .with_metrics(Arc::clone(&upload_metrics))
            .with_audit_tx(audit_tx.clone());
        let mut handle = tokio::spawn(async move { uploader.run(component_rx).await });

        info!("Endpoint uploader started");

        let restart = tokio::select! {
            _ = &mut handle => {
                info!("Uploader stopped");
                false
            }
            msg = restart_rx.recv() => {
                if msg.is_some() {
                    info!("Endpoint restart requested");
                    let _ = component_shutdown_tx.send(());
                    let _ = handle.await;
                    true
                } else {
                    false // channel closed
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Endpoint shutting down");
                let _ = component_shutdown_tx.send(());
                let _ = handle.await;
                false
            }
        };

        if !restart {
            break;
        }

        info!("Restarting endpoint uploader...");
    }
}

/// Broadcast InpointStatus and EndpointStatus every 2 seconds so the
/// WebSocket-driven dashboard has live-updating numbers without HTTP polling.
async fn run_status_broadcast(
    pool: SqlitePool,
    ws_tx: broadcast::Sender<WsEvent>,
    inpoint_state: InpointState,
    chunk_duration_ms: u64,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = shutdown_rx.recv() => {
                info!("Status broadcast shutting down");
                break;
            }
        }

        let rtmp_connected = inpoint_state.is_connected();

        // Get current streaming event for received_bytes
        let (received_bytes, receiving) = match db::get_streaming_event(&pool).await {
            Ok(Some(evt)) => (evt.received_bytes as u64, evt.receiving_activated),
            _ => (0, false),
        };

        // Get chunk stats
        let stats = db::get_chunk_stats(&pool, chunk_duration_ms)
            .await
            .unwrap_or_default();

        let inpoint_state_str = if rtmp_connected {
            "receiving"
        } else if receiving {
            "waiting"
        } else {
            "idle"
        };

        let _ = ws_tx.send(WsEvent::InpointStatus {
            state: inpoint_state_str.to_string(),
            rtmp_connected,
            received_bytes,
            chunk_count: stats.total_chunks as u64,
        });

        let ep_state = if stats.pending_chunks > 0 {
            "uploading"
        } else if stats.total_chunks > 0 {
            "idle"
        } else {
            "waiting"
        };

        let buffer_duration = {
            let total = stats.buffer_duration_secs as u64;
            let h = total / 3600;
            let m = (total % 3600) / 60;
            let s = total % 60;
            format!("{h:02}:{m:02}:{s:02}")
        };

        let _ = ws_tx.send(WsEvent::EndpointStatus {
            state: ep_state.to_string(),
            pending_chunks: stats.pending_chunks as u64,
            active_uploads: stats.in_process_chunks as u32,
            buffer_duration,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::db;

    /// Test: ServiceCore should accept an externally provided pool via with_pool()
    /// This prevents duplicate pool creation in GUI mode.
    #[tokio::test]
    async fn service_core_with_pool_stores_provided_pool() {
        // Arrange: Create a pool externally (simulating GUI mode)
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();

        // Create test config
        let config = Config::for_testing();
        let config_path = PathBuf::from("/tmp/test-config.json");
        let log_buffer = LogBuffer::new(10);

        // Act: Create ServiceCore with externally provided pool
        let core = ServiceCore::new(config, config_path, log_buffer).with_pool(pool.clone());

        // Assert: ServiceCore should have the provided pool stored
        assert!(
            core.provided_pool.is_some(),
            "ServiceCore should store the provided pool"
        );
    }

    /// Test: When pool is provided, the provided pool should contain our test data
    /// This verifies we're using the SAME pool, not creating a new one.
    #[tokio::test]
    async fn service_core_with_pool_uses_same_pool_instance() {
        // Arrange: Create a pool and insert test data
        let pool = db::create_memory_pool().await.unwrap();
        db::run_migrations(&pool).await.unwrap();

        // Insert a test client profile to verify we're using THIS pool
        db::upsert_client_profile(&pool, "test-client-uuid")
            .await
            .unwrap();

        let config = Config::for_testing();
        let config_path = PathBuf::from("/tmp/test-config.json");
        let log_buffer = LogBuffer::new(10);

        // Act: Create ServiceCore with the pool containing test data
        let core = ServiceCore::new(config, config_path, log_buffer).with_pool(pool.clone());

        // Assert: The pool should be the same one we provided (has our test data)
        let provided_pool = core.provided_pool.as_ref().unwrap();
        let profile = db::get_client_profile(provided_pool).await.unwrap();
        assert!(profile.is_some(), "Should find test data in provided pool");
        assert_eq!(profile.unwrap().user_uuid, "test-client-uuid");
    }
}
