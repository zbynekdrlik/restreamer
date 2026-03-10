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
use rs_core::config::Config;
use rs_core::db;
use rs_core::log_buffer::LogBuffer;
use rs_core::models::{InpointState, WsEvent};
use rs_endpoint::s3::S3Client;
use rs_endpoint::uploader::ChunkUploader;
use rs_inpoint::chunker::ChunkSink;
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
        }
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
        self,
        shutdown_signal: impl Future<Output = ()>,
    ) -> anyhow::Result<()> {
        let shutdown = ShutdownCoordinator::new();

        // Database
        let pool = db::create_pool(&self.db_path)
            .await
            .context("failed to create database pool")?;
        db::run_migrations(&pool)
            .await
            .context("failed to run database migrations")?;
        info!("Database initialized at {}", self.db_path.display());

        // Set up client profile
        db::upsert_client_profile(&pool, &self.config.client_uuid)
            .await
            .context("failed to set client profile")?;

        // WebSocket broadcast channel
        let (ws_tx, _) = broadcast::channel::<WsEvent>(256);

        // Restart channels for inpoint and endpoint
        let (inpoint_restart_tx, inpoint_restart_rx) = mpsc::channel::<()>(1);
        let (endpoint_restart_tx, endpoint_restart_rx) = mpsc::channel::<()>(1);

        // Shared RTMP connection state
        let inpoint_state = self.inpoint_state.clone();

        // API server
        let api_addr: SocketAddr =
            format!("{}:{}", self.config.api.bind, self.config.api.port).parse()?;
        let mut api_state = AppState::new(pool.clone(), self.config.clone(), ws_tx.clone())
            .with_config_path(self.config_path)
            .with_log_buffer(self.log_buffer)
            .with_inpoint_state(inpoint_state.clone())
            .with_restart_channels(inpoint_restart_tx, endpoint_restart_tx);

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

        // RTMP Inpoint server
        let chunk_sink = Arc::new(ChunkSink::new(
            self.chunk_dir.clone(),
            Duration::from_millis(self.config.inpoint.chunk_duration_ms),
        ));

        // Forward chunk events to the database
        let mut chunk_rx = chunk_sink.subscribe();
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
                        warn!("Chunk event receiver lagged, missed {n} events");
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
        let inpoint_sink = Arc::clone(&chunk_sink);
        let inpoint_state_clone = inpoint_state.clone();
        let inpoint_task = tokio::spawn(async move {
            run_inpoint_loop(
                inpoint_bind,
                inpoint_port,
                inpoint_sink,
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
        let endpoint_task = tokio::spawn(async move {
            run_endpoint_loop(
                endpoint_pool,
                s3_config,
                endpoint_ws_tx,
                endpoint_restart_rx,
                endpoint_shutdown_rx,
            )
            .await;
        });

        // Wait for shutdown signal
        info!("All services started.");
        shutdown_signal.await;
        info!("Shutdown signal received");

        // Trigger shutdown
        shutdown.trigger();

        // Flush remaining chunks before uploader stops
        chunk_sink.flush().await;

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
        drop(chunk_sink);
        match chunk_task.await {
            Ok(()) => info!("Chunk forwarder stopped cleanly"),
            Err(e) => tracing::error!("Chunk forwarder panicked: {e}"),
        }
        // Abort the API server (it has no shutdown signal)
        api_handle.abort();
        info!("API server stopped");

        info!("Service stopped");
        Ok(())
    }
}

/// Run the RTMP inpoint server with restart support.
async fn run_inpoint_loop(
    bind: String,
    port: u16,
    chunk_sink: Arc<ChunkSink>,
    inpoint_state: InpointState,
    mut restart_rx: mpsc::Receiver<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    loop {
        let server = RtmpServer::new(&bind, port);
        let rtmp_shutdown = server.shutdown_handle();
        let sink = Arc::clone(&chunk_sink);
        let state = inpoint_state.clone();
        let mut handle = tokio::spawn(async move { server.run(sink, state).await });

        info!("Inpoint RTMP server started on {bind}:{port}");

        let restart = tokio::select! {
            result = &mut handle => {
                match result {
                    Ok(Ok(())) => info!("RTMP server stopped"),
                    Ok(Err(e)) => tracing::error!("RTMP server error: {e}"),
                    Err(e) => tracing::error!("RTMP task panicked: {e}"),
                }
                false
            }
            msg = restart_rx.recv() => {
                if msg.is_some() {
                    info!("Inpoint restart requested");
                    let _ = rtmp_shutdown.send(());
                    let _ = handle.await;
                    true
                } else {
                    false // channel closed
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Inpoint shutting down");
                let _ = rtmp_shutdown.send(());
                let _ = handle.await;
                false
            }
        };

        chunk_sink.flush().await;

        if !restart {
            break;
        }

        info!("Restarting inpoint RTMP server...");
    }
}

/// Run the endpoint uploader with restart support.
async fn run_endpoint_loop(
    pool: SqlitePool,
    s3_config: rs_core::config::S3Config,
    ws_tx: broadcast::Sender<WsEvent>,
    mut restart_rx: mpsc::Receiver<()>,
    mut shutdown_rx: broadcast::Receiver<()>,
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

        let uploader = ChunkUploader::new(pool.clone(), s3, ws_tx.clone());
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
