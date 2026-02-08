use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::broadcast;
use tracing::info;

use rs_api::state::AppState;
use rs_core::config::Config;
use rs_core::db;
use rs_core::models::WsEvent;
use rs_endpoint::manager_api::ManagerClient;
use rs_endpoint::s3::S3Client;
use rs_endpoint::uploader::ChunkUploader;
use rs_inpoint::chunker::ChunkSink;
use rs_inpoint::rtmp_server::RtmpServer;

use crate::poller::Poller;
use crate::shutdown::ShutdownCoordinator;

/// Main service orchestrator that starts all components.
pub struct ServiceRunner {
    config: Config,
    db_path: PathBuf,
    chunk_dir: PathBuf,
}

impl ServiceRunner {
    pub fn new(config: Config) -> Self {
        let data_dir = if cfg!(windows) {
            PathBuf::from(r"C:\ProgramData\Restreamer")
        } else {
            PathBuf::from("/var/lib/restreamer")
        };

        Self {
            db_path: data_dir.join("restreamer.db"),
            chunk_dir: data_dir.join("chunks"),
            config,
        }
    }

    /// Start all service components and wait for shutdown.
    pub async fn run(self) -> anyhow::Result<()> {
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

        // API server
        let api_addr: SocketAddr =
            format!("{}:{}", self.config.api.bind, self.config.api.port).parse()?;
        let api_state = AppState::new(pool.clone(), self.config.clone(), ws_tx.clone());
        let actual_addr = rs_api::serve(api_state, api_addr).await?;
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
        tokio::spawn(async move {
            while let Ok(chunk_info) = chunk_rx.recv().await {
                // Get current streaming event for the chunk
                if let Ok(Some(event)) = db::get_streaming_event(&chunk_pool).await {
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
                            let _ = db::update_received_bytes(
                                &chunk_pool,
                                event.id,
                                chunk_info.size as i64,
                            )
                            .await;
                            let _ = chunk_ws_tx.send(WsEvent::ChunkReceived {
                                id,
                                data_size: chunk_info.size as i64,
                                md5: chunk_info.md5,
                            });
                        }
                        Err(e) => {
                            tracing::error!("Failed to insert chunk record: {e}");
                        }
                    }
                }
            }
        });

        let rtmp_server = RtmpServer::new(self.config.inpoint.rtmp_port);
        let rtmp_shutdown = rtmp_server.shutdown_handle();
        let rtmp_sink = Arc::clone(&chunk_sink);
        let rtmp_handle = tokio::spawn(async move { rtmp_server.run(rtmp_sink).await });

        // Endpoint uploader
        let s3_client = S3Client::new(&self.config.s3).context("failed to create S3 client")?;
        let manager_client = ManagerClient::new(&self.config.manager_url);

        let uploader = ChunkUploader::new(pool.clone(), s3_client, manager_client, ws_tx.clone());
        let upload_shutdown = shutdown.subscribe();
        let upload_handle = tokio::spawn(async move { uploader.run(upload_shutdown).await });

        // Poller
        let poller_manager = ManagerClient::new(&self.config.manager_url);
        let poller = Poller::new(
            pool.clone(),
            poller_manager,
            self.config.client_uuid.clone(),
            ws_tx.clone(),
        );
        let poller_shutdown = shutdown.subscribe();
        let poller_handle = tokio::spawn(async move { poller.run(poller_shutdown).await });

        // Wait for Ctrl+C (or Windows service stop)
        info!("All services started. Press Ctrl+C to stop.");
        tokio::signal::ctrl_c().await?;
        info!("Shutdown signal received");

        // Trigger shutdown
        shutdown.trigger();
        let _ = rtmp_shutdown.send(());

        // Wait for all tasks
        let _ = rtmp_handle.await;
        let _ = upload_handle.await;
        let _ = poller_handle.await;

        // Flush remaining chunks
        chunk_sink.flush().await;

        info!("Service stopped");
        Ok(())
    }
}
