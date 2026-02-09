use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{Semaphore, broadcast};
use tracing::{debug, error, info, warn};

use rs_core::db;
use rs_core::models::WsEvent;

use crate::manager_api::{ChunkUploadNotification, ManagerClient};
use crate::s3::S3Client;

/// Maximum retry attempts per batch cycle for transient S3/manager failures.
/// Failed chunks are automatically retried in subsequent batch cycles.
const MAX_RETRIES: u32 = 10;
/// Delay between retries (flat 3-second interval per spec).
const RETRY_DELAY: Duration = Duration::from_secs(3);

/// Watches for unsent chunks and uploads them to S3, then notifies the manager.
pub struct ChunkUploader {
    pool: SqlitePool,
    s3: Arc<S3Client>,
    manager: Arc<ManagerClient>,
    max_concurrent: usize,
    ws_tx: broadcast::Sender<WsEvent>,
}

impl ChunkUploader {
    pub fn new(
        pool: SqlitePool,
        s3: S3Client,
        manager: ManagerClient,
        ws_tx: broadcast::Sender<WsEvent>,
    ) -> Self {
        Self {
            pool,
            s3: Arc::new(s3),
            manager: Arc::new(manager),
            max_concurrent: 4,
            ws_tx,
        }
    }

    /// Run the upload loop until shutdown signal.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) {
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    info!("Chunk uploader shutting down");
                    break;
                }
                _ = self.upload_batch() => {}
            }

            // Brief pause between batches
            tokio::select! {
                _ = shutdown.recv() => break,
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
        }
    }

    async fn upload_batch(&self) {
        let chunks = match db::get_unsent_chunks(&self.pool, 20).await {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to query unsent chunks: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                return;
            }
        };

        if chunks.is_empty() {
            return;
        }

        info!("Found {} unsent chunks to upload", chunks.len());

        let semaphore = Arc::new(Semaphore::new(self.max_concurrent));
        let mut handles = Vec::new();
        for chunk in chunks {
            let permit = match semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(e) => {
                    error!("Semaphore closed: {e}");
                    break;
                }
            };
            let pool = self.pool.clone();
            let s3 = Arc::clone(&self.s3);
            let manager = Arc::clone(&self.manager);
            let ws_tx = self.ws_tx.clone();

            handles.push(tokio::spawn(async move {
                let _permit = permit;

                // Look up the event identifier from the chunk's own streaming_event_id
                let event_id = match db::get_streaming_event_by_id(
                    &pool,
                    chunk.streaming_event_id,
                )
                .await
                {
                    Ok(Some(event)) => match event.identifier {
                        Some(id) => id,
                        None => {
                            warn!(
                                "Chunk {} has event {} with no identifier, skipping",
                                chunk.id, chunk.streaming_event_id
                            );
                            return;
                        }
                    },
                    Ok(None) => {
                        warn!(
                            "Chunk {} references missing event {}, skipping",
                            chunk.id, chunk.streaming_event_id
                        );
                        return;
                    }
                    Err(e) => {
                        error!("Failed to query event for chunk {}: {e}", chunk.id);
                        return;
                    }
                };

                // Mark as in-process
                if let Err(e) = db::set_chunk_in_process(&pool, chunk.id, true).await {
                    error!("Failed to mark chunk {} in-process: {e}", chunk.id);
                    return;
                }

                // Extract filename from path
                let filename = Path::new(&chunk.chunk_file_path)
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_else(|| {
                        warn!("Chunk {} has no filename in path, using fallback", chunk.id);
                        format!("chunk_{}.bin", chunk.id)
                    });

                let s3_key = S3Client::chunk_key(&event_id, &filename);

                // Upload to S3 with retry
                let mut uploaded = false;
                for attempt in 0..MAX_RETRIES {
                    match s3
                        .upload_file(Path::new(&chunk.chunk_file_path), &s3_key)
                        .await
                    {
                        Ok(()) => {
                            uploaded = true;
                            break;
                        }
                        Err(e) => {
                            if attempt + 1 < MAX_RETRIES {
                                warn!(
                                    "S3 upload failed for chunk {} (attempt {}/{}): {e}, retrying in {}s",
                                    chunk.id, attempt + 1, MAX_RETRIES, RETRY_DELAY.as_secs()
                                );
                                tokio::time::sleep(RETRY_DELAY).await;
                            } else {
                                warn!(
                                    "S3 upload failed for chunk {} after {MAX_RETRIES} attempts: {e}",
                                    chunk.id
                                );
                            }
                        }
                    }
                }
                if !uploaded {
                    if let Err(re) = db::set_chunk_in_process(&pool, chunk.id, false).await {
                        error!("Failed to rollback in_process for chunk {}: {re}", chunk.id);
                    }
                    return;
                }

                // Notify manager with retry
                let notification = ChunkUploadNotification {
                    event_identifier: event_id.clone(),
                    chunk_filename: filename.clone(),
                    data_size: chunk.data_size,
                    md5: chunk.md5.clone(),
                };
                let mut notified = false;
                for attempt in 0..MAX_RETRIES {
                    match manager.notify_chunk_uploaded(&notification).await {
                        Ok(()) => {
                            notified = true;
                            break;
                        }
                        Err(e) => {
                            if attempt + 1 < MAX_RETRIES {
                                warn!(
                                    "Manager notification failed for chunk {} (attempt {}/{}): {e}, retrying in {}s",
                                    chunk.id, attempt + 1, MAX_RETRIES, RETRY_DELAY.as_secs()
                                );
                                tokio::time::sleep(RETRY_DELAY).await;
                            } else {
                                warn!(
                                    "Manager notification failed for chunk {} after {MAX_RETRIES} attempts: {e}",
                                    chunk.id
                                );
                            }
                        }
                    }
                }
                if !notified {
                    if let Err(re) = db::set_chunk_in_process(&pool, chunk.id, false).await {
                        error!("Failed to rollback in_process for chunk {}: {re}", chunk.id);
                    }
                    return;
                }

                // Verify with manager
                match manager.check_chunk(&event_id, &filename).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!("Manager did not verify chunk {}", chunk.id);
                        if let Err(re) = db::set_chunk_in_process(&pool, chunk.id, false).await {
                            error!("Failed to rollback in_process for chunk {}: {re}", chunk.id);
                        }
                        return;
                    }
                    Err(e) => {
                        warn!("Chunk verification failed for {}: {e}", chunk.id);
                        if let Err(re) = db::set_chunk_in_process(&pool, chunk.id, false).await {
                            error!("Failed to rollback in_process for chunk {}: {re}", chunk.id);
                        }
                        return;
                    }
                }

                // Mark as sent
                if let Err(e) = db::set_chunk_sent(&pool, chunk.id).await {
                    error!("Failed to mark chunk {} as sent: {e}", chunk.id);
                    return;
                }

                // Delete local file
                if let Err(e) = tokio::fs::remove_file(&chunk.chunk_file_path).await {
                    error!(
                        "Failed to delete chunk file {} — disk may fill: {e}",
                        chunk.chunk_file_path
                    );
                }

                info!("Chunk {} uploaded and verified", chunk.id);
                if let Err(e) = ws_tx.send(WsEvent::ChunkUploaded { chunk_id: chunk.id }) {
                    debug!("No WS subscribers for ChunkUploaded: {e}");
                }
            }));
        }

        for handle in handles {
            if let Err(e) = handle.await {
                error!("Upload task panicked: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::config::S3Config;
    use rs_core::db;

    fn test_s3_config() -> S3Config {
        S3Config {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "test-key".to_string(),
            secret_access_key: "test-secret".to_string(),
        }
    }

    async fn setup_db() -> SqlitePool {
        let pool = db::create_pool(std::path::Path::new(":memory:"))
            .await
            .unwrap();
        db::run_migrations(&pool).await.unwrap();
        db::upsert_client_profile(&pool, "test-uuid").await.unwrap();
        pool
    }

    #[tokio::test]
    async fn uploader_shuts_down_cleanly() {
        let pool = setup_db().await;
        let s3 = S3Client::new(&test_s3_config()).unwrap();
        let manager = crate::manager_api::ManagerClient::new("http://127.0.0.1:1").unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, manager, ws_tx);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        let handle = tokio::spawn(async move { uploader.run(shutdown_rx).await });

        // Let it run one batch cycle (should find no chunks)
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Signal shutdown
        let _ = shutdown_tx.send(());

        // Should complete without panic
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("uploader timed out")
            .expect("uploader panicked");
    }

    #[tokio::test]
    async fn upload_batch_with_no_chunks_returns_quickly() {
        let pool = setup_db().await;
        let s3 = S3Client::new(&test_s3_config()).unwrap();
        let manager = crate::manager_api::ManagerClient::new("http://127.0.0.1:1").unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, manager, ws_tx);

        // upload_batch should return immediately when no chunks exist
        uploader.upload_batch().await;
    }

    #[tokio::test]
    async fn uploader_constructor_sets_defaults() {
        let pool = setup_db().await;
        let s3 = S3Client::new(&test_s3_config()).unwrap();
        let manager = crate::manager_api::ManagerClient::new("http://127.0.0.1:1").unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, manager, ws_tx);
        assert_eq!(uploader.max_concurrent, 4);
    }

    #[test]
    fn retry_constants_are_valid() {
        assert!(MAX_RETRIES > 0);
        assert!(RETRY_DELAY.as_secs() > 0);
    }
}
