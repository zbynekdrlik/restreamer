use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{Semaphore, broadcast};
use tracing::{error, info, warn};

use rs_core::db;
use rs_core::models::WsEvent;

use crate::manager_api::{ChunkUploadNotification, ManagerClient};
use crate::s3::S3Client;

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
        // Get current event identifier from DB (fresh each batch)
        let event_identifier = match db::get_streaming_event(&self.pool).await {
            Ok(Some(event)) => match event.identifier {
                Some(id) => id,
                None => return,
            },
            Ok(None) => return,
            Err(e) => {
                error!("Failed to query streaming event: {e}");
                return;
            }
        };

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
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let pool = self.pool.clone();
            let s3 = Arc::clone(&self.s3);
            let manager = Arc::clone(&self.manager);
            let event_id = event_identifier.clone();
            let ws_tx = self.ws_tx.clone();

            handles.push(tokio::spawn(async move {
                let _permit = permit;

                // Mark as in-process
                if let Err(e) = db::set_chunk_in_process(&pool, chunk.id, true).await {
                    error!("Failed to mark chunk {} in-process: {e}", chunk.id);
                    return;
                }

                // Extract filename from path
                let filename = Path::new(&chunk.chunk_file_path)
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_else(|| format!("chunk_{}.bin", chunk.id));

                let s3_key = S3Client::chunk_key(&event_id, &filename);

                // Upload to S3
                match s3
                    .upload_file(Path::new(&chunk.chunk_file_path), &s3_key)
                    .await
                {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("S3 upload failed for chunk {}: {e}", chunk.id);
                        let _ = db::set_chunk_in_process(&pool, chunk.id, false).await;
                        return;
                    }
                }

                // Notify manager
                let notification = ChunkUploadNotification {
                    event_identifier: event_id.clone(),
                    chunk_filename: filename.clone(),
                    data_size: chunk.data_size,
                    md5: chunk.md5.clone(),
                };
                if let Err(e) = manager.notify_chunk_uploaded(&notification).await {
                    warn!("Manager notification failed for chunk {}: {e}", chunk.id);
                    let _ = db::set_chunk_in_process(&pool, chunk.id, false).await;
                    return;
                }

                // Verify with manager
                match manager.check_chunk(&event_id, &filename).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!("Manager did not verify chunk {}", chunk.id);
                        let _ = db::set_chunk_in_process(&pool, chunk.id, false).await;
                        return;
                    }
                    Err(e) => {
                        warn!("Chunk verification failed for {}: {e}", chunk.id);
                        let _ = db::set_chunk_in_process(&pool, chunk.id, false).await;
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
                    warn!("Failed to delete chunk file {}: {e}", chunk.chunk_file_path);
                }

                info!("Chunk {} uploaded and verified", chunk.id);
                let _ = ws_tx.send(WsEvent::ChunkUploaded { chunk_id: chunk.id });
            }));
        }

        for handle in handles {
            let _ = handle.await;
        }
    }
}
