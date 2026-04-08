use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use tokio::sync::{Semaphore, broadcast};
use tracing::{debug, error, info, warn};

use rs_core::db;
use rs_core::models::WsEvent;

use crate::s3::S3Client;

/// Maximum retry attempts per batch cycle for transient S3 failures.
/// Failed chunks are automatically retried in subsequent batch cycles.
const MAX_RETRIES: u32 = 10;
/// Base delay for exponential backoff between retries.
const RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
/// Maximum delay cap for exponential backoff.
const RETRY_MAX_DELAY: Duration = Duration::from_secs(30);

/// Watches for unsent chunks and uploads them to S3.
/// No manager notification needed — delivery VPS probes S3 directly.
pub struct ChunkUploader {
    pool: SqlitePool,
    s3: Arc<S3Client>,
    max_concurrent: usize,
    ws_tx: broadcast::Sender<WsEvent>,
    /// When true, upload_batch skips all uploads (simulates S3 outage for testing).
    upload_blocked: Arc<std::sync::atomic::AtomicBool>,
}

impl ChunkUploader {
    pub fn new(pool: SqlitePool, s3: S3Client, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        Self {
            pool,
            s3: Arc::new(s3),
            max_concurrent: 4,
            ws_tx,
            upload_blocked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Set a shared upload-blocked flag (for test API control).
    pub fn with_upload_blocked(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.upload_blocked = flag;
        self
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

    /// Process one batch of unsent chunks.
    /// Public for integration testing; normally called internally via `run()`.
    pub async fn upload_batch(&self) {
        // Test hook: skip uploads when blocked (simulates S3 outage)
        if self
            .upload_blocked
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return;
        }
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
            let ws_tx = self.ws_tx.clone();

            handles.push(tokio::spawn(async move {
                let _permit = permit;

                // Look up the event identifier from the chunk's streaming_event_id
                let event_id = match db::get_streaming_event_by_id(
                    &pool,
                    chunk.streaming_event_id,
                )
                .await
                {
                    Ok(Some(event)) => event.name,
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

                // Upload to S3 with retry — duration stored as S3 object metadata
                let mut uploaded = false;
                for attempt in 0..MAX_RETRIES {
                    match s3
                        .upload_chunk(
                            Path::new(&chunk.chunk_file_path),
                            &event_id,
                            chunk.sequence_number,
                            chunk.duration_ms,
                        )
                        .await
                    {
                        Ok(()) => {
                            uploaded = true;
                            break;
                        }
                        Err(e) => {
                            if attempt + 1 < MAX_RETRIES {
                                let delay = RETRY_BASE_DELAY
                                    .saturating_mul(1 << attempt.min(5))
                                    .min(RETRY_MAX_DELAY);
                                warn!(
                                    "S3 upload failed for chunk {} (attempt {}/{}): {e}, retrying in {:.0}s",
                                    chunk.id, attempt + 1, MAX_RETRIES, delay.as_secs_f64()
                                );
                                tokio::time::sleep(delay).await;
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

                // Mark as sent — no manager notification needed,
                // delivery VPS probes S3 directly by sequential sequence_number
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

                info!("Chunk {} uploaded to S3", chunk.id);
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
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, ws_tx);
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
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, ws_tx);

        // upload_batch should return immediately when no chunks exist
        uploader.upload_batch().await;
    }

    #[tokio::test]
    async fn uploader_constructor_sets_defaults() {
        let pool = setup_db().await;
        let s3 = S3Client::new(&test_s3_config()).unwrap();
        let (ws_tx, _) = broadcast::channel::<WsEvent>(16);

        let uploader = ChunkUploader::new(pool, s3, ws_tx);
        assert_eq!(uploader.max_concurrent, 4);
    }

    #[test]
    fn retry_constants_are_valid() {
        assert!(RETRY_BASE_DELAY.as_secs() > 0);
        assert!(RETRY_MAX_DELAY >= RETRY_BASE_DELAY);
    }

    #[test]
    fn exponential_backoff_delays_are_bounded() {
        for attempt in 0..MAX_RETRIES {
            let delay = RETRY_BASE_DELAY
                .saturating_mul(1 << attempt.min(5))
                .min(RETRY_MAX_DELAY);
            assert!(delay >= RETRY_BASE_DELAY);
            assert!(delay <= RETRY_MAX_DELAY);
        }
    }
}
