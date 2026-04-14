use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tracing::{debug, error, warn};

use rs_core::db;
use rs_core::models::WsEvent;

use crate::metrics::{UploadEvent, UploadMetrics};
use crate::s3::S3Client;

const MAX_ATTEMPTS: i64 = 10;
const MAX_WALL_CLOCK_MS: i64 = 600_000; // 10 min total retry budget
const MIN_CONCURRENCY: usize = 4;
// MAX_CONCURRENCY is defined but unused here; Task 6 adds the controller.
#[allow(dead_code)]
pub(crate) const MAX_CONCURRENCY: usize = 32;

pub(crate) fn backoff_ms(attempt: i64) -> u64 {
    // 1s, 2s, 4s, 8s, 16s, 30s (cap)
    let base = 1000u64;
    let shift = (attempt.saturating_sub(1) as u32).min(5);
    base.saturating_mul(1 << shift).min(30_000)
}

/// Watches for unsent chunks and uploads them to S3 using a continuous worker pool.
/// Retries live in the DB via `record_upload_failure` (writes `upload_next_retry_at`).
pub struct ChunkUploader {
    pool: SqlitePool,
    s3: Arc<S3Client>,
    ws_tx: broadcast::Sender<WsEvent>,
    /// When true, workers skip uploads (simulates S3 outage for testing).
    upload_blocked: Arc<std::sync::atomic::AtomicBool>,
    metrics: Arc<UploadMetrics>,
    in_flight: Arc<AtomicUsize>,
}

impl ChunkUploader {
    pub fn new(pool: SqlitePool, s3: S3Client, ws_tx: broadcast::Sender<WsEvent>) -> Self {
        Self {
            pool,
            s3: Arc::new(s3),
            ws_tx,
            upload_blocked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            metrics: Arc::new(UploadMetrics::default()),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Set a shared upload-blocked flag (for test API control).
    pub fn with_upload_blocked(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.upload_blocked = flag;
        self
    }

    /// Expose metrics for the /uploads/stats API.
    pub fn metrics(&self) -> Arc<UploadMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Run the worker pool until shutdown signal.
    /// Spawns MIN_CONCURRENCY workers, each running an independent picker loop.
    pub async fn run(&self, shutdown: broadcast::Receiver<()>) {
        let mut handles = Vec::with_capacity(MIN_CONCURRENCY);

        for _ in 0..MIN_CONCURRENCY {
            let pool = self.pool.clone();
            let s3 = Arc::clone(&self.s3);
            let ws_tx = self.ws_tx.clone();
            let upload_blocked = Arc::clone(&self.upload_blocked);
            let metrics = Arc::clone(&self.metrics);
            let in_flight = Arc::clone(&self.in_flight);
            let worker_shutdown = shutdown.resubscribe();

            handles.push(tokio::spawn(async move {
                run_worker(
                    pool,
                    s3,
                    ws_tx,
                    upload_blocked,
                    metrics,
                    in_flight,
                    worker_shutdown,
                )
                .await;
            }));
        }

        // Wait for all workers to finish
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Upload worker panicked: {e}");
            }
        }
    }
}

async fn run_worker(
    pool: SqlitePool,
    s3: Arc<S3Client>,
    ws_tx: broadcast::Sender<WsEvent>,
    upload_blocked: Arc<std::sync::atomic::AtomicBool>,
    metrics: Arc<UploadMetrics>,
    in_flight: Arc<AtomicUsize>,
    mut shutdown: broadcast::Receiver<()>,
) {
    loop {
        // Check shutdown at the top of each iteration
        if shutdown.try_recv().is_ok() {
            break;
        }

        if upload_blocked.load(Ordering::Relaxed) {
            tokio::select! {
                _ = shutdown.recv() => break,
                _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
            }
            continue;
        }

        let now_ms = chrono::Utc::now().timestamp_millis();
        match db::pick_next_uploadable_chunk(&pool, now_ms).await {
            Ok(None) => {
                tokio::select! {
                    _ = shutdown.recv() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {}
                }
                continue;
            }
            Err(e) => {
                error!("Failed to pick next uploadable chunk: {e}");
                tokio::select! {
                    _ = shutdown.recv() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                }
                continue;
            }
            Ok(Some(chunk)) => {
                // Resolve event identifier; if parent is gone, mark as sent and drop out of queue
                let event_id =
                    match db::get_streaming_event_by_id(&pool, chunk.streaming_event_id).await {
                        Ok(Some(ev)) => ev.name,
                        _ => {
                            warn!(
                                "Chunk {} references missing/deleted event {}, marking complete",
                                chunk.id, chunk.streaming_event_id
                            );
                            let _ = db::record_upload_success(&pool, chunk.id, now_ms, 0).await;
                            continue;
                        }
                    };

                let _ = db::record_upload_attempt(&pool, chunk.id, now_ms).await;
                let attempt = chunk.upload_attempts + 1;
                let _ = ws_tx.send(WsEvent::ChunkUploadAttempt {
                    chunk_id: chunk.id,
                    attempt,
                });

                let n = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                metrics.set_in_flight(n);

                let started = Instant::now();
                let result = s3
                    .upload_chunk(
                        Path::new(&chunk.chunk_file_path),
                        &event_id,
                        chunk.sequence_number,
                        chunk.duration_ms,
                    )
                    .await;
                let duration = started.elapsed();
                let n = in_flight.fetch_sub(1, Ordering::SeqCst) - 1;
                metrics.set_in_flight(n);

                match result {
                    Ok(()) => {
                        let completed_at = chrono::Utc::now().timestamp_millis();
                        let _ = db::record_upload_success(
                            &pool,
                            chunk.id,
                            completed_at,
                            duration.as_millis() as i64,
                        )
                        .await;
                        let _ = tokio::fs::remove_file(&chunk.chunk_file_path).await;
                        metrics.record(UploadEvent {
                            at: Instant::now(),
                            duration_ms: duration.as_millis() as u32,
                            success: true,
                        });
                        debug!("Chunk {} uploaded to S3", chunk.id);
                        let _ = ws_tx.send(WsEvent::ChunkUploaded { chunk_id: chunk.id });
                    }
                    Err(e) => {
                        let wall_clock_ms = chrono::Utc::now().timestamp_millis()
                            - chunk.upload_first_attempt_at.unwrap_or(now_ms);
                        let permanent =
                            attempt >= MAX_ATTEMPTS || wall_clock_ms >= MAX_WALL_CLOCK_MS;
                        if permanent {
                            let _ = db::mark_upload_permanently_failed(&pool, chunk.id).await;
                        } else {
                            let backoff = backoff_ms(attempt) as i64;
                            let next_retry = chrono::Utc::now().timestamp_millis() + backoff;
                            let _ = db::record_upload_failure(
                                &pool,
                                chunk.id,
                                &e.to_string(),
                                next_retry,
                                duration.as_millis() as i64,
                            )
                            .await;
                        }
                        let _ = ws_tx.send(WsEvent::ChunkUploadFailed {
                            chunk_id: chunk.id,
                            error: e.to_string(),
                            permanent,
                        });
                        metrics.record(UploadEvent {
                            at: Instant::now(),
                            duration_ms: duration.as_millis() as u32,
                            success: false,
                        });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::config::S3Config;
    use rs_core::db;
    use std::time::Duration;

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

        // Let workers spin once (should find no chunks and sleep 100ms)
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Signal shutdown
        let _ = shutdown_tx.send(());

        // Should complete without panic
        tokio::time::timeout(Duration::from_secs(3), handle)
            .await
            .expect("uploader timed out")
            .expect("uploader panicked");
    }

    #[test]
    fn backoff_grows_and_caps() {
        assert_eq!(backoff_ms(1), 1000);
        assert_eq!(backoff_ms(2), 2000);
        assert_eq!(backoff_ms(3), 4000);
        assert_eq!(backoff_ms(4), 8000);
        assert_eq!(backoff_ms(5), 16_000);
        assert_eq!(backoff_ms(6), 30_000);
        assert_eq!(backoff_ms(100), 30_000);
    }

    #[test]
    fn backoff_attempt_zero_is_sane() {
        assert!(backoff_ms(0) >= 1000);
    }

    #[test]
    fn max_concurrency_constant_is_valid() {
        assert!(MAX_CONCURRENCY > MIN_CONCURRENCY);
    }
}
