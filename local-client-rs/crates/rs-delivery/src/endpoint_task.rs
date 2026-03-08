/// Per-endpoint tokio task: S3 poll -> normalize -> ffmpeg pipe.
///
/// Each endpoint runs as an independent async task that:
/// 1. Polls S3 for the next chunk by sequential ID
/// 2. Normalizes timestamps (YT_HLS only)
/// 3. Pipes data to ffmpeg stdin
/// 4. Auto-restarts ffmpeg on crash
use crate::api::{EndpointConfig, S3Config};
use crate::s3_fetch::S3Fetcher;
use rs_ffmpeg::{FfmpegProcess, ServiceType};
use rs_ts_normalize::TSTimestampNormalizer;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

/// Stats tracked per endpoint: (bytes_processed, current_chunk_id)
type Stats = Arc<Mutex<(u64, i64)>>;

pub struct EndpointHandle {
    task: JoinHandle<()>,
    stop_tx: watch::Sender<bool>,
    stats: Stats,
}

impl EndpointHandle {
    pub fn spawn(
        ep_cfg: EndpointConfig,
        s3_cfg: S3Config,
        event_identifier: String,
        start_chunk_id: i64,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new((0, start_chunk_id)));

        let task = tokio::spawn(endpoint_loop(
            ep_cfg,
            s3_cfg,
            event_identifier,
            start_chunk_id,
            stop_rx,
            stats.clone(),
        ));

        Self {
            task,
            stop_tx,
            stats,
        }
    }

    pub fn is_alive(&self) -> bool {
        !self.task.is_finished()
    }

    pub async fn stats(&self) -> (u64, i64) {
        *self.stats.lock().await
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.task).await;
    }
}

async fn endpoint_loop(
    ep_cfg: EndpointConfig,
    s3_cfg: S3Config,
    event_identifier: String,
    start_chunk_id: i64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
) {
    let alias = ep_cfg.alias.clone();
    let service_type: ServiceType = ep_cfg.service_type.parse().unwrap_or(ServiceType::TestFile);

    let fetcher = match S3Fetcher::new(&s3_cfg, &event_identifier) {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(alias = %alias, "Failed to create S3 fetcher: {e}");
            return;
        }
    };

    let use_normalizer = service_type == ServiceType::YtHls;
    let mut normalizer = if use_normalizer {
        Some(TSTimestampNormalizer::new())
    } else {
        None
    };

    let mut chunk_id = start_chunk_id;
    let mut ffmpeg: Option<FfmpegProcess> = None;

    loop {
        // Check for stop signal
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Stop signal received");
            break;
        }

        // Ensure ffmpeg is running
        if !ffmpeg.as_mut().is_some_and(|f| f.is_alive()) {
            if ffmpeg.is_some() {
                tracing::warn!(alias = %alias, "ffmpeg died, restarting in 3s");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                // Reset normalizer on ffmpeg restart
                if use_normalizer {
                    normalizer = Some(TSTimestampNormalizer::new());
                }
            }
            match FfmpegProcess::spawn(service_type, &ep_cfg.stream_key, &alias) {
                Ok(proc) => {
                    tracing::info!(alias = %alias, "ffmpeg started");
                    ffmpeg = Some(proc);
                }
                Err(e) => {
                    tracing::error!(alias = %alias, "Failed to spawn ffmpeg: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            }
        }

        // Fetch next chunk from S3
        match fetcher.fetch_chunk(chunk_id).await {
            Ok(Some(data)) => {
                let processed = if let Some(ref mut norm) = normalizer {
                    norm.normalize(&data)
                } else {
                    data
                };

                if let Some(ref mut proc) = ffmpeg {
                    match proc.write(&processed).await {
                        Ok(()) => {
                            let mut s = stats.lock().await;
                            s.0 += processed.len() as u64;
                            s.1 = chunk_id;
                        }
                        Err(e) => {
                            tracing::warn!(alias = %alias, "ffmpeg write failed: {e}");
                            if let Some(mut p) = ffmpeg.take() {
                                p.kill().await;
                            }
                            continue;
                        }
                    }
                }

                chunk_id += 1;
                // Small delay to match real-time playback
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Ok(None) => {
                // Chunk not available yet
                tracing::debug!(alias = %alias, chunk_id, "Chunk not found, waiting");
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }
            }
            Err(e) => {
                tracing::error!(alias = %alias, "S3 fetch error: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }

    // Cleanup
    if let Some(mut proc) = ffmpeg {
        proc.kill().await;
    }
    tracing::info!(alias = %alias, "Endpoint task stopped");
}
