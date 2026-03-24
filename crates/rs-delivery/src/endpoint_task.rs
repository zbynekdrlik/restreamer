/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
use async_trait::async_trait;
use rs_ffmpeg::{ChunkFormat, FfmpegProcess, ServiceType};
use rs_ts_normalize::TSTimestampNormalizer;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::api::{EndpointConfig, S3Config};
use crate::s3_fetch::S3Fetcher;

/// Interval for smooth byte-level writes (10ms = 100 writes per second)
const SMOOTH_WRITE_INTERVAL_MS: u64 = 10;

const MAX_FFMPEG_RESTARTS: u32 = 10;
const CIRCUIT_BREAKER_COOLDOWN_SECS: u64 = 30;
const MAX_CHUNK_MISS_COUNT: u32 = 60; // ~2min at 2s polls
const SKIP_AHEAD_PROBE: i64 = 10;
const WRITE_TIMEOUT_SECS: u64 = 30;

/// Trait for fetching chunks (S3 or mock).
pub trait ChunkFetcher: Send + Sync {
    fn fetch_chunk(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<Vec<u8>>, String>> + Send;
}

/// Trait for output process (ffmpeg or mock).
/// Uses async_trait for object safety (Box<dyn OutputProcess>).
#[async_trait]
pub trait OutputProcess: Send {
    fn is_alive(&mut self) -> bool;
    async fn write(&mut self, data: &[u8]) -> Result<(), String>;
    async fn kill(&mut self);
    fn last_stderr_line(&self) -> Option<String>;
}

/// Factory for spawning output processes.
pub trait OutputProcessFactory: Send + Sync {
    fn spawn(
        &self,
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
        chunk_format: ChunkFormat,
    ) -> Result<Box<dyn OutputProcess>, String>;
}

/// Real S3 chunk fetcher implementing ChunkFetcher.
impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
        S3Fetcher::fetch_chunk(self, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Real ffmpeg process implementing OutputProcess.
#[async_trait]
impl OutputProcess for FfmpegProcess {
    fn is_alive(&mut self) -> bool {
        self.try_exit_code().is_none()
    }

    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        rs_ffmpeg::FfmpegProcess::write(self, data)
            .await
            .map_err(|e| e.to_string())
    }

    async fn kill(&mut self) {
        rs_ffmpeg::FfmpegProcess::kill(self).await;
    }

    fn last_stderr_line(&self) -> Option<String> {
        rs_ffmpeg::FfmpegProcess::last_stderr_line(self)
    }
}

/// Real ffmpeg process factory.
pub struct FfmpegProcessFactory;

impl OutputProcessFactory for FfmpegProcessFactory {
    fn spawn(
        &self,
        service_type: ServiceType,
        stream_key: &str,
        alias: &str,
        chunk_format: ChunkFormat,
    ) -> Result<Box<dyn OutputProcess>, String> {
        FfmpegProcess::spawn_with_format(service_type, stream_key, alias, chunk_format)
            .map(|p| Box::new(p) as Box<dyn OutputProcess>)
            .map_err(|e| e.to_string())
    }
}

/// Stats tracked per endpoint with diagnostics.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct EndpointStats {
    pub bytes_processed_total: u64,
    pub current_chunk_id: i64,
    pub chunks_processed: u64,
    // Diagnostics
    pub ffmpeg_restart_count: u32,
    pub consecutive_ffmpeg_failures: u32,
    pub consecutive_chunk_misses: u32,
    pub last_error: Option<String>,
    pub stall_reason: Option<String>,
    pub ffmpeg_last_stderr: Option<String>,
}

pub type Stats = Arc<Mutex<EndpointStats>>;

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
        delivery_delay_chunks: i64,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats {
            current_chunk_id: start_chunk_id,
            ..Default::default()
        }));

        let fetcher = match S3Fetcher::new(&s3_cfg, &event_identifier) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(alias = %ep_cfg.alias, "Failed to create S3 fetcher: {e}");
                let stats_clone = stats.clone();
                let task = tokio::spawn(async move {
                    let mut s = stats_clone.lock().await;
                    s.last_error = Some(format!("S3 fetcher init failed: {e}"));
                });
                return Self {
                    task,
                    stop_tx,
                    stats,
                };
            }
        };

        // Fast endpoints skip the delay entirely
        let effective_delay = if ep_cfg.is_fast {
            0
        } else {
            delivery_delay_chunks
        };

        let task = tokio::spawn(endpoint_loop(
            fetcher,
            FfmpegProcessFactory,
            ep_cfg,
            start_chunk_id,
            effective_delay,
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

    pub async fn stats(&self) -> EndpointStats {
        self.stats.lock().await.clone()
    }

    pub async fn stop(self) {
        let _ = self.stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), self.task).await;
    }
}

/// Write data to ffmpeg stdin spread evenly over `duration`.
/// Eliminates burst+gap pattern that causes YouTube's bitrateLow.
/// At 12 Mbps, a 1.5 MB chunk is written as ~15 KB every 10ms.
async fn smooth_write(
    proc: &mut dyn OutputProcess,
    data: &[u8],
    duration: std::time::Duration,
) -> Result<(), String> {
    let total = data.len();
    if total == 0 {
        return Ok(());
    }

    let interval = std::time::Duration::from_millis(SMOOTH_WRITE_INTERVAL_MS);
    let num_intervals = (duration.as_millis() / interval.as_millis()).max(1) as usize;
    let block_size = total.div_ceil(num_intervals);

    let start = tokio::time::Instant::now();
    let mut offset = 0;

    for i in 0..num_intervals {
        let end = (offset + block_size).min(total);
        if offset >= total {
            break;
        }

        proc.write(&data[offset..end]).await?;
        offset = end;

        // Sleep until next write slot
        let target = interval * (i as u32 + 1);
        let elapsed = start.elapsed();
        if elapsed < target {
            tokio::time::sleep(target - elapsed).await;
        }
    }

    // Write any remaining bytes
    if offset < total {
        proc.write(&data[offset..]).await?;
    }

    Ok(())
}

/// Core endpoint loop — generic over ChunkFetcher and OutputProcessFactory for testability.
#[allow(clippy::too_many_arguments)]
pub async fn endpoint_loop<F: ChunkFetcher, P: OutputProcessFactory>(
    fetcher: F,
    factory: P,
    ep_cfg: EndpointConfig,
    start_chunk_id: i64,
    delivery_delay_chunks: i64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
) {
    let alias = ep_cfg.alias.clone();

    // Wait for enough chunks to buffer before starting (delayed start approach)
    if delivery_delay_chunks > 0 {
        let target_chunk = start_chunk_id + delivery_delay_chunks;
        tracing::info!(alias = %alias, target_chunk, "Waiting for buffer fill");
        loop {
            if *stop_rx.borrow() {
                return;
            }
            if let Ok(Some(_)) = fetcher.fetch_chunk(target_chunk).await {
                tracing::info!(alias = %alias, target_chunk, "Buffer filled");
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                _ = stop_rx.changed() => { if *stop_rx.borrow() { return; } }
            }
        }
    }

    let service_type: ServiceType = match ep_cfg.service_type.parse() {
        Ok(st) => st,
        Err(e) => {
            tracing::error!(alias = %alias, "Unknown service type '{}': {e}", ep_cfg.service_type);
            return;
        }
    };

    let chunk_format = ChunkFormat::from_config(&ep_cfg.chunk_format);

    // TS normalizer only needed for MPEG-TS chunks (fixes cross-chunk timestamp discontinuities).
    // FLV chunks have correct timestamps from xiu — no normalization needed.
    let use_normalizer = chunk_format == ChunkFormat::Ts
        && (service_type == ServiceType::YtHls || service_type == ServiceType::YtRtmp);
    let mut normalizer = if use_normalizer {
        Some(TSTimestampNormalizer::new())
    } else {
        None
    };

    tracing::info!(
        alias = %alias,
        chunk_format = ?chunk_format,
        use_normalizer,
        "Endpoint delivery configured"
    );

    let mut chunk_id = start_chunk_id;
    let mut proc: Option<Box<dyn OutputProcess>> = None;
    let mut consecutive_ffmpeg_failures: u32 = 0;
    let mut consecutive_chunk_misses: u32 = 0;

    loop {
        // Check for stop signal
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Stop signal received");
            break;
        }

        // Ensure output process is running
        if !proc.as_mut().is_some_and(|p| p.is_alive()) {
            if proc.is_some() {
                let mut s = stats.lock().await;
                s.ffmpeg_restart_count += 1;
                // Capture stderr before dropping
                if let Some(ref mut p) = proc {
                    s.ffmpeg_last_stderr = p.last_stderr_line();
                }
                drop(s);
                tracing::warn!(alias = %alias, "ffmpeg died, restarting in 3s");
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                if use_normalizer {
                    normalizer = Some(TSTimestampNormalizer::new());
                }
            }

            match factory.spawn(service_type, &ep_cfg.stream_key, &alias, chunk_format) {
                Ok(new_proc) => {
                    tracing::info!(alias = %alias, "ffmpeg started");
                    proc = Some(new_proc);
                    consecutive_ffmpeg_failures = 0;
                    let mut s = stats.lock().await;
                    s.consecutive_ffmpeg_failures = 0;
                    if s.stall_reason.as_deref() == Some("ffmpeg_crash_loop") {
                        s.stall_reason = None;
                    }
                }
                Err(e) => {
                    consecutive_ffmpeg_failures += 1;
                    let mut s = stats.lock().await;
                    s.consecutive_ffmpeg_failures = consecutive_ffmpeg_failures;
                    s.last_error = Some(e.clone());

                    // Circuit breaker: after MAX_FFMPEG_RESTARTS, enter cooldown
                    if consecutive_ffmpeg_failures >= MAX_FFMPEG_RESTARTS {
                        let cooldown = CIRCUIT_BREAKER_COOLDOWN_SECS;
                        tracing::error!(
                            alias = %alias,
                            failures = consecutive_ffmpeg_failures,
                            "ffmpeg circuit breaker, cooldown {cooldown}s"
                        );
                        s.stall_reason = Some("ffmpeg_crash_loop".to_string());
                        drop(s);
                        let sleep_dur =
                            std::time::Duration::from_secs(CIRCUIT_BREAKER_COOLDOWN_SECS);
                        tokio::select! {
                            _ = tokio::time::sleep(sleep_dur) => {}
                            _ = stop_rx.changed() => {
                                if *stop_rx.borrow() { break; }
                            }
                        }
                        consecutive_ffmpeg_failures = 0;
                        let mut s = stats.lock().await;
                        s.consecutive_ffmpeg_failures = 0;
                    } else {
                        drop(s);
                        tracing::error!(alias = %alias, "Failed to spawn ffmpeg: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    continue;
                }
            }
        }

        // Fetch next chunk from S3
        match fetcher.fetch_chunk(chunk_id).await {
            Ok(Some(data)) => {
                consecutive_chunk_misses = 0;
                {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = 0;
                    if s.stall_reason.as_deref() == Some("chunk_gap") {
                        s.stall_reason = None;
                    }
                }

                let processed = if let Some(ref mut norm) = normalizer {
                    norm.normalize(&data)
                } else {
                    data
                };

                if let Some(ref mut p) = proc {
                    // Smooth write: spread bytes evenly over chunk duration.
                    // This eliminates the burst+gap pattern that causes
                    // YouTube's bitrateLow warning.
                    let pace =
                        std::time::Duration::from_millis(if ep_cfg.is_fast { 100 } else { 1000 });
                    let write_result = tokio::time::timeout(
                        std::time::Duration::from_secs(WRITE_TIMEOUT_SECS),
                        smooth_write(p.as_mut(), &processed, pace),
                    )
                    .await;

                    match write_result {
                        Ok(Ok(())) => {
                            let mut s = stats.lock().await;
                            s.bytes_processed_total += processed.len() as u64;
                            s.current_chunk_id = chunk_id;
                            s.chunks_processed += 1;
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(alias = %alias, "ffmpeg write failed: {e}");
                            let mut s = stats.lock().await;
                            s.last_error = Some(e);
                            s.ffmpeg_restart_count += 1;
                            drop(s);
                            if let Some(mut p) = proc.take() {
                                p.kill().await;
                            }
                            continue;
                        }
                        Err(_) => {
                            tracing::error!(alias = %alias, "ffmpeg write timed out");
                            let mut s = stats.lock().await;
                            s.last_error = Some("write_timeout".to_string());
                            s.stall_reason = Some("write_timeout".to_string());
                            s.ffmpeg_restart_count += 1;
                            drop(s);
                            if let Some(mut p) = proc.take() {
                                p.kill().await;
                            }
                            continue;
                        }
                    }
                }

                chunk_id += 1;
                // No separate sleep — smooth_write inherently paces delivery
            }
            Ok(None) => {
                consecutive_chunk_misses += 1;

                // Chunk gap skip-ahead logic
                if consecutive_chunk_misses >= MAX_CHUNK_MISS_COUNT {
                    tracing::warn!(
                        alias = %alias,
                        chunk_id,
                        misses = consecutive_chunk_misses,
                        "Probing ahead for chunks"
                    );

                    let mut found_ahead = false;
                    for offset in 1..=SKIP_AHEAD_PROBE {
                        let probe_id = chunk_id + offset;
                        if let Ok(Some(_)) = fetcher.fetch_chunk(probe_id).await {
                            tracing::info!(
                                alias = %alias,
                                from = chunk_id,
                                to = probe_id,
                                "Skipping ahead to chunk"
                            );
                            chunk_id = probe_id;
                            consecutive_chunk_misses = 0;
                            // Reset normalizer on skip
                            if use_normalizer {
                                normalizer = Some(TSTimestampNormalizer::new());
                            }
                            let mut s = stats.lock().await;
                            s.consecutive_chunk_misses = 0;
                            s.stall_reason = None;
                            found_ahead = true;
                            break;
                        }
                    }

                    if !found_ahead {
                        let mut s = stats.lock().await;
                        s.stall_reason = Some("chunk_gap".to_string());
                        s.consecutive_chunk_misses = consecutive_chunk_misses;
                        drop(s);
                        tracing::warn!(
                            alias = %alias,
                            chunk_id,
                            "No chunks found in probe range, marking chunk_gap stall"
                        );
                        // Reset counter so we probe again after another cycle
                        consecutive_chunk_misses = 0;
                    }
                } else {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = consecutive_chunk_misses;
                }

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
                let mut s = stats.lock().await;
                s.last_error = Some(e);
                drop(s);
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    }

    // Cleanup
    if let Some(mut p) = proc {
        p.kill().await;
    }
    tracing::info!(alias = %alias, "Endpoint task stopped");
}

#[cfg(test)]
#[path = "endpoint_task_tests.rs"]
mod tests;
