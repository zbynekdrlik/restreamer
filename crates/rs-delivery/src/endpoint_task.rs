/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
use async_trait::async_trait;
use rs_ffmpeg::{ChunkFormat, FfmpegProcess, ServiceType};
use rs_ts_normalize::TSTimestampNormalizer;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::api::{EndpointConfig, S3Config};
use crate::s3_fetch::S3Fetcher;

/// FLV stream normalizer: strips duplicate FLV headers and sequence headers
/// from concatenated FLV chunks, producing a single continuous FLV stream.
struct FlvStreamNormalizer {
    sent_header: bool,
}

impl FlvStreamNormalizer {
    fn new() -> Self {
        Self { sent_header: false }
    }

    /// Normalize an FLV chunk for continuous streaming.
    /// First chunk: pass through as-is (has FLV header + sequence headers).
    /// Subsequent chunks: strip FLV header (9+4 bytes) and sequence header tags,
    /// keeping only data tags.
    fn normalize(&mut self, data: &[u8]) -> Vec<u8> {
        // Not valid FLV — pass through raw
        if data.len() < 13 || &data[0..3] != b"FLV" {
            return data.to_vec();
        }

        if !self.sent_header {
            // First chunk: send everything as-is
            self.sent_header = true;
            return data.to_vec();
        }

        // Subsequent chunks: skip FLV header and sequence header tags
        let mut offset = 9 + 4; // Skip FLV header + first prev_tag_size
        let mut result = Vec::with_capacity(data.len());

        while offset + 11 <= data.len() {
            let tag_type = data[offset];
            if tag_type != 8 && tag_type != 9 && tag_type != 18 {
                break;
            }

            let data_size = ((data[offset + 1] as u32) << 16)
                | ((data[offset + 2] as u32) << 8)
                | (data[offset + 3] as u32);

            let tag_total = 11 + data_size as usize + 4; // header + body + prev_tag_size

            if offset + tag_total > data.len() {
                break;
            }

            // Check if this is a sequence header (skip it — already sent in first chunk)
            let is_seq_header = (tag_type == 9 || tag_type == 8)
                && offset + 12 < data.len()
                && data[offset + 12] == 0x00;

            if !is_seq_header {
                // Copy data tag as-is (with absolute timestamps from xiu)
                result.extend_from_slice(&data[offset..offset + tag_total]);
            }

            offset += tag_total;
        }

        result
    }
}

const MAX_FFMPEG_RESTARTS: u32 = 10;
const MAX_CHUNK_MISS_COUNT: u32 = 60; // ~2min at 2s polls
const SKIP_AHEAD_PROBE: i64 = 10;
const WRITE_TIMEOUT_SECS: u64 = 30;
/// Base S3 backoff (doubles on each error, max 60s, resets on success).
const S3_BACKOFF_BASE_SECS: u64 = 2;
const S3_BACKOFF_MAX_SECS: u64 = 60;
/// Heartbeat interval for endpoint delivery loop.
const ENDPOINT_HEARTBEAT_SECS: u64 = 60;

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
    let use_ts_normalizer = chunk_format == ChunkFormat::Ts
        && (service_type == ServiceType::YtHls || service_type == ServiceType::YtRtmp);
    let mut normalizer = if use_ts_normalizer {
        Some(TSTimestampNormalizer::new())
    } else {
        None
    };

    // FLV stream normalizer: strips duplicate FLV headers from concatenated chunks.
    let mut flv_normalizer = if chunk_format == ChunkFormat::Flv {
        Some(FlvStreamNormalizer::new())
    } else {
        None
    };

    tracing::info!(
        alias = %alias,
        chunk_format = ?chunk_format,
        use_ts_normalizer,
        "Endpoint delivery configured"
    );

    let mut chunk_id = start_chunk_id;
    let mut proc: Option<Box<dyn OutputProcess>> = None;
    let mut consecutive_ffmpeg_failures: u32 = 0;
    let mut consecutive_chunk_misses: u32 = 0;
    let mut circuit_trips: u32 = 0;
    let mut s3_backoff_secs: u64 = S3_BACKOFF_BASE_SECS;
    let mut last_heartbeat = std::time::Instant::now();

    loop {
        // Check for stop signal
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Stop signal received");
            break;
        }

        // Periodic heartbeat
        if last_heartbeat.elapsed() >= std::time::Duration::from_secs(ENDPOINT_HEARTBEAT_SECS) {
            tracing::info!(
                alias = %alias,
                chunk_id,
                ffmpeg_alive = proc.as_mut().is_some_and(|p| p.is_alive()),
                "Delivery endpoint heartbeat"
            );
            last_heartbeat = std::time::Instant::now();
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
                if use_ts_normalizer {
                    normalizer = Some(TSTimestampNormalizer::new());
                }
                if chunk_format == ChunkFormat::Flv {
                    flv_normalizer = Some(FlvStreamNormalizer::new());
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
                        circuit_trips += 1;
                        let cooldown = (30 * 2u64.pow(circuit_trips.min(4) - 1)).min(300);
                        tracing::error!(
                            alias = %alias,
                            failures = consecutive_ffmpeg_failures,
                            circuit_trip = circuit_trips,
                            "ffmpeg circuit breaker #{circuit_trips}, cooldown {cooldown}s"
                        );
                        s.stall_reason = Some("ffmpeg_crash_loop".to_string());
                        drop(s);
                        let sleep_dur = std::time::Duration::from_secs(cooldown);
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
                s3_backoff_secs = S3_BACKOFF_BASE_SECS;
                if circuit_trips > 0 {
                    circuit_trips = 0;
                    tracing::info!(alias = %alias, "ffmpeg circuit breaker reset after successful chunk");
                }
                {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = 0;
                    if s.stall_reason.as_deref() == Some("chunk_gap") {
                        s.stall_reason = None;
                    }
                }

                let processed = if let Some(ref mut norm) = normalizer {
                    norm.normalize(&data)
                } else if let Some(ref mut flv_norm) = flv_normalizer {
                    flv_norm.normalize(&data)
                } else {
                    data
                };

                // Write full chunk directly to ffmpeg with timeout.
                // ffmpeg handles its own output buffering to RTMP/HLS.
                if let Some(ref mut p) = proc {
                    let write_result = tokio::time::timeout(
                        std::time::Duration::from_secs(WRITE_TIMEOUT_SECS),
                        p.write(&processed),
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
                // No artificial sleep — delivery is naturally paced by S3 chunk
                // availability. When we catch up to the live edge, fetch_chunk
                // returns None and the loop waits 2s before retrying.
                // This keeps the cache delay stable at the configured target.
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
                            // Reset normalizers on skip
                            if use_ts_normalizer {
                                normalizer = Some(TSTimestampNormalizer::new());
                            }
                            if chunk_format == ChunkFormat::Flv {
                                flv_normalizer = Some(FlvStreamNormalizer::new());
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
                tracing::error!(
                    alias = %alias,
                    chunk_id,
                    backoff_secs = s3_backoff_secs,
                    "S3 fetch error, retrying in {s3_backoff_secs}s: {e}"
                );
                let mut s = stats.lock().await;
                s.last_error = Some(e);
                drop(s);
                tokio::time::sleep(std::time::Duration::from_secs(s3_backoff_secs)).await;
                s3_backoff_secs = (s3_backoff_secs * 2).min(S3_BACKOFF_MAX_SECS);
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
