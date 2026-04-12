/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
///
/// Architecture: producer-consumer pipeline with pre-fetch buffer.
///   Producer (S3 fetcher) -> bounded channel (10 chunks ~20s) -> Consumer (ffmpeg writer)
use async_trait::async_trait;
use rs_ffmpeg::{FfmpegProcess, ServiceType};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use crate::api::{EndpointConfig, S3Config};
use crate::s3_fetch::S3Fetcher;

/// FLV stream normalizer: strips duplicate FLV headers and sequence headers
/// from concatenated FLV chunks, producing a single continuous FLV stream.
pub struct FlvStreamNormalizer {
    sent_header: bool,
}

impl Default for FlvStreamNormalizer {
    fn default() -> Self {
        Self::new()
    }
}

impl FlvStreamNormalizer {
    pub fn new() -> Self {
        Self { sent_header: false }
    }

    /// Normalize an FLV chunk for continuous streaming.
    /// First chunk: pass through as-is (has FLV header + sequence headers).
    /// Subsequent chunks: strip FLV header (9+4 bytes) and sequence header tags,
    /// keeping only data tags.
    pub fn normalize(&mut self, data: &[u8]) -> Vec<u8> {
        // Not valid FLV -- pass through raw
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

            // Check if this is a sequence header (skip it -- already sent in first chunk)
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
const MAX_CHUNK_MISS_COUNT: u32 = 40; // ~80s at 2s polls
const SKIP_AHEAD_PROBE: i64 = 10;
const WRITE_TIMEOUT_SECS: u64 = 30;
const MAX_WRITE_FAILURES_PER_CHUNK: u32 = 3;
/// Base S3 backoff (doubles on each error, max 60s, resets on success).
const S3_BACKOFF_BASE_SECS: u64 = 2;
const S3_BACKOFF_MAX_SECS: u64 = 60;
/// Heartbeat interval for endpoint delivery loop.
const ENDPOINT_HEARTBEAT_SECS: u64 = 60;
/// Pre-fetch buffer size: ~20s of chunks (10 x ~2s each).
const PREFETCH_BUFFER_SIZE: usize = 10;

/// A chunk that has been fetched from S3 and is ready for the consumer.
struct PrefetchedChunk {
    chunk_id: i64,
    data: Vec<u8>,
    duration_ms: i64,
}

/// Shared buffer state between producer and consumer for rescue mode.
pub struct BufferState {
    /// Estimated buffer duration in ms (chunks available on S3 ahead of consumer).
    pub buffer_duration_ms: AtomicU64,
    /// Whether the producer is actively finding new chunks (vs stalled).
    pub producer_active: AtomicBool,
}

impl BufferState {
    pub fn new() -> Self {
        Self {
            buffer_duration_ms: AtomicU64::new(0),
            producer_active: AtomicBool::new(true),
        }
    }
}

impl Default for BufferState {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for fetching chunks (S3 or mock).
pub trait ChunkFetcher: Send + Sync {
    fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<(Vec<u8>, i64)>, String>> + Send;

    fn chunk_duration_ms(
        &self,
        chunk_id: i64,
    ) -> impl std::future::Future<Output = Result<Option<i64>, String>> + Send;
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
    ) -> Result<Box<dyn OutputProcess>, String>;
}

/// Real S3 chunk fetcher implementing ChunkFetcher.
impl ChunkFetcher for S3Fetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        match S3Fetcher::fetch_chunk_with_meta(self, chunk_id).await {
            Ok(Some(cd)) => Ok(Some((cd.data, cd.duration_ms))),
            Ok(None) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        S3Fetcher::head_chunk_duration(self, chunk_id)
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
    ) -> Result<Box<dyn OutputProcess>, String> {
        FfmpegProcess::spawn(service_type, stream_key, alias)
            .map(|p| Box::new(p) as Box<dyn OutputProcess>)
            .map_err(|e| e.to_string())
    }
}

/// One row in the ffmpeg restart audit log. Captures everything we know
/// about a process death so the operator can diagnose patterns (e.g. all
/// restarts after exactly 65s = upstream session timeout).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FfmpegRestartRecord {
    /// Wall-clock unix epoch (ms) of the death.
    pub timestamp_ms: i64,
    /// chunk_id the process was on when it died.
    pub chunk_id: i64,
    /// How long the ffmpeg process lived before dying.
    pub lifetime_secs: u64,
    /// Reason classification: "stdin_closed", "spawn_failed", "write_error",
    /// "killed", "init_failed".
    pub reason: String,
    /// Last few stderr lines from ffmpeg (if available).
    pub stderr_tail: Option<String>,
    /// Backoff applied before the next spawn attempt.
    pub backoff_secs: u64,
}

/// Cap on the per-endpoint restart history ring buffer. Keeps the API
/// payload bounded while still showing operators a useful pattern.
pub const RESTART_HISTORY_CAP: usize = 100;

/// Stats tracked per endpoint with diagnostics.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EndpointStats {
    pub bytes_processed_total: u64,
    pub duration_processed_ms: u64,
    pub current_chunk_id: i64,
    pub chunks_processed: u64,
    // Diagnostics
    pub ffmpeg_restart_count: u32,
    pub consecutive_ffmpeg_failures: u32,
    pub consecutive_chunk_misses: u32,
    pub last_error: Option<String>,
    pub stall_reason: Option<String>,
    pub ffmpeg_last_stderr: Option<String>,
    /// Per-endpoint ring buffer of recent ffmpeg restarts. Capped at
    /// RESTART_HISTORY_CAP — oldest dropped first.
    pub restart_history: std::collections::VecDeque<FfmpegRestartRecord>,
    /// Current delivery mode: "normal", "warmup", "rescue", "recovering".
    pub delivery_mode: String,
    /// ETA in seconds until rescue mode ends (warmup or buffer refill).
    pub rescue_eta_secs: Option<u64>,
}

impl Default for EndpointStats {
    fn default() -> Self {
        Self {
            bytes_processed_total: 0,
            duration_processed_ms: 0,
            current_chunk_id: 0,
            chunks_processed: 0,
            ffmpeg_restart_count: 0,
            consecutive_ffmpeg_failures: 0,
            consecutive_chunk_misses: 0,
            last_error: None,
            stall_reason: None,
            ffmpeg_last_stderr: None,
            restart_history: std::collections::VecDeque::new(),
            delivery_mode: "normal".to_string(),
            rescue_eta_secs: None,
        }
    }
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
        delivery_delay_ms: u64,
        rescue_video_url: Option<String>,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);

        let initial_mode = if rescue_video_url.is_some() && !ep_cfg.is_fast && delivery_delay_ms > 0
        {
            "warmup".to_string()
        } else {
            "normal".to_string()
        };

        let stats: Stats = Arc::new(Mutex::new(EndpointStats {
            current_chunk_id: start_chunk_id,
            delivery_mode: initial_mode,
            ..Default::default()
        }));

        let buffer_state = Arc::new(BufferState::new());

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
        let effective_delay = if ep_cfg.is_fast { 0 } else { delivery_delay_ms };

        let task = tokio::spawn(endpoint_loop(
            fetcher,
            FfmpegProcessFactory,
            ep_cfg,
            start_chunk_id,
            effective_delay,
            stop_rx,
            stats.clone(),
            rescue_video_url,
            buffer_state,
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

/// Producer task: fetches chunks from S3 and sends them into the bounded channel.
/// Blocks on channel send when buffer is full (backpressure).
async fn producer_task<F: ChunkFetcher>(
    fetcher: F,
    tx: mpsc::Sender<PrefetchedChunk>,
    start_chunk_id: i64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    alias: String,
    buffer_state: Arc<BufferState>,
) {
    let mut chunk_id = start_chunk_id;
    let mut consecutive_chunk_misses: u32 = 0;
    let mut s3_backoff_secs: u64 = S3_BACKOFF_BASE_SECS;

    loop {
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Producer: stop signal received");
            break;
        }

        // Fetch chunk with metadata in one S3 GET
        match fetcher.fetch_chunk_with_meta(chunk_id).await {
            Ok(Some((data, duration_ms))) => {
                consecutive_chunk_misses = 0;
                s3_backoff_secs = S3_BACKOFF_BASE_SECS;
                {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = 0;
                    if s.stall_reason.as_deref() == Some("chunk_gap") {
                        s.stall_reason = None;
                    }
                }

                let chunk = PrefetchedChunk {
                    chunk_id,
                    data,
                    duration_ms,
                };

                // Track buffer growth for rescue mode
                let current_buf = buffer_state
                    .buffer_duration_ms
                    .load(AtomicOrdering::Relaxed);
                buffer_state.buffer_duration_ms.store(
                    current_buf.saturating_add(duration_ms.max(0) as u64),
                    AtomicOrdering::Relaxed,
                );
                buffer_state
                    .producer_active
                    .store(true, AtomicOrdering::Relaxed);

                // Send into channel; blocks if buffer full (backpressure).
                // If receiver is dropped (consumer gone), stop.
                if tx.send(chunk).await.is_err() {
                    tracing::info!(alias = %alias, "Producer: consumer gone, stopping");
                    break;
                }

                chunk_id += 1;
                tokio::task::yield_now().await;
            }
            Ok(None) => {
                consecutive_chunk_misses += 1;

                // Chunk gap skip-ahead logic
                if consecutive_chunk_misses >= MAX_CHUNK_MISS_COUNT {
                    tracing::warn!(
                        alias = %alias,
                        chunk_id,
                        misses = consecutive_chunk_misses,
                        "Producer: probing ahead for chunks"
                    );

                    let mut found_ahead = false;
                    for offset in 1..=SKIP_AHEAD_PROBE {
                        let probe_id = chunk_id + offset;
                        // Use HEAD (duration check) instead of GET to avoid downloading data
                        if let Ok(Some(_)) = fetcher.chunk_duration_ms(probe_id).await {
                            tracing::info!(
                                alias = %alias,
                                from = chunk_id,
                                to = probe_id,
                                "Producer: skipping ahead to chunk"
                            );
                            chunk_id = probe_id;
                            consecutive_chunk_misses = 0;
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
                            "Producer: no chunks found in probe range"
                        );
                        // Reset counter so we probe again after another cycle
                        consecutive_chunk_misses = 0;
                    }
                } else {
                    let mut s = stats.lock().await;
                    s.consecutive_chunk_misses = consecutive_chunk_misses;
                }

                // Signal producer stall for rescue mode detection
                if consecutive_chunk_misses >= 15 {
                    buffer_state
                        .producer_active
                        .store(false, AtomicOrdering::Relaxed);
                }

                tracing::debug!(alias = %alias, chunk_id, "Producer: chunk not found, waiting");
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
                    "Producer: S3 fetch error, retrying in {s3_backoff_secs}s: {e}"
                );
                let mut s = stats.lock().await;
                s.last_error = Some(e);
                drop(s);
                tokio::time::sleep(std::time::Duration::from_secs(s3_backoff_secs)).await;
                s3_backoff_secs = (s3_backoff_secs * 2).min(S3_BACKOFF_MAX_SECS);
            }
        }
    }

    tracing::info!(alias = %alias, "Producer task stopped");
}

/// Consumer task: pulls pre-fetched chunks from the channel, normalizes FLV, writes to ffmpeg.
/// Never makes S3 calls -- zero network I/O.
///
/// Paces chunk delivery to real time using a wall-clock anchor:
///
///   pacing_anchor + delivered_ms == wall_now
///
/// If we are ahead of the anchor, the consumer sleeps the difference BEFORE
/// writing the next chunk. This is essential because ffmpeg's `-re` pacing
/// re-anchors on every process start — after an ffmpeg restart the fresh
/// process sees FLV timestamps deep in the past (xiu writes absolute
/// timestamps that grow for the lifetime of the ingest), treats the stream
/// as "behind", and drains stdin as fast as possible. Without Rust-side
/// pacing, that would burn through the pre-fetch buffer in seconds, the
/// producer would catch up to the latest S3 chunk, skip-ahead would fire,
/// and the configured cache delay would collapse to ~0s permanently. The
/// anchor is preserved across ffmpeg restarts so the delay is maintained.
async fn consumer_task<P: OutputProcessFactory>(
    mut rx: mpsc::Receiver<PrefetchedChunk>,
    factory: P,
    ep_cfg: EndpointConfig,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    rescue_video_url: Option<String>,
    buffer_state: Arc<BufferState>,
) {
    let alias = ep_cfg.alias.clone();

    let service_type: ServiceType = match ep_cfg.service_type.parse() {
        Ok(st) => st,
        Err(e) => {
            tracing::error!(alias = %alias, "Unknown service type '{}': {e}", ep_cfg.service_type);
            return;
        }
    };

    let mut flv_normalizer = FlvStreamNormalizer::new();
    let mut proc: Option<Box<dyn OutputProcess>> = None;
    let mut consecutive_ffmpeg_failures: u32 = 0;
    // consecutive_ffmpeg_deaths counts ffmpeg processes that died AFTER
    // spawning successfully (e.g. destination RTMP server rejected the
    // stream key after accepting some bytes). consecutive_ffmpeg_failures
    // only counts spawn errors, so without this separate counter the FB
    // stale-key restart loop never backs off.
    let mut consecutive_ffmpeg_deaths: u32 = 0;
    let mut proc_spawned_at: Option<tokio::time::Instant> = None;
    let mut circuit_trips: u32 = 0;
    let mut consecutive_write_failures: u32 = 0;
    let mut last_heartbeat = std::time::Instant::now();

    // Pacing anchor: set when the first successful write completes. Maintained
    // across ffmpeg restarts so the configured cache delay survives restarts.
    // Uses tokio::time::Instant so it respects paused/mock time in tests.
    let mut pacing_anchor: Option<tokio::time::Instant> = None;
    let mut delivered_ms: u64 = 0;

    tracing::info!(alias = %alias, "Consumer: endpoint delivery configured (FLV-only)");

    loop {
        if *stop_rx.borrow() {
            tracing::info!(alias = %alias, "Consumer: stop signal received");
            break;
        }

        // Periodic heartbeat
        if last_heartbeat.elapsed() >= std::time::Duration::from_secs(ENDPOINT_HEARTBEAT_SECS) {
            tracing::info!(
                alias = %alias,
                ffmpeg_alive = proc.as_mut().is_some_and(|p| p.is_alive()),
                "Consumer: delivery endpoint heartbeat"
            );
            last_heartbeat = std::time::Instant::now();
        }

        // Ensure output process is running
        if !proc.as_mut().is_some_and(|p| p.is_alive()) {
            if proc.is_some() {
                // ffmpeg died after running. Track death and back off
                // exponentially. If it lived a long time (>= LIFETIME_RESET
                // secs), this was a real working session — reset the counter
                // so a one-off death after hours doesn't trigger a 60s
                // penalty.
                const LIFETIME_RESET_SECS: u64 = 60;
                let lifetime_secs = proc_spawned_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
                if lifetime_secs >= LIFETIME_RESET_SECS {
                    consecutive_ffmpeg_deaths = 0;
                }

                let stderr_tail = proc.as_mut().and_then(|p| p.last_stderr_line());

                // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s (cap).
                // Without this, a stale stream key causes ~1 ffmpeg restart
                // per minute for the lifetime of the stream (observed: 524
                // restarts in 9.5h).
                let backoff_secs = (1u64 << consecutive_ffmpeg_deaths.min(6)).min(60);
                consecutive_ffmpeg_deaths = consecutive_ffmpeg_deaths.saturating_add(1);

                let current_chunk_id_for_record = {
                    let s = stats.lock().await;
                    s.current_chunk_id
                };
                let timestamp_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                let record = FfmpegRestartRecord {
                    timestamp_ms,
                    chunk_id: current_chunk_id_for_record,
                    lifetime_secs,
                    reason: "stdin_closed".to_string(),
                    stderr_tail: stderr_tail.clone(),
                    backoff_secs,
                };
                let mut s = stats.lock().await;
                s.ffmpeg_restart_count += 1;
                s.ffmpeg_last_stderr = stderr_tail;
                if s.restart_history.len() >= RESTART_HISTORY_CAP {
                    s.restart_history.pop_front();
                }
                s.restart_history.push_back(record);
                drop(s);

                tracing::warn!(
                    alias = %alias,
                    lifetime_secs,
                    consecutive_deaths = consecutive_ffmpeg_deaths,
                    backoff_secs,
                    "Consumer: ffmpeg died, backing off before restart"
                );
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(backoff_secs)) => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }
                flv_normalizer = FlvStreamNormalizer::new();
            }

            match factory.spawn(service_type, &ep_cfg.stream_key, &alias) {
                Ok(new_proc) => {
                    tracing::info!(alias = %alias, "Consumer: ffmpeg started");
                    proc = Some(new_proc);
                    proc_spawned_at = Some(tokio::time::Instant::now());
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

                    if consecutive_ffmpeg_failures >= MAX_FFMPEG_RESTARTS {
                        circuit_trips += 1;
                        let cooldown = (30 * 2u64.pow(circuit_trips.min(4) - 1)).min(300);
                        tracing::error!(
                            alias = %alias,
                            failures = consecutive_ffmpeg_failures,
                            circuit_trip = circuit_trips,
                            "Consumer: ffmpeg circuit breaker #{circuit_trips}, cooldown {cooldown}s"
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
                        tracing::error!(alias = %alias, "Consumer: failed to spawn ffmpeg: {e}");
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                    continue;
                }
            }
        }

        // Pull next chunk from channel (rescue-mode-aware)
        let chunk = tokio::select! {
            maybe_chunk = rx.recv() => {
                match maybe_chunk {
                    Some(c) => {
                        // Decrease buffer duration tracking as consumer pulls chunks
                        let dur = c.duration_ms.max(0) as u64;
                        let current = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
                        buffer_state.buffer_duration_ms.store(current.saturating_sub(dur), AtomicOrdering::Relaxed);
                        c
                    }
                    None => {
                        tracing::info!(alias = %alias, "Consumer: producer gone, stopping");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS)) => {
                if let Some(ref rescue_url) = rescue_video_url {
                    if !buffer_state.producer_active.load(AtomicOrdering::Relaxed) {
                        tracing::warn!(alias = %alias, "Consumer: buffer empty + producer stalled, entering rescue mode");

                        // Kill current ffmpeg
                        if let Some(mut p) = proc.take() {
                            p.kill().await;
                        }

                        // Update stats
                        {
                            let mut s = stats.lock().await;
                            s.delivery_mode = "rescue".to_string();
                            s.rescue_eta_secs = Some(crate::rescue::RESCUE_REFILL_TARGET_SECS);
                        }

                        // Spawn rescue ffmpeg
                        let svc_type: rs_ffmpeg::ServiceType = ep_cfg.service_type.parse().unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                        let ep_url = crate::rescue::endpoint_url_for_service(svc_type, &ep_cfg.stream_key);
                        let out_fmt = crate::rescue::output_format_for_service(svc_type);
                        let rescue_args = crate::rescue::build_rescue_ffmpeg_args(rescue_url, &ep_url, out_fmt, &alias);

                        // Write initial countdown
                        let initial_text = crate::rescue::format_countdown_text(
                            &crate::rescue::DeliveryMode::Rescue { reason: crate::rescue::RescueReason::BufferEmpty },
                            crate::rescue::RESCUE_REFILL_TARGET_SECS,
                        );
                        crate::rescue::write_countdown_file(&alias, &initial_text);

                        // Spawn rescue ffmpeg process
                        let mut rescue_proc = match tokio::process::Command::new("ffmpeg")
                            .args(&rescue_args)
                            .stdin(std::process::Stdio::null())
                            .stdout(std::process::Stdio::null())
                            .stderr(std::process::Stdio::null())
                            .kill_on_drop(true)
                            .spawn()
                        {
                            Ok(p) => p,
                            Err(e) => {
                                tracing::error!(alias = %alias, "Failed to spawn rescue ffmpeg: {e}");
                                continue;
                            }
                        };

                        tracing::info!(alias = %alias, "Consumer: rescue ffmpeg started");

                        let target_ms = crate::rescue::RESCUE_REFILL_TARGET_SECS * 1000;
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                                    let buf_ms = buffer_state.buffer_duration_ms.load(AtomicOrdering::Relaxed);
                                    let eta_secs = if buf_ms >= target_ms { 0 } else { (target_ms - buf_ms) / 1000 };

                                    let text = crate::rescue::format_countdown_text(
                                        &crate::rescue::DeliveryMode::Rescue { reason: crate::rescue::RescueReason::BufferEmpty },
                                        eta_secs,
                                    );
                                    crate::rescue::write_countdown_file(&alias, &text);

                                    {
                                        let mut s = stats.lock().await;
                                        s.delivery_mode = if buffer_state.producer_active.load(AtomicOrdering::Relaxed) {
                                            "recovering".to_string()
                                        } else {
                                            "rescue".to_string()
                                        };
                                        s.rescue_eta_secs = Some(eta_secs);
                                    }

                                    if buf_ms >= target_ms {
                                        tracing::info!(alias = %alias, buf_ms, "Consumer: buffer refilled, exiting rescue mode");
                                        break;
                                    }
                                }
                                _ = stop_rx.changed() => {
                                    if *stop_rx.borrow() {
                                        let _ = rescue_proc.kill().await;
                                        crate::rescue::cleanup_countdown_file(&alias);
                                        // Kill proc and exit outer loop
                                        return;
                                    }
                                }
                            }
                        }

                        // Kill rescue ffmpeg and clean up
                        let _ = rescue_proc.kill().await;
                        crate::rescue::cleanup_countdown_file(&alias);

                        // Update stats back to normal
                        {
                            let mut s = stats.lock().await;
                            s.delivery_mode = "normal".to_string();
                            s.rescue_eta_secs = None;
                        }

                        // Reset normalizer for fresh ffmpeg
                        flv_normalizer = FlvStreamNormalizer::new();
                        tracing::info!(alias = %alias, "Consumer: resumed normal delivery");
                    }
                }
                continue;
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    tracing::info!(alias = %alias, "Consumer: stop signal during recv");
                    break;
                }
                continue;
            }
        };

        let chunk_id = chunk.chunk_id;
        let chunk_duration_ms = chunk.duration_ms;
        let processed = flv_normalizer.normalize(&chunk.data);

        // Rust-side real-time pacing. We guarantee that over the lifetime of
        // the consumer (across ffmpeg restarts), the total chunk duration
        // pulled from the channel never runs ahead of wall-clock elapsed
        // since the pacing anchor was set. If we would, sleep the
        // difference. This prevents the producer-consumer buffer from being
        // drained faster than real-time when ffmpeg misbehaves (e.g., just
        // after a restart).
        //
        // The anchor is set lazily on the first chunk pulled from the
        // channel so the initial buffer fill in endpoint_loop doesn't count
        // against the real-time budget and the pipeline enters steady state
        // cleanly.
        //
        // delivered_ms is incremented BEFORE the write (not after success).
        // A chunk that fails to write still "consumed" its real-time budget
        // — without this, failed chunks would let the consumer race past
        // real time and the cache delay would collapse on endpoints whose
        // ffmpeg crashes repeatedly (e.g., stale Facebook stream keys).
        if pacing_anchor.is_none() {
            pacing_anchor = Some(tokio::time::Instant::now());
            tracing::info!(alias = %alias, "Consumer: pacing anchor set");
        }
        if let Some(anchor) = pacing_anchor {
            let elapsed_ms = anchor.elapsed().as_millis() as u64;
            if delivered_ms > elapsed_ms {
                let ahead_ms = delivered_ms - elapsed_ms;
                // Be interruptible so stop signals don't wait for the sleep.
                let sleep_fut = tokio::time::sleep(std::time::Duration::from_millis(ahead_ms));
                tokio::pin!(sleep_fut);
                tokio::select! {
                    _ = &mut sleep_fut => {}
                    _ = stop_rx.changed() => {
                        if *stop_rx.borrow() { break; }
                    }
                }
            }
        }
        delivered_ms += chunk_duration_ms.max(0) as u64;

        if let Some(ref mut p) = proc {
            let write_result = tokio::time::timeout(
                std::time::Duration::from_secs(WRITE_TIMEOUT_SECS),
                p.write(&processed),
            )
            .await;

            match write_result {
                Ok(Ok(())) => {
                    consecutive_write_failures = 0;
                    if circuit_trips > 0 {
                        circuit_trips = 0;
                        tracing::info!(alias = %alias, "Consumer: circuit breaker reset");
                    }
                    let mut s = stats.lock().await;
                    s.bytes_processed_total += processed.len() as u64;
                    s.duration_processed_ms += chunk_duration_ms.max(0) as u64;
                    s.current_chunk_id = chunk_id;
                    s.chunks_processed += 1;
                }
                Ok(Err(e)) => {
                    consecutive_write_failures += 1;
                    tracing::warn!(alias = %alias, chunk_id, failures = consecutive_write_failures, "Consumer: ffmpeg write failed: {e}");
                    let mut s = stats.lock().await;
                    s.last_error = Some(e);
                    drop(s);
                    // Kill the process IN PLACE — leave the Option as Some
                    // so the death handler at the top of the loop catches
                    // it and applies backoff + audit log on the next
                    // iteration. Previously this called proc.take() which
                    // left proc=None, and the death handler's `if
                    // proc.is_some()` check skipped both backoff AND audit.
                    //
                    // Race note: on a real FfmpegProcess, kill() sends
                    // SIGKILL but is_alive() reads the child's try_wait()
                    // exit code non-blockingly. There's a ~ms window where
                    // is_alive() may still return true on the next loop
                    // iteration, causing one more write attempt before the
                    // death handler catches it. The 10ms sleep below gives
                    // the OS time to reap the child so the death handler
                    // runs on the next iteration with accurate
                    // lifetime_secs in the audit entry.
                    if let Some(p) = proc.as_mut() {
                        p.kill().await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if consecutive_write_failures >= MAX_WRITE_FAILURES_PER_CHUNK {
                        tracing::error!(alias = %alias, chunk_id, "Consumer: skipping chunk after {consecutive_write_failures} write failures");
                        consecutive_write_failures = 0;
                        flv_normalizer = FlvStreamNormalizer::new();
                        let mut s = stats.lock().await;
                        s.current_chunk_id = chunk_id;
                    }
                    continue;
                }
                Err(_) => {
                    consecutive_write_failures += 1;
                    tracing::error!(alias = %alias, chunk_id, failures = consecutive_write_failures, "Consumer: ffmpeg write timed out");
                    let mut s = stats.lock().await;
                    s.last_error = Some("write_timeout".to_string());
                    s.stall_reason = Some("write_timeout".to_string());
                    drop(s);
                    // Same fix as above — kill in place, let the death
                    // handler record the audit entry and apply backoff.
                    // The 10ms post-kill sleep gives the OS a chance to
                    // reap the child so is_alive() returns false on the
                    // next iteration. See the race note in the Ok(Err)
                    // branch above.
                    if let Some(p) = proc.as_mut() {
                        p.kill().await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    if consecutive_write_failures >= MAX_WRITE_FAILURES_PER_CHUNK {
                        tracing::error!(alias = %alias, chunk_id, "Consumer: skipping chunk after {consecutive_write_failures} write timeouts");
                        consecutive_write_failures = 0;
                        flv_normalizer = FlvStreamNormalizer::new();
                        let mut s = stats.lock().await;
                        s.current_chunk_id = chunk_id;
                    }
                    continue;
                }
            }
        }
    }

    // Cleanup
    if let Some(mut p) = proc {
        p.kill().await;
    }
    tracing::info!(alias = %alias, "Consumer task stopped");
}

/// Core endpoint loop -- generic over ChunkFetcher and OutputProcessFactory for testability.
/// Orchestrates buffer fill, then spawns producer-consumer pipeline.
#[allow(clippy::too_many_arguments)]
pub async fn endpoint_loop<F: ChunkFetcher + 'static, P: OutputProcessFactory + 'static>(
    fetcher: F,
    factory: P,
    ep_cfg: EndpointConfig,
    start_chunk_id: i64,
    delivery_delay_ms: u64,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    rescue_video_url: Option<String>,
    buffer_state: Arc<BufferState>,
) {
    let alias = ep_cfg.alias.clone();

    // Wait for enough duration to buffer before starting (duration-based approach)
    if delivery_delay_ms > 0 {
        let mut accum_ms: u64 = 0;
        let mut probe_id = start_chunk_id;
        tracing::info!(alias = %alias, delivery_delay_ms, "Waiting for duration-based buffer fill");
        loop {
            if *stop_rx.borrow() {
                return;
            }
            match fetcher.chunk_duration_ms(probe_id).await {
                Ok(Some(dur_ms)) => {
                    accum_ms += dur_ms.max(0) as u64;
                    probe_id += 1;

                    // Update warmup ETA stats
                    if rescue_video_url.is_some() {
                        let mut s = stats.lock().await;
                        s.delivery_mode = "warmup".to_string();
                        let remaining_ms = delivery_delay_ms.saturating_sub(accum_ms);
                        s.rescue_eta_secs = Some(remaining_ms / 1000);
                    }

                    if accum_ms >= delivery_delay_ms {
                        tracing::info!(alias = %alias, accum_ms, probe_id, "Buffer filled");
                        break;
                    }
                }
                Ok(None) => {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                        _ = stop_rx.changed() => { if *stop_rx.borrow() { return; } }
                    }
                }
                Err(e) => {
                    tracing::warn!(alias = %alias, "Buffer fill fetch error: {e}");
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
            }
        }

        // Buffer fill complete — transition to normal mode
        {
            let mut s = stats.lock().await;
            s.delivery_mode = "normal".to_string();
            s.rescue_eta_secs = None;
        }
    }

    tracing::info!(alias = %alias, "Starting producer-consumer pipeline");

    // Create bounded channel for pre-fetch buffer
    let (tx, rx) = mpsc::channel::<PrefetchedChunk>(PREFETCH_BUFFER_SIZE);

    let producer_stop = stop_rx.clone();
    let producer_stats = stats.clone();
    let producer_alias = alias.clone();
    let producer_buffer_state = buffer_state.clone();
    let producer = tokio::spawn(producer_task(
        fetcher,
        tx,
        start_chunk_id,
        producer_stop,
        producer_stats,
        producer_alias,
        producer_buffer_state,
    ));

    let consumer_stop = stop_rx.clone();
    let consumer_stats = stats.clone();
    let consumer = tokio::spawn(consumer_task(
        rx,
        factory,
        ep_cfg,
        consumer_stop,
        consumer_stats,
        rescue_video_url,
        buffer_state,
    ));

    // Wait for either task to finish or stop signal.
    // Both producer and consumer already listen for stop_rx internally,
    // but we also watch here for cleanup coordination.
    tokio::pin!(producer);
    tokio::pin!(consumer);

    loop {
        tokio::select! {
            result = &mut producer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Producer panicked: {e}");
                }
                tracing::info!(alias = %alias, "Producer finished, waiting for consumer to drain");
                // Consumer will stop when channel is drained (recv returns None)
                let _ = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    &mut consumer,
                ).await;
                break;
            }
            result = &mut consumer => {
                if let Err(e) = result {
                    tracing::error!(alias = %alias, "Consumer panicked: {e}");
                }
                tracing::info!(alias = %alias, "Consumer finished, aborting producer");
                producer.abort();
                break;
            }
            _ = stop_rx.changed() => {
                if *stop_rx.borrow() {
                    tracing::info!(alias = %alias, "Stop signal received, aborting pipeline");
                    producer.abort();
                    consumer.abort();
                    break;
                }
            }
        }
    }

    tracing::info!(alias = %alias, "Endpoint pipeline stopped");
}

#[cfg(test)]
#[path = "endpoint_task_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "endpoint_task_pacing_tests.rs"]
mod pacing_tests;

#[cfg(test)]
#[path = "endpoint_task_backoff_tests.rs"]
mod backoff_tests;

#[cfg(test)]
#[path = "endpoint_task_rescue_tests.rs"]
mod rescue_tests;
