/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
///
/// Architecture: producer-consumer pipeline with pre-fetch buffer.
///   Producer (S3 fetcher) -> bounded channel (10 chunks ~20s) -> Consumer (ffmpeg writer)
use async_trait::async_trait;
use rs_ffmpeg::{FfmpegProcess, ServiceType};
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use crate::api::{EndpointConfig, S3Config};
use crate::audit_ring::AuditRing;
pub use crate::buffer_state::{BufferState, initial_delivery_mode};
use crate::ffmpeg_reason::{self, ReasonClass};
use crate::s3_fetch::S3Fetcher;

#[path = "flv_normalizer.rs"]
mod flv_normalizer;
pub use flv_normalizer::FlvStreamNormalizer;

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
        rs_ffmpeg::FfmpegProcess::stderr_tail(self)
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

/// One row in the ffmpeg restart audit log. Captures process-death
/// details so operators can diagnose patterns (e.g. all restarts after
/// exactly 65s = upstream session timeout).
#[derive(Debug, Clone, serde::Serialize)]
pub struct FfmpegRestartRecord {
    pub timestamp_ms: i64,
    pub chunk_id: i64,
    pub lifetime_secs: u64,
    /// Serialized `ReasonClass` (e.g. "youtube_rtmp_closed").
    pub reason: String,
    pub stderr_tail: Option<String>,
    pub backoff_secs: u64,
}

/// Cap on the per-endpoint restart history ring buffer.
pub const RESTART_HISTORY_CAP: usize = 100;

/// Per-class consecutive-death counter feeding `ffmpeg_reason::reconnect_floor`.
/// Resets to 1 when the class changes so (e.g.) `NetworkTimeout` -> `YoutubeRtmpClosed`
/// starts a fresh backoff ladder instead of carrying the old exponent.
#[derive(Debug, Clone, Copy, Default)]
pub struct EndpointRestartState {
    pub consecutive_same_class: u32,
    pub last_class: Option<ReasonClass>,
}

impl EndpointRestartState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called after each ffmpeg death. Returns updated state.
    pub fn advance(self, class: ReasonClass) -> Self {
        let count = if self.last_class == Some(class) {
            self.consecutive_same_class.saturating_add(1)
        } else {
            1
        };
        Self {
            consecutive_same_class: count,
            last_class: Some(class),
        }
    }
}

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

/// Build initial EndpointStats for a newly spawned endpoint. Starts from
/// Default (delivery_mode = "normal") and overrides the two fields that
/// differ per-endpoint: current_chunk_id (start position) and
/// delivery_mode (warmup if rescue video configured, else normal).
///
/// Extracted from EndpointHandle::spawn to make the field-assignment
/// mutation-testable without spinning up S3/ffmpeg infrastructure.
/// Uses explicit assignment (not struct literal) so `delete stmt`
/// mutations on each field are caught by unit tests.
#[allow(clippy::field_reassign_with_default)]
pub fn initial_endpoint_stats(start_chunk_id: i64, initial_mode: String) -> EndpointStats {
    let mut s = EndpointStats::default();
    s.current_chunk_id = start_chunk_id;
    s.delivery_mode = initial_mode;
    s
}

pub struct EndpointHandle {
    task: JoinHandle<()>,
    stop_tx: watch::Sender<bool>,
    stats: Stats,
}

impl EndpointHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        ep_cfg: EndpointConfig,
        s3_cfg: S3Config,
        event_identifier: String,
        start_chunk_id: i64,
        delivery_delay_ms: u64,
        rescue_video_url: Option<String>,
        audit_ring: Option<Arc<AuditRing>>,
    ) -> Self {
        let (stop_tx, stop_rx) = watch::channel(false);

        let initial_mode = initial_delivery_mode(
            rescue_video_url.is_some(),
            ep_cfg.is_fast,
            delivery_delay_ms,
        );

        let stats: Stats = Arc::new(Mutex::new(initial_endpoint_stats(
            start_chunk_id,
            initial_mode,
        )));

        let buffer_state = Arc::new(BufferState::new());

        let fetcher = match S3Fetcher::new(&s3_cfg, &event_identifier) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!(alias = %ep_cfg.alias, "Failed to create S3 fetcher: {e}");
                if let Some(ring) = &audit_ring {
                    ring.push(
                        rs_core::audit::Severity::Error,
                        rs_core::audit::Source::Vps,
                        Some(ep_cfg.alias.clone()),
                        rs_core::audit::Action::EndpointFfmpegRestartFailed,
                        serde_json::json!({
                            "phase": "s3_fetcher_init",
                            "error": e.to_string(),
                        }),
                    );
                }
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
            audit_ring,
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
#[allow(clippy::too_many_arguments)]
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

                // Signal producer stall for rescue mode detection.
                // Polls are 2s apart, so 3 misses = ~6s of genuinely no new
                // chunks on S3. Triggering sooner means rescue activates
                // faster after OBS stops, at the cost of occasional false
                // positives on transient S3 errors (which self-heal on the
                // next successful fetch — producer_active returns to true).
                if consecutive_chunk_misses >= 3 {
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
#[allow(clippy::too_many_arguments)]
async fn consumer_task<P: OutputProcessFactory>(
    mut rx: mpsc::Receiver<PrefetchedChunk>,
    factory: P,
    ep_cfg: EndpointConfig,
    mut stop_rx: watch::Receiver<bool>,
    stats: Stats,
    rescue_video_url: Option<String>,
    buffer_state: Arc<BufferState>,
    audit_ring: Option<Arc<AuditRing>>,
) {
    let alias = ep_cfg.alias.clone();
    let service_type_str = ep_cfg.service_type.clone();

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
    // Class-aware backoff: tracks the ReasonClass of the most recent death
    // and how many deaths in a row shared that class. See `ffmpeg_reason`.
    let mut restart_state = EndpointRestartState::new();
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
                // ffmpeg died. Classify from stderr, advance per-class counter,
                // apply class-specific reconnect floor. Reset counter if
                // process lived long enough to prove the session was real.
                const LIFETIME_RESET_SECS: u64 = 60;
                let lifetime_secs = proc_spawned_at.map(|t| t.elapsed().as_secs()).unwrap_or(0);
                if lifetime_secs >= LIFETIME_RESET_SECS {
                    restart_state = EndpointRestartState::new();
                }

                let stderr_tail = proc.as_mut().and_then(|p| p.last_stderr_line());
                let stderr_for_classify = stderr_tail.clone().unwrap_or_default();
                let class = ffmpeg_reason::classify(&service_type_str, &stderr_for_classify);
                restart_state = restart_state.advance(class);

                let floor = ffmpeg_reason::reconnect_floor(
                    class,
                    restart_state.consecutive_same_class.saturating_sub(1),
                );
                let is_process_killed = floor.is_none();
                // reason is the serialized ReasonClass (e.g. "youtube_rtmp_closed").
                let reason_str = serde_json::to_string(&class)
                    .unwrap_or_default()
                    .trim_matches('"')
                    .to_string();
                let backoff_secs = floor.map(|d| d.as_secs()).unwrap_or(0);

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
                    reason: reason_str.clone(),
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

                // Emit EndpointFfmpegDied audit row so the host mirror
                // can pull it next tick. Persisted evidence is what the
                // 2026-04-19 post-mortem lacked; this is the primary
                // sink for per-endpoint death diagnostics.
                if let Some(ring) = &audit_ring {
                    ring.push(
                        rs_core::audit::Severity::Warn,
                        rs_core::audit::Source::Ffmpeg,
                        Some(alias.clone()),
                        rs_core::audit::Action::EndpointFfmpegDied,
                        serde_json::json!({
                            "lifetime_secs": lifetime_secs,
                            "reason": reason_str,
                            "stderr_tail": record.stderr_tail,
                            "backoff_secs": backoff_secs,
                            "consecutive_same_class": restart_state.consecutive_same_class,
                        }),
                    );
                }

                if is_process_killed {
                    tracing::info!(
                        alias = %alias,
                        reason = %reason_str,
                        "Consumer: ffmpeg was intentionally killed; not restarting"
                    );
                    break;
                }

                tracing::warn!(
                    alias = %alias,
                    lifetime_secs,
                    consecutive_same_class = restart_state.consecutive_same_class,
                    reason = %reason_str,
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
                    // Previous spawn existed iff we've ever tracked a start
                    // time. The death handler above keeps `proc` as `Some`
                    // with a dead child, so `proc.is_none()` alone cannot
                    // distinguish first spawn from restart.
                    let was_dead = proc_spawned_at.is_some();
                    proc = Some(new_proc);
                    proc_spawned_at = Some(tokio::time::Instant::now());
                    consecutive_ffmpeg_failures = 0;
                    let mut s = stats.lock().await;
                    s.consecutive_ffmpeg_failures = 0;
                    if s.stall_reason.as_deref() == Some("ffmpeg_crash_loop") {
                        s.stall_reason = None;
                    }
                    drop(s);
                    // Audit: first spawn OR recovery after restart. This is
                    // the VPS-side equivalent of host's "endpoint alive
                    // transition" — a visible post-event milestone.
                    if let Some(ring) = &audit_ring {
                        let action = if was_dead {
                            rs_core::audit::Action::EndpointAliveTransition
                        } else {
                            rs_core::audit::Action::EndpointStarted
                        };
                        ring.push(
                            rs_core::audit::Severity::Info,
                            rs_core::audit::Source::Ffmpeg,
                            Some(alias.clone()),
                            action,
                            serde_json::json!({
                                "service_type": ep_cfg.service_type,
                                "stream_key_len": ep_cfg.stream_key.len(),
                            }),
                        );
                    }
                }
                Err(e) => {
                    consecutive_ffmpeg_failures += 1;
                    let mut s = stats.lock().await;
                    s.consecutive_ffmpeg_failures = consecutive_ffmpeg_failures;
                    s.last_error = Some(e.clone());
                    drop(s);
                    // Audit: spawn failed — operator sees the actual
                    // ffmpeg spawn error (missing binary, permission
                    // denied, etc.) instead of just "endpoint red".
                    if let Some(ring) = &audit_ring {
                        ring.push(
                            rs_core::audit::Severity::Error,
                            rs_core::audit::Source::Ffmpeg,
                            Some(alias.clone()),
                            rs_core::audit::Action::EndpointFfmpegRestartFailed,
                            serde_json::json!({
                                "consecutive_failures": consecutive_ffmpeg_failures,
                                "error": e,
                            }),
                        );
                    }

                    if consecutive_ffmpeg_failures >= MAX_FFMPEG_RESTARTS {
                        circuit_trips += 1;
                        let cooldown = (30 * 2u64.pow(circuit_trips.min(4) - 1)).min(300);
                        tracing::error!(
                            alias = %alias,
                            failures = consecutive_ffmpeg_failures,
                            circuit_trip = circuit_trips,
                            "Consumer: ffmpeg circuit breaker #{circuit_trips}, cooldown {cooldown}s"
                        );
                        let mut s = stats.lock().await;
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

                        // Kill current ffmpeg before entering rescue
                        if let Some(mut p) = proc.take() {
                            p.kill().await;
                        }
                        // Update stats to rescue mode
                        {
                            let mut s = stats.lock().await;
                            s.delivery_mode = "rescue".to_string();
                            s.rescue_eta_secs = Some(crate::rescue::RESCUE_REFILL_TARGET_SECS);
                        }
                        let svc_type: rs_ffmpeg::ServiceType =
                            ep_cfg.service_type.parse().unwrap_or(rs_ffmpeg::ServiceType::TestFile);
                        let should_stop = crate::rescue::run_rescue_loop(
                            &alias,
                            rescue_url,
                            svc_type,
                            &ep_cfg.stream_key,
                            &buffer_state,
                            &stats,
                            &mut stop_rx,
                        )
                        .await;
                        if should_stop {
                            return;
                        }
                        // Rescue complete — reset to normal delivery
                        {
                            let mut s = stats.lock().await;
                            s.delivery_mode = "normal".to_string();
                            s.rescue_eta_secs = None;
                        }
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

        // Rust-side real-time pacing: total chunk duration pulled from the
        // channel never runs ahead of wall-clock elapsed since the anchor.
        // Anchor is set lazily on the first chunk so the initial buffer fill
        // doesn't count against the real-time budget. delivered_ms is
        // incremented BEFORE the write (not after success) — a chunk that
        // fails to write still consumed its real-time budget.
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
    audit_ring: Option<Arc<AuditRing>>,
) {
    let alias = ep_cfg.alias.clone();

    // Wait for enough duration to buffer before starting (duration-based approach).
    // When rescue_video_url is configured and the endpoint is not fast, the
    // helper also spawns a rescue ffmpeg in parallel so viewers see the
    // rescue video (with countdown) during the initial cache fill. Without
    // this, viewers see nothing until ~120s of buffer has accumulated.
    if delivery_delay_ms > 0 {
        let stopped = crate::rescue::run_warmup_loop(
            &fetcher,
            &alias,
            &ep_cfg,
            start_chunk_id,
            delivery_delay_ms,
            rescue_video_url.as_deref(),
            &stats,
            &mut stop_rx,
        )
        .await;
        if stopped {
            return;
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
        audit_ring,
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

#[cfg(test)]
#[path = "endpoint_task_flv_tests.rs"]
mod flv_tests;
