/// Per-endpoint delivery task: S3 poll -> normalize -> ffmpeg pipe.
use async_trait::async_trait;
use rs_ffmpeg::{FfmpegProcess, ServiceType};
use rs_ts_normalize::TSTimestampNormalizer;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use crate::api::{EndpointConfig, S3Config};
use crate::s3_fetch::S3Fetcher;

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
    ) -> Result<Box<dyn OutputProcess>, String> {
        FfmpegProcess::spawn(service_type, stream_key, alias)
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

    let use_normalizer = service_type == ServiceType::YtHls;
    let mut normalizer = if use_normalizer {
        Some(TSTimestampNormalizer::new())
    } else {
        None
    };

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

            match factory.spawn(service_type, &ep_cfg.stream_key, &alias) {
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
                    // Write with timeout
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
                            // Write timeout
                            tracing::error!(
                                alias = %alias,
                                "ffmpeg write timed out"
                            );
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
                // Small delay to match real-time playback
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
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
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use tokio::sync::Mutex as TokioMutex;

    struct MockFetcher {
        chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
    }

    impl MockFetcher {
        fn new(chunks: Vec<(i64, Vec<u8>)>) -> Self {
            let map: std::collections::HashMap<i64, Vec<u8>> = chunks.into_iter().collect();
            Self {
                chunks: Arc::new(TokioMutex::new(map)),
            }
        }
    }

    impl ChunkFetcher for MockFetcher {
        async fn fetch_chunk(&self, chunk_id: i64) -> Result<Option<Vec<u8>>, String> {
            let map = self.chunks.lock().await;
            Ok(map.get(&chunk_id).cloned())
        }
    }

    struct MockProcess {
        alive: Arc<AtomicBool>,
        writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
        fail_after: Option<u32>,
        write_count: u32,
        hang_on_write: bool,
    }

    impl MockProcess {
        fn new(alive: Arc<AtomicBool>, writes: Arc<TokioMutex<Vec<Vec<u8>>>>) -> Self {
            Self {
                alive,
                writes,
                fail_after: None,
                write_count: 0,
                hang_on_write: false,
            }
        }
    }

    #[async_trait]
    impl OutputProcess for MockProcess {
        fn is_alive(&mut self) -> bool {
            self.alive.load(Ordering::Relaxed)
        }

        async fn write(&mut self, data: &[u8]) -> Result<(), String> {
            if self.hang_on_write {
                // Simulate a hanging write
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
                return Ok(());
            }
            self.write_count += 1;
            if let Some(limit) = self.fail_after {
                if self.write_count > limit {
                    self.alive.store(false, Ordering::Relaxed);
                    return Err("mock process died".to_string());
                }
            }
            self.writes.lock().await.push(data.to_vec());
            Ok(())
        }

        async fn kill(&mut self) {
            self.alive.store(false, Ordering::Relaxed);
        }

        fn last_stderr_line(&self) -> Option<String> {
            Some("mock stderr line".to_string())
        }
    }

    struct MockProcessFactory {
        alive: Arc<AtomicBool>,
        writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
        fail_after_writes: Option<u32>,
        spawn_fail: Arc<AtomicBool>,
        spawn_count: Arc<AtomicU32>,
        hang_on_write: bool,
    }

    impl MockProcessFactory {
        fn new() -> Self {
            Self {
                alive: Arc::new(AtomicBool::new(true)),
                writes: Arc::new(TokioMutex::new(Vec::new())),
                fail_after_writes: None,
                spawn_fail: Arc::new(AtomicBool::new(false)),
                spawn_count: Arc::new(AtomicU32::new(0)),
                hang_on_write: false,
            }
        }
    }

    impl OutputProcessFactory for MockProcessFactory {
        fn spawn(
            &self,
            _service_type: ServiceType,
            _stream_key: &str,
            _alias: &str,
        ) -> Result<Box<dyn OutputProcess>, String> {
            self.spawn_count.fetch_add(1, Ordering::Relaxed);
            if self.spawn_fail.load(Ordering::Relaxed) {
                return Err("mock spawn failed".to_string());
            }
            self.alive.store(true, Ordering::Relaxed);
            let mut proc = MockProcess::new(self.alive.clone(), self.writes.clone());
            proc.fail_after = self.fail_after_writes;
            proc.hang_on_write = self.hang_on_write;
            Ok(Box::new(proc))
        }
    }

    fn test_ep_cfg() -> EndpointConfig {
        EndpointConfig {
            alias: "test-ep".to_string(),
            service_type: "TEST_FILE".to_string(),
            stream_key: "test-key".to_string(),
            is_fast: false,
        }
    }

    #[tokio::test]
    async fn test_processes_sequential_chunks() {
        tokio::time::pause();
        let chunks: Vec<(i64, Vec<u8>)> = (1..=5).map(|i| (i, vec![i as u8; 100])).collect();
        let fetcher = MockFetcher::new(chunks);
        let factory = MockProcessFactory::new();
        let writes = factory.writes.clone();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Let the loop process chunks — advance time past chunk processing
        for _ in 0..10 {
            tokio::time::advance(std::time::Duration::from_millis(200)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert_eq!(s.chunks_processed, 5, "Should have processed 5 chunks");
        assert_eq!(s.current_chunk_id, 5);
        assert_eq!(s.bytes_processed_total, 500);
        drop(s);

        let w = writes.lock().await;
        assert_eq!(w.len(), 5);
        drop(w);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_stops_on_signal() {
        tokio::time::pause();
        let fetcher = MockFetcher::new(vec![]);
        let factory = MockProcessFactory::new();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Advance a bit, then stop
        tokio::time::advance(std::time::Duration::from_millis(500)).await;
        tokio::task::yield_now().await;
        let _ = stop_tx.send(true);

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), handle).await;
        assert!(result.is_ok(), "Task should have stopped cleanly");
    }

    #[tokio::test]
    async fn test_restarts_ffmpeg_on_death() {
        tokio::time::pause();
        // Chunks 1-6, ffmpeg dies after 3 writes
        let chunks: Vec<(i64, Vec<u8>)> = (1..=6).map(|i| (i, vec![i as u8; 50])).collect();
        let fetcher = MockFetcher::new(chunks);
        let mut factory = MockProcessFactory::new();
        factory.fail_after_writes = Some(3);
        let spawn_count = factory.spawn_count.clone();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Process time for chunks + restart delays
        for _ in 0..40 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert!(
            s.ffmpeg_restart_count >= 1,
            "Should have restarted at least once, got {}",
            s.ffmpeg_restart_count
        );
        assert!(
            s.chunks_processed >= 3,
            "Should have processed at least 3 chunks before death, got {}",
            s.chunks_processed
        );
        drop(s);

        assert!(
            spawn_count.load(Ordering::Relaxed) >= 2,
            "Factory should have been called at least twice"
        );

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_tracks_ffmpeg_restart_count() {
        tokio::time::pause();
        // Chunks available but ffmpeg keeps dying after 1 write
        let chunks: Vec<(i64, Vec<u8>)> = (1..=20).map(|i| (i, vec![i as u8; 10])).collect();
        let fetcher = MockFetcher::new(chunks);
        let mut factory = MockProcessFactory::new();
        factory.fail_after_writes = Some(1);

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        for _ in 0..30 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert!(
            s.ffmpeg_restart_count >= 2,
            "Should track multiple restarts, got {}",
            s.ffmpeg_restart_count
        );
        drop(s);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_tracks_consecutive_chunk_misses() {
        tokio::time::pause();
        // Only chunk 1 available, then nothing
        let fetcher = MockFetcher::new(vec![(1, vec![1; 10])]);
        let factory = MockProcessFactory::new();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Process chunk 1, then accumulate misses for chunk 2
        for _ in 0..20 {
            tokio::time::advance(std::time::Duration::from_secs(2)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert_eq!(s.chunks_processed, 1, "Should have processed chunk 1");
        assert!(
            s.consecutive_chunk_misses > 0,
            "Should have tracked chunk misses, got {}",
            s.consecutive_chunk_misses
        );
        drop(s);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_tracks_last_error() {
        tokio::time::pause();
        // Chunk 1 available, ffmpeg dies on write of chunk 2
        let chunks: Vec<(i64, Vec<u8>)> = (1..=2).map(|i| (i, vec![i as u8; 10])).collect();
        let fetcher = MockFetcher::new(chunks);
        let mut factory = MockProcessFactory::new();
        factory.fail_after_writes = Some(1); // Die after first write

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        for _ in 0..15 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert!(s.last_error.is_some(), "Should have recorded last error");
        drop(s);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_ffmpeg_circuit_breaker_triggers() {
        tokio::time::pause();
        let chunks: Vec<(i64, Vec<u8>)> = (1..=5).map(|i| (i, vec![i as u8; 10])).collect();
        let fetcher = MockFetcher::new(chunks);
        let mut factory = MockProcessFactory::new();
        factory.spawn_fail.store(true, Ordering::Relaxed);
        let spawn_count = factory.spawn_count.clone();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Let it attempt MAX_FFMPEG_RESTARTS spawns (5s each) then enter cooldown
        for _ in 0..60 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert_eq!(
            s.stall_reason.as_deref(),
            Some("ffmpeg_crash_loop"),
            "Should have set ffmpeg_crash_loop stall reason"
        );
        drop(s);

        assert!(
            spawn_count.load(Ordering::Relaxed) >= MAX_FFMPEG_RESTARTS,
            "Should have attempted at least {} spawns, got {}",
            MAX_FFMPEG_RESTARTS,
            spawn_count.load(Ordering::Relaxed)
        );

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_chunk_gap_skip_ahead() {
        tokio::time::pause();
        // Chunks 1-15, skip 16, have 17-20
        let mut chunks: Vec<(i64, Vec<u8>)> = (1..=15).map(|i| (i, vec![i as u8; 10])).collect();
        chunks.extend((17..=20).map(|i| (i, vec![i as u8; 10])));
        let fetcher = MockFetcher::new(chunks);
        let factory = MockProcessFactory::new();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Process chunks 1-15 fast, then wait for miss timeout (~120s at 2s each)
        // Plus the skip-ahead probe
        for _ in 0..200 {
            tokio::time::advance(std::time::Duration::from_secs(2)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert!(
            s.chunks_processed >= 18,
            "Should have processed 15 + at least 3 after skip, got {}",
            s.chunks_processed
        );
        assert!(
            s.current_chunk_id >= 17,
            "Should have skipped to at least chunk 17, got {}",
            s.current_chunk_id
        );
        drop(s);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_chunk_gap_detected_when_no_skip_found() {
        tokio::time::pause();
        // Only chunks 1-15, nothing after
        let chunks: Vec<(i64, Vec<u8>)> = (1..=15).map(|i| (i, vec![i as u8; 10])).collect();
        let fetcher = MockFetcher::new(chunks);
        let factory = MockProcessFactory::new();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Process 15 chunks then wait for MAX_CHUNK_MISS_COUNT misses
        for _ in 0..200 {
            tokio::time::advance(std::time::Duration::from_secs(2)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert_eq!(s.chunks_processed, 15, "Should have processed 15 chunks");
        assert_eq!(
            s.stall_reason.as_deref(),
            Some("chunk_gap"),
            "Should have set chunk_gap stall reason"
        );
        drop(s);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_write_timeout_kills_ffmpeg() {
        tokio::time::pause();
        let chunks: Vec<(i64, Vec<u8>)> = (1..=2).map(|i| (i, vec![i as u8; 10])).collect();
        let fetcher = MockFetcher::new(chunks);
        let mut factory = MockProcessFactory::new();
        factory.hang_on_write = true;
        let spawn_count = factory.spawn_count.clone();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Advance past the write timeout
        for _ in 0..40 {
            tokio::time::advance(std::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert_eq!(
            s.last_error.as_deref(),
            Some("write_timeout"),
            "Should have write_timeout as last error"
        );
        drop(s);

        assert!(
            spawn_count.load(Ordering::Relaxed) >= 2,
            "Should have respawned ffmpeg after timeout"
        );

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_processes_100_sequential_chunks() {
        tokio::time::pause();
        let chunks: Vec<(i64, Vec<u8>)> = (1..=100).map(|i| (i, vec![i as u8; 100])).collect();
        let fetcher = MockFetcher::new(chunks);
        let factory = MockProcessFactory::new();
        let writes = factory.writes.clone();

        let (stop_tx, stop_rx) = watch::channel(false);
        let stats: Stats = Arc::new(Mutex::new(EndpointStats::default()));

        let stats_clone = stats.clone();
        let handle = tokio::spawn(async move {
            endpoint_loop(fetcher, factory, test_ep_cfg(), 1, 0, stop_rx, stats_clone).await;
        });

        // Advance time to process all 100 chunks (100ms per chunk + overhead)
        for _ in 0..150 {
            tokio::time::advance(std::time::Duration::from_millis(200)).await;
            tokio::task::yield_now().await;
        }

        let s = stats.lock().await;
        assert_eq!(s.chunks_processed, 100, "Must process all 100 chunks");
        assert_eq!(s.current_chunk_id, 100);
        assert_eq!(s.bytes_processed_total, 10000);
        assert!(s.stall_reason.is_none(), "No stall: {:?}", s.stall_reason);
        drop(s);

        let w = writes.lock().await;
        assert_eq!(w.len(), 100);
        drop(w);

        let _ = stop_tx.send(true);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), handle).await;
    }

    #[tokio::test]
    async fn test_stats_struct_serializes() {
        let stats = EndpointStats {
            bytes_processed_total: 1000,
            current_chunk_id: 42,
            chunks_processed: 10,
            ffmpeg_restart_count: 2,
            consecutive_ffmpeg_failures: 0,
            consecutive_chunk_misses: 5,
            last_error: Some("test error".to_string()),
            stall_reason: Some("chunk_gap".to_string()),
            ffmpeg_last_stderr: Some("connection refused".to_string()),
        };
        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"stall_reason\":\"chunk_gap\""));
        assert!(json.contains("\"ffmpeg_restart_count\":2"));
    }
}
