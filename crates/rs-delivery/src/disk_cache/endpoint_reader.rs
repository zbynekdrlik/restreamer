//! EndpointReader -- replaces consumer_task hot loop. Reads chunks from
//! local disk and pushes via the configured RTMP backend. No S3 calls in
//! the hot path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use super::EndpointPrefetchQueue;
use super::position_registry::EndpointPositionRegistry;
use super::registry::{ChunkAvailability, ChunkRegistry};

#[derive(Debug, thiserror::Error)]
pub enum ReaderError {
    #[error("read stall: chunk {chunk_id} did not arrive within {timeout_secs}s")]
    StallTimeout { chunk_id: i64, timeout_secs: u64 },
    #[error("push failed: {0}")]
    PushFailed(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

#[async_trait::async_trait]
pub trait ReaderPusher: Send {
    async fn push_chunk(&mut self, bytes: Vec<u8>) -> Result<(), String>;
}

#[derive(Clone)]
pub struct ReaderConfig {
    pub cache_dir: PathBuf,
    pub alias: String,
    pub start_chunk_id: i64,
    pub stall_timeout_secs: u64,
    /// Test-only: stop after pushing this many chunks. `None` = unlimited.
    pub max_chunks: Option<u64>,
    /// When `Some`, `EndpointReader::run_once` pops pre-fetched chunks from
    /// this queue instead of polling the registry. The queue is driven by a
    /// background `PrefetchReader`; lifecycle sampling (stages E/F) is done
    /// on the pusher side (LifecycleAwarePusher in endpoint_task.rs).
    /// `None` = registry-poll path (existing behaviour, used by tests and
    /// non-fast endpoints without explicit prefetch configuration).
    pub queue: Option<EndpointPrefetchQueue>,
}

pub struct EndpointReader;

impl EndpointReader {
    /// Drive the reader loop. In production `max_chunks` is `None` and the
    /// loop runs until cancelled by an external stop signal (handled by
    /// the caller via tokio::select with the watch channel).
    pub async fn run_once(
        cfg: ReaderConfig,
        registry: Arc<ChunkRegistry>,
        positions: Arc<EndpointPositionRegistry>,
        mut pusher: Box<dyn ReaderPusher>,
    ) -> Result<(), ReaderError> {
        let mut chunk_id = cfg.start_chunk_id;
        let mut pushed = 0u64;

        // Queue-driven path: pops pre-fetched chunks from a PrefetchQueue
        // instead of polling the registry. The PrefetchReader background task
        // performs S3 fetch + infinite retry; this loop is pure FIFO drain.
        // Lifecycle stamping (stages E/F) and LifecycleSampler observation
        // happen in the pusher wrapper (endpoint_task.rs::LifecycleAwarePusher).
        if let Some(ref q) = cfg.queue {
            let q = Arc::clone(q);
            loop {
                if let Some(cap) = cfg.max_chunks {
                    if pushed >= cap {
                        return Ok(());
                    }
                }
                let arc_bytes = q
                    .pop_front()
                    .await
                    .map_err(|_| ReaderError::PushFailed("queue closed".into()))?;
                let bytes = (*arc_bytes).clone();
                pusher
                    .push_chunk(bytes)
                    .await
                    .map_err(ReaderError::PushFailed)?;
                positions.advance(&cfg.alias, chunk_id);
                chunk_id += 1;
                pushed += 1;
            }
        }

        // Registry-poll path (existing behaviour): used by non-queue endpoints
        // and all tests that pass `queue: None`.
        loop {
            if let Some(cap) = cfg.max_chunks {
                if pushed >= cap {
                    return Ok(());
                }
            }
            let state = registry
                .wait_for_chunk_with_timeout(chunk_id, Duration::from_secs(cfg.stall_timeout_secs))
                .await
                .map_err(|_| ReaderError::StallTimeout {
                    chunk_id,
                    timeout_secs: cfg.stall_timeout_secs,
                })?;
            match state {
                ChunkAvailability::Available { .. } => {
                    let path = cfg.cache_dir.join(format!("{chunk_id}.bin"));
                    let bytes = tokio::fs::read(&path).await?;
                    pusher
                        .push_chunk(bytes)
                        .await
                        .map_err(ReaderError::PushFailed)?;
                    positions.advance(&cfg.alias, chunk_id);
                    chunk_id += 1;
                    pushed += 1;
                }
                ChunkAvailability::NotFound => {
                    // Skip ahead. EndpointAudit logs the gap; this keeps
                    // the stream advancing past genuine 404s instead of
                    // stalling forever.
                    chunk_id += 1;
                }
                ChunkAvailability::Evicted => {
                    // Treat like NotFound: skip and advance. The
                    // operator's EvictionTask only deletes chunks
                    // outside any endpoint window, so a reader hitting
                    // Evicted means it fell behind its own window --
                    // recovering by skipping is preferable to blocking.
                    chunk_id += 1;
                }
                ChunkAvailability::InFlight => {
                    // wait_for_chunk only returns terminal states; reaching
                    // InFlight here means the timeout elapsed without
                    // resolution. Surface as stall to the caller.
                    return Err(ReaderError::StallTimeout {
                        chunk_id,
                        timeout_secs: cfg.stall_timeout_secs,
                    });
                }
            }
        }
    }
}

/// Used by the unit tests below; the production emission lives in
/// endpoint_task::emit_disk_cache_push_sample (via disk_cache_push_sample.rs).
#[allow(clippy::too_many_arguments)]
fn build_push_sample_payload(
    endpoint: &str,
    chunk_id: u64,
    chunk_supply_lag_ms: i64,
    inter_chunk_gap_ms: u64,
    chunk_duration_ms: u64,
    delivery_delay_secs: u64,
    current_chunk_delay_secs: f64,
) -> serde_json::Value {
    let burst_factor = if inter_chunk_gap_ms == 0 {
        0.0
    } else {
        chunk_duration_ms as f64 / inter_chunk_gap_ms as f64
    };
    serde_json::json!({
        "endpoint": endpoint,
        "chunk_id": chunk_id,
        "chunk_supply_lag_ms": chunk_supply_lag_ms,
        "inter_chunk_gap_ms": inter_chunk_gap_ms,
        "burst_factor": burst_factor,
        "delivery_delay_secs": delivery_delay_secs,
        "current_chunk_delay_secs": current_chunk_delay_secs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::position_registry::EndpointPositionRegistry;
    use crate::disk_cache::registry::ChunkRegistry;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Mock pusher that counts pushed chunks.
    struct MockPusher {
        pushed: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl ReaderPusher for MockPusher {
        async fn push_chunk(&mut self, _bytes: Vec<u8>) -> Result<(), String> {
            self.pushed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn reader_pushes_chunks_in_order_after_marking_available() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        std::fs::write(event_dir.join("0.bin"), b"AAAA").unwrap();
        std::fs::write(event_dir.join("1.bin"), b"BBBB").unwrap();
        let registry = ChunkRegistry::new();
        registry.mark_available(0, 4);
        registry.mark_available(1, 4);
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10);
        let pushed = Arc::new(AtomicU32::new(0));
        let pusher = Box::new(MockPusher {
            pushed: pushed.clone(),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 5,
            max_chunks: Some(2),
            queue: None,
        };
        EndpointReader::run_once(cfg, registry, positions, pusher)
            .await
            .unwrap();
        assert_eq!(pushed.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn reader_advances_position_registry_after_each_push() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        std::fs::write(event_dir.join("0.bin"), b"x").unwrap();
        let registry = ChunkRegistry::new();
        registry.mark_available(0, 1);
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10);
        let pushed = Arc::new(AtomicU32::new(0));
        let pusher = Box::new(MockPusher {
            pushed: pushed.clone(),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 5,
            max_chunks: Some(1),
            queue: None,
        };
        EndpointReader::run_once(cfg, registry, positions.clone(), pusher)
            .await
            .unwrap();
        let snap = positions.snapshot();
        assert_eq!(snap[0].current_chunk_id, 0);
    }

    #[tokio::test]
    async fn reader_returns_stall_timeout_when_chunk_never_arrives() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        let registry = ChunkRegistry::new();
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10);
        let pusher = Box::new(MockPusher {
            pushed: Arc::new(AtomicU32::new(0)),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 1,
            max_chunks: Some(1),
            queue: None,
        };
        let result = EndpointReader::run_once(cfg, registry, positions, pusher).await;
        assert!(matches!(result, Err(ReaderError::StallTimeout { .. })));
    }

    #[test]
    fn push_sample_payload_math() {
        let payload = build_push_sample_payload(
            "FB-NewLevel",
            100,
            /* chunk_supply_lag_ms = */ 320,
            /* inter_chunk_gap_ms = */ 850,
            /* chunk_duration_ms = */ 1000,
            /* delivery_delay_secs = */ 120,
            /* current_chunk_delay_secs = */ 151.3,
        );
        assert_eq!(payload["endpoint"], "FB-NewLevel");
        assert_eq!(payload["chunk_id"], 100);
        assert_eq!(payload["chunk_supply_lag_ms"], 320);
        assert_eq!(payload["inter_chunk_gap_ms"], 850);
        let burst = payload["burst_factor"].as_f64().unwrap();
        assert!((burst - (1000.0 / 850.0)).abs() < 1e-6);
        assert_eq!(payload["delivery_delay_secs"], 120);
        let cd = payload["current_chunk_delay_secs"].as_f64().unwrap();
        assert!((cd - 151.3).abs() < 1e-6);
    }

    #[test]
    fn push_sample_burst_factor_is_zero_when_gap_is_zero() {
        // Edge case: first push, no previous chunk -> inter_chunk_gap_ms = 0.
        // Avoid div-by-zero; report burst_factor = 0.0 and let the consumer
        // treat it as "no signal yet".
        let payload = build_push_sample_payload("YT NLCH 4K", 1, 0, 0, 1000, 120, 0.0);
        assert_eq!(payload["burst_factor"].as_f64().unwrap(), 0.0);
    }
}
