//! EndpointReader -- replaces consumer_task hot loop. Reads chunks from
//! local disk and pushes via the configured RTMP backend. No S3 calls in
//! the hot path.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

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

#[derive(Debug, Clone)]
pub struct ReaderConfig {
    pub cache_dir: PathBuf,
    pub alias: String,
    pub start_chunk_id: i64,
    pub stall_timeout_secs: u64,
    /// Test-only: stop after pushing this many chunks. `None` = unlimited.
    pub max_chunks: Option<u64>,
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
                    positions.advance(&cfg.alias, chunk_id).await;
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
        positions.register("a".into(), 10).await;
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
        positions.register("a".into(), 10).await;
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
        };
        EndpointReader::run_once(cfg, registry, positions.clone(), pusher)
            .await
            .unwrap();
        let snap = positions.snapshot().await;
        assert_eq!(snap[0].current_chunk_id, 0);
    }

    #[tokio::test]
    async fn reader_returns_stall_timeout_when_chunk_never_arrives() {
        let tmp = tempfile::tempdir().unwrap();
        let event_dir = tmp.path().join("evt");
        std::fs::create_dir_all(&event_dir).unwrap();
        let registry = ChunkRegistry::new();
        let positions = EndpointPositionRegistry::new();
        positions.register("a".into(), 10).await;
        let pusher = Box::new(MockPusher {
            pushed: Arc::new(AtomicU32::new(0)),
        });
        let cfg = ReaderConfig {
            cache_dir: event_dir,
            alias: "a".into(),
            start_chunk_id: 0,
            stall_timeout_secs: 1,
            max_chunks: Some(1),
        };
        let result = EndpointReader::run_once(cfg, registry, positions, pusher).await;
        assert!(matches!(result, Err(ReaderError::StallTimeout { .. })));
    }
}
