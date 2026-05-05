//! EndpointReader -- replaces the consumer_task hot loop. Reads chunks
//! from local disk and pushes via RtmpPusher. No S3 calls in hot path.

pub struct EndpointReader {
    _placeholder: (),
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
