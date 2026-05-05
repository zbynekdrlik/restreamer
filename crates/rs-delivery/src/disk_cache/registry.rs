//! ChunkRegistry -- in-memory chunk-availability tracker with async wake.
//!
//! Owns the source of truth for "is chunk N on disk and ready to read?".
//! `DownloadService` calls `mark_available` after the file rename;
//! `EndpointReader` calls `wait_for_chunk` to block until ready.

use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum ChunkAvailability {
    Available { size_bytes: u64 },
    NotFound,
    InFlight,
    Evicted,
}

pub struct ChunkRegistry {
    // implemented in Task 4
    _placeholder: (),
}

impl ChunkRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn wait_for_chunk_returns_immediately_when_already_available() {
        let r = ChunkRegistry::new();
        r.mark_available(42, 1024);
        let got = tokio::time::timeout(Duration::from_millis(50), r.wait_for_chunk(42))
            .await
            .expect("should not block");
        assert!(matches!(
            got,
            Ok(ChunkAvailability::Available { size_bytes: 1024 })
        ));
    }

    #[tokio::test]
    async fn wait_for_chunk_blocks_until_mark_available() {
        let r = ChunkRegistry::new();
        let r2 = r.clone();
        let waiter = tokio::spawn(async move { r2.wait_for_chunk(7).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "must block until mark_available");
        r.mark_available(7, 2048);
        let got = tokio::time::timeout(Duration::from_millis(100), waiter)
            .await
            .expect("must wake")
            .expect("task panicked");
        assert!(matches!(
            got,
            Ok(ChunkAvailability::Available { size_bytes: 2048 })
        ));
    }

    #[tokio::test]
    async fn wait_for_chunk_returns_not_found_when_marked() {
        let r = ChunkRegistry::new();
        r.mark_not_found(99);
        let got = r.wait_for_chunk(99).await;
        assert!(matches!(got, Ok(ChunkAvailability::NotFound)));
    }

    #[tokio::test]
    async fn wait_for_chunk_returns_evicted_after_eviction() {
        let r = ChunkRegistry::new();
        r.mark_available(5, 1000);
        r.mark_evicted(5);
        let got = r.wait_for_chunk(5).await;
        assert!(matches!(got, Ok(ChunkAvailability::Evicted)));
    }

    #[tokio::test]
    async fn wait_for_chunk_times_out_after_configured_duration() {
        let r = ChunkRegistry::new();
        let result = r
            .wait_for_chunk_with_timeout(123, Duration::from_millis(50))
            .await;
        assert!(result.is_err(), "expected timeout error");
    }

    #[tokio::test]
    async fn concurrent_waiters_all_wake_on_single_mark_available() {
        let r = ChunkRegistry::new();
        let mut handles = Vec::new();
        for _ in 0..6 {
            let r2 = r.clone();
            handles.push(tokio::spawn(async move { r2.wait_for_chunk(11).await }));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        r.mark_available(11, 512);
        for h in handles {
            let got = tokio::time::timeout(Duration::from_millis(100), h)
                .await
                .expect("waiter must wake")
                .expect("task panicked");
            assert!(matches!(
                got,
                Ok(ChunkAvailability::Available { size_bytes: 512 })
            ));
        }
    }

    #[test]
    fn exists_returns_false_for_unknown_chunk() {
        let r = ChunkRegistry::new();
        assert!(!r.exists(404));
    }

    #[test]
    fn exists_returns_true_after_mark_available() {
        let r = ChunkRegistry::new();
        r.mark_available(7, 100);
        assert!(r.exists(7));
    }
}
