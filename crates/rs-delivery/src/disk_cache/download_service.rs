//! DownloadService -- bandwidth-managed S3 chunk downloader with dedup.

use std::sync::Arc;

pub struct DownloadService {
    _placeholder: (),
}

impl DownloadService {
    pub fn new() -> Arc<Self> {
        Arc::new(Self { _placeholder: () })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_cache::registry::{ChunkAvailability, ChunkRegistry};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::{Duration, Instant};

    /// Deterministic mock S3 backend. Counts GETs per chunk.
    #[derive(Default)]
    struct MockBackend {
        get_count: AtomicU32,
        result: std::sync::Mutex<Option<Result<(Vec<u8>, i64), String>>>,
    }

    impl MockBackend {
        fn set_ok(&self, data: Vec<u8>, dur: i64) {
            *self.result.lock().unwrap() = Some(Ok((data, dur)));
        }
        #[allow(dead_code)]
        fn set_err(&self, msg: &str) {
            *self.result.lock().unwrap() = Some(Err(msg.into()));
        }
        fn count(&self) -> u32 {
            self.get_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl S3Backend for MockBackend {
        async fn fetch(&self, _chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
            self.get_count.fetch_add(1, Ordering::SeqCst);
            match self.result.lock().unwrap().clone() {
                Some(Ok((d, dur))) => Ok(Some((d, dur))),
                Some(Err(e)) => Err(e),
                None => Ok(None),
            }
        }
    }

    #[tokio::test]
    async fn dedup_six_concurrent_requests_for_same_chunk_yield_one_get() {
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(vec![0u8; 1024], 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000, // 10 Gbit cap so test isn't bandwidth-limited
            8,
        );
        let mut handles = Vec::new();
        for _ in 0..6 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(42).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(backend.count(), 1, "deduplicate concurrent requests");
    }

    #[tokio::test]
    async fn fetch_writes_atomic_file_then_marks_registry_available() {
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(b"hello".to_vec(), 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        svc.request_chunk(7).await;
        let path = tmp.path().join("evt").join("7.bin");
        assert!(
            path.exists(),
            "file must exist after request_chunk completes"
        );
        assert_eq!(tokio::fs::read(&path).await.unwrap(), b"hello");
        assert!(registry.exists(7));
    }

    #[tokio::test]
    async fn fetch_404_marks_registry_not_found_no_file() {
        let backend = Arc::new(MockBackend::default());
        // Ok(None) signals 404 / not-found.
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        svc.request_chunk(404).await;
        let state = registry.wait_for_chunk(404).await.unwrap();
        assert!(matches!(state, ChunkAvailability::NotFound));
        let path = tmp.path().join("evt").join("404.bin");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn fetch_5xx_retries_with_backoff_then_succeeds() {
        // Mock that fails twice then succeeds — verify retry happens.
        let attempts = Arc::new(AtomicU32::new(0));
        struct FlakyBackend(Arc<AtomicU32>);
        #[async_trait::async_trait]
        impl S3Backend for FlakyBackend {
            async fn fetch(&self, _id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err("S3 fetch error: status 503".into())
                } else {
                    Ok(Some((vec![1, 2, 3], 2000)))
                }
            }
        }
        let backend = Arc::new(FlakyBackend(attempts.clone()));
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            10_000,
            8,
        );
        svc.request_chunk(99).await;
        let state = registry.wait_for_chunk(99).await.unwrap();
        assert!(matches!(state, ChunkAvailability::Available { .. }));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn bandwidth_cap_throttles_combined_throughput() {
        // 5 concurrent fetches x 1 MB each at 100 Mbit/s combined cap
        //   = 5 MB total / 12.5 MB/s ~= 400 ms minimum.
        // Use 1 MB body to keep math obvious.
        let backend = Arc::new(MockBackend::default());
        backend.set_ok(vec![0u8; 1_000_000], 2000);
        let tmp = tempfile::tempdir().unwrap();
        let registry = ChunkRegistry::new();
        let svc = DownloadService::new(
            backend.clone(),
            registry.clone(),
            tmp.path().to_path_buf(),
            "evt".into(),
            100, // 100 Mbit/s cap
            5,
        );
        let started = Instant::now();
        let mut handles = Vec::new();
        for id in 0..5 {
            let s = svc.clone();
            handles.push(tokio::spawn(async move { s.request_chunk(id).await }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(350),
            "bandwidth cap must throttle (got {:?})",
            elapsed
        );
    }
}
