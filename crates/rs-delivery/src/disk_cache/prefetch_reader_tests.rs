use super::download_service::{DownloadService, FetchedChunk, S3Backend};
use super::prefetch_queue::PrefetchQueue;
use super::prefetch_reader::PrefetchReader;
use super::registry::ChunkRegistry;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::Duration;
use tempfile::TempDir;

/// Backend that always fails with 503 to drive the infinite-retry path.
struct AlwaysFailing(AtomicU32);

#[async_trait::async_trait]
impl S3Backend for AlwaysFailing {
    async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err("S3 fetch error: status 503".into())
    }
}

/// Backend that fails N times then succeeds.
struct FlakyBackend {
    fail_count: AtomicU32,
    fail_until: u32,
}

#[async_trait::async_trait]
impl S3Backend for FlakyBackend {
    async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
        let n = self.fail_count.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_until {
            Err("S3 fetch error: status 503".into())
        } else {
            Ok(Some(FetchedChunk {
                data: vec![n as u8; 16],
                duration_ms: 2000,
                host_emit_ts: None,
                s3_upload_complete_ts: None,
            }))
        }
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retries_forever_on_503_then_eventually_succeeds() {
    let backend = Arc::new(FlakyBackend {
        fail_count: AtomicU32::new(0),
        fail_until: 50,
    });
    let tmp = TempDir::new().unwrap();
    let registry = ChunkRegistry::new();
    let download = DownloadService::new(
        backend.clone(),
        registry.clone(),
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    );
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next_id = Arc::new(AtomicI64::new(0));
    let queue_run = Arc::clone(&queue);
    let download_run = Arc::clone(&download);
    let next_run = Arc::clone(&next_id);
    let task = tokio::spawn(async move {
        PrefetchReader::run(queue_run, download_run, next_run, None).await;
    });
    // tokio::time::sleep with start_paused auto-advances time when the
    // runtime is idle, polling spawned tasks at each timer wake-up.
    // 50 retries × cap-60s ≈ 50min worst case; 2h budget covers it.
    let got = tokio::time::timeout(Duration::from_secs(2 * 60 * 60), queue.pop_front())
        .await
        .expect("reader did not deliver chunk after 50 retries")
        .expect("queue not closed");
    assert!(!got.is_empty());
    assert!(
        backend.fail_count.load(Ordering::SeqCst) >= 50,
        "expected >=50 attempts, got {}",
        backend.fail_count.load(Ordering::SeqCst)
    );
    queue.close();
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retries_continue_indefinitely_no_max_attempts_cap() {
    // Distinct from the above: assert that even past 100 failures, the
    // reader is still trying. Catches a regression that re-introduces
    // any attempt cap.
    let backend = Arc::new(AlwaysFailing(AtomicU32::new(0)));
    let tmp = TempDir::new().unwrap();
    let registry = ChunkRegistry::new();
    let download = DownloadService::new(
        backend.clone(),
        registry.clone(),
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    );
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next_id = Arc::new(AtomicI64::new(0));
    let queue_run = Arc::clone(&queue);
    let download_run = Arc::clone(&download);
    let next_run = Arc::clone(&next_id);
    let task = tokio::spawn(async move {
        PrefetchReader::run(queue_run, download_run, next_run, None).await;
    });
    // sleep auto-advances time AND polls the spawned reader chain at
    // each timer wake-up (advance alone does not). Drive 3 simulated
    // hours to prove the retry loop never gives up.
    tokio::time::sleep(Duration::from_secs(3 * 60 * 60)).await;
    let count = backend.0.load(Ordering::SeqCst);
    assert!(
        count >= 100,
        "expected >=100 attempts after 3 simulated hours, got {count}"
    );
    queue.close();
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn close_unblocks_reader_and_task_exits() {
    // Backend that returns 404 (Ok(None)) so request_chunk returns
    // promptly each iteration, allowing the outer loop to observe
    // queue closure between iterations. With a permanently-erroring
    // backend, request_chunk hangs indefinitely (retry-forever) and
    // close() cannot interrupt mid-fetch — that's an operator-stop
    // worst case measured in minutes, not seconds, and tested at the
    // integration-test level rather than unit level.
    struct NotFoundBackend;
    #[async_trait::async_trait]
    impl S3Backend for NotFoundBackend {
        async fn fetch(&self, _id: i64) -> Result<Option<FetchedChunk>, String> {
            Ok(None)
        }
    }

    let backend = Arc::new(NotFoundBackend);
    let tmp = TempDir::new().unwrap();
    let registry = ChunkRegistry::new();
    let download = DownloadService::new(
        backend,
        registry.clone(),
        tmp.path().to_path_buf(),
        "evt".into(),
        10_000,
        8,
    );
    let queue: Arc<PrefetchQueue<Arc<Vec<u8>>>> = PrefetchQueue::new(1);
    let next_id = Arc::new(AtomicI64::new(0));
    let queue_run = Arc::clone(&queue);
    let download_run = Arc::clone(&download);
    let next_run = Arc::clone(&next_id);
    let task = tokio::spawn(async move {
        PrefetchReader::run(queue_run, download_run, next_run, None).await;
    });
    // Let the reader cycle a few iterations.
    tokio::time::sleep(Duration::from_secs(5)).await;
    queue.close();
    // Reader must exit at the next outer-loop is_closed check.
    let join = tokio::time::timeout(Duration::from_secs(120), task).await;
    assert!(join.is_ok(), "reader task must exit after close()");
}
