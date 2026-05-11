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

#[tokio::test(flavor = "current_thread")]
async fn retries_forever_on_503_then_eventually_succeeds() {
    // Real-time test (not paused). FlakyBackend fails 5 times then
    // succeeds. The inner fetch_with_retry backoff is 1s,2s,4s,8s,16s,
    // so 5 fails = 31s real time before the 6th attempt succeeds.
    //
    // fail_until=5 is the critical regression boundary: the OLD
    // download_service.rs::fetch_with_retry capped at 5 attempts and
    // would mark NotFound after the 5th fail; the new retry-forever
    // loop keeps going to attempt 6 and delivers.
    //
    // start_paused was rejected for these tests because tarpaulin's
    // coverage instrumentation is slow enough that paused-time tests
    // with many virtual sleep wake-ups exhaust tarpaulin's 5-min
    // per-test timeout. Real time + small fail counts is the robust
    // pattern.
    let backend = Arc::new(FlakyBackend {
        fail_count: AtomicU32::new(0),
        fail_until: 5,
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
    // Wait up to 60s real time for the chunk to arrive (5 backoffs
    // total ≈ 31s plus runtime overhead).
    let got = tokio::time::timeout(Duration::from_secs(60), queue.pop_front())
        .await
        .expect("reader did not deliver chunk after 5 retries")
        .expect("queue not closed");
    assert!(!got.is_empty());
    assert!(
        backend.fail_count.load(Ordering::SeqCst) >= 5,
        "expected >=5 attempts (proving past the old 5-attempt cap), got {}",
        backend.fail_count.load(Ordering::SeqCst)
    );
    queue.close();
    let _ = task.await;
}

#[tokio::test(flavor = "current_thread")]
async fn retries_continue_indefinitely_no_max_attempts_cap() {
    // Distinct from `retries_forever_on_503_then_eventually_succeeds`:
    // backend fails forever; assert the reader keeps trying past the
    // old 5-attempt cap. Real-time test (not paused) so tarpaulin's
    // 5-min instrumentation timeout doesn't trip on a long simulated
    // backoff schedule. 250ms of real time is enough for >=5 attempts
    // even on a slow CI runner because the inner fetch_with_retry
    // backoff (1s, 2s, ...) is shorter than 5×250ms only at start —
    // `tokio::task::yield_now` between asserts gives the spawned chain
    // chances to progress.
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
    // 8 real seconds is enough for the inner fetch_with_retry to do
    // at least 4 attempts (1s+2s+4s = 7s plus the initial fetch).
    // The 5-attempt-cap regression would top out at 5 attempts THEN
    // mark NotFound; with retry-forever, attempts keep accumulating.
    tokio::time::sleep(Duration::from_secs(8)).await;
    let count = backend.0.load(Ordering::SeqCst);
    queue.close();
    let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
    assert!(
        count >= 4,
        "expected >=4 attempts in 8s real time (retry-forever), got {count}"
    );
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
