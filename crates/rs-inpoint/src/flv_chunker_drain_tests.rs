//! #368: `wait_for_writes` lets the background chunk writes finish (and
//! report their chunks) before the runtime running them shuts down.

use super::*;

use std::time::Instant;

fn sink() -> FlvChunkSink {
    FlvChunkSink::new(PathBuf::from("/nonexistent-368"), Duration::from_secs(1))
}

#[tokio::test]
async fn nothing_pending_returns_at_once() {
    let started = Instant::now();
    assert_eq!(sink().wait_for_writes(Duration::from_secs(5)).await, 0);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "no write is pending, so there is nothing to wait for"
    );
}

#[tokio::test]
async fn waits_until_the_pending_writes_finish() {
    let sink = sink();
    sink.pending_writes.store(1, Ordering::Relaxed);
    let pending = Arc::clone(&sink.pending_writes);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        pending.fetch_sub(1, Ordering::Relaxed);
    });
    let started = Instant::now();
    assert_eq!(sink.wait_for_writes(Duration::from_secs(5)).await, 0);
    assert!(started.elapsed() >= Duration::from_millis(150));
}

#[tokio::test]
async fn gives_up_after_the_limit() {
    let sink = sink();
    sink.pending_writes.store(1, Ordering::Relaxed);
    let started = Instant::now();
    assert_eq!(sink.wait_for_writes(Duration::from_millis(100)).await, 1);
    assert!(started.elapsed() >= Duration::from_millis(100));
}

/// A wall clock frozen at one instant, so chunk file names are known.
#[cfg(unix)]
struct FixedClock;

#[cfg(unix)]
impl WallClock for FixedClock {
    fn now_ms(&self) -> i64 {
        1_000
    }
}

/// A REAL background write in flight: the first chunk's file is a FIFO, so
/// the write blocks until a reader opens it. `wait_for_writes` counts it as
/// pending until it has written AND reported the chunk.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_blocked_on_its_file_is_pending_until_it_reports() {
    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("chunk_1000_000000.bin");
    let made = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("run mkfifo");
    assert!(made.success(), "mkfifo {fifo:?}");
    let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_millis(10))
        .with_wall_clock(Arc::new(FixedClock));
    let mut reports = sink.subscribe();
    let keyframe = BytesMut::from(&[0x17, 0x01, 0, 0, 0, 0xAB][..]);

    sink.write_video(0, &keyframe).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    sink.write_video(40, &keyframe).await; // cuts chunk 0: its write blocks

    assert_eq!(
        sink.wait_for_writes(Duration::from_millis(100)).await,
        1,
        "the write blocked on the FIFO is still pending"
    );
    let reader = tokio::task::spawn_blocking(move || std::fs::read(&fifo));
    assert_eq!(sink.wait_for_writes(Duration::from_secs(10)).await, 0);
    let chunk = reports.try_recv().expect("the write reported its chunk");
    assert_eq!(chunk.index, 0);
    let written = reader.await.unwrap().expect("read the FIFO");
    assert_eq!(written.len(), chunk.size);
}
