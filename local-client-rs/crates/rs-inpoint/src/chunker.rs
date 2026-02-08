use md5::{Digest, Md5};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use tracing::info;

use crate::muxer::TsMuxer;

/// Receives raw media data and produces time-based MPEG-TS chunk files.
///
/// Chunks are accumulated for `chunk_duration` and then flushed to disk
/// as `.bin` files with MD5 checksums.
pub struct ChunkSink {
    inner: Mutex<ChunkSinkInner>,
    chunk_tx: broadcast::Sender<ChunkInfo>,
}

struct ChunkSinkInner {
    muxer: TsMuxer,
    buffer: Vec<u8>,
    chunk_dir: PathBuf,
    chunk_duration: Duration,
    chunk_start: Option<Instant>,
    chunk_index: u64,
    /// When true, discard all data (null sink for testing).
    null_mode: bool,
}

/// Information about a completed chunk.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub path: PathBuf,
    pub size: usize,
    pub md5: String,
    pub index: u64,
}

impl ChunkSink {
    pub fn new(chunk_dir: PathBuf, chunk_duration: Duration) -> Self {
        let (chunk_tx, _) = broadcast::channel(256);
        Self {
            inner: Mutex::new(ChunkSinkInner {
                muxer: TsMuxer::new(),
                buffer: Vec::with_capacity(128 * 1024),
                chunk_dir,
                chunk_duration,
                chunk_start: None,
                chunk_index: 0,
                null_mode: false,
            }),
            chunk_tx,
        }
    }

    /// Create a null sink that discards all data (for testing).
    pub fn new_null() -> Self {
        let (chunk_tx, _) = broadcast::channel(1);
        Self {
            inner: Mutex::new(ChunkSinkInner {
                muxer: TsMuxer::new(),
                buffer: Vec::new(),
                chunk_dir: PathBuf::new(),
                chunk_duration: Duration::from_secs(1),
                chunk_start: None,
                chunk_index: 0,
                null_mode: true,
            }),
            chunk_tx,
        }
    }

    /// Subscribe to chunk completion events.
    pub fn subscribe(&self) -> broadcast::Receiver<ChunkInfo> {
        self.chunk_tx.subscribe()
    }

    /// Write raw media data into the chunker.
    pub async fn write_data(&self, data: &[u8]) {
        let mut inner = self.inner.lock().await;
        if inner.null_mode {
            return;
        }

        // Start chunk timer on first data
        if inner.chunk_start.is_none() {
            inner.chunk_start = Some(Instant::now());
        }

        // Mux data into TS packets
        let ts_data = inner.muxer.write(data);
        inner.buffer.extend_from_slice(&ts_data);

        // Check if chunk duration has elapsed
        if let Some(start) = inner.chunk_start {
            if start.elapsed() >= inner.chunk_duration {
                // Flush muxer and finalize chunk
                let remainder = inner.muxer.flush();
                inner.buffer.extend_from_slice(&remainder);

                if let Some(chunk_info) = self.finalize_chunk(&mut inner) {
                    let _ = self.chunk_tx.send(chunk_info);
                }
            }
        }
    }

    /// Force flush any buffered data as a final chunk.
    pub async fn flush(&self) {
        let mut inner = self.inner.lock().await;
        if inner.null_mode || inner.buffer.is_empty() {
            return;
        }

        let remainder = inner.muxer.flush();
        inner.buffer.extend_from_slice(&remainder);

        if let Some(chunk_info) = self.finalize_chunk(&mut inner) {
            let _ = self.chunk_tx.send(chunk_info);
        }
    }

    fn finalize_chunk(&self, inner: &mut ChunkSinkInner) -> Option<ChunkInfo> {
        if inner.buffer.is_empty() {
            return None;
        }

        let index = inner.chunk_index;
        inner.chunk_index += 1;

        // Compute MD5
        let mut hasher = Md5::new();
        hasher.update(&inner.buffer);
        let md5 = format!("{:x}", hasher.finalize());

        // Build file path
        let filename = format!("chunk_{index:06}.bin");
        let path = inner.chunk_dir.join(&filename);

        let size = inner.buffer.len();

        // Write to disk (best effort — caller handles DB insert)
        if let Err(e) = std::fs::create_dir_all(&inner.chunk_dir) {
            tracing::error!("Failed to create chunk dir: {e}");
            inner.buffer.clear();
            inner.chunk_start = None;
            return None;
        }
        if let Err(e) = std::fs::write(&path, &inner.buffer) {
            tracing::error!("Failed to write chunk file: {e}");
            inner.buffer.clear();
            inner.chunk_start = None;
            return None;
        }

        info!("Chunk {index} written: {size} bytes, md5={md5}");

        inner.buffer.clear();
        inner.chunk_start = None;
        inner.muxer.reset();

        Some(ChunkInfo {
            path,
            size,
            md5,
            index,
        })
    }

    /// Reset the chunker state.
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.buffer.clear();
        inner.chunk_start = None;
        inner.muxer.reset();
    }

    /// Get the total number of chunks produced.
    pub async fn chunk_count(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.chunk_index
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn null_sink_discards_data() {
        let sink = ChunkSink::new_null();
        sink.write_data(&[0u8; 1024]).await;
        assert_eq!(sink.chunk_count().await, 0);
    }

    #[tokio::test]
    async fn produces_chunk_after_duration() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(ChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_millis(50),
        ));
        let mut rx = sink.subscribe();

        // Write data and wait for chunk duration
        sink.write_data(&vec![0xAA; 1000]).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        sink.write_data(&vec![0xBB; 100]).await;

        let chunk = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(chunk.path.exists());
        assert!(chunk.size > 0);
        assert!(!chunk.md5.is_empty());
        assert_eq!(chunk.index, 0);
    }

    #[tokio::test]
    async fn flush_produces_final_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(ChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_secs(60), // long duration so it won't auto-flush
        ));
        let mut rx = sink.subscribe();

        sink.write_data(&vec![0xCC; 500]).await;
        sink.flush().await;

        let chunk = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(chunk.path.exists());
        assert!(chunk.size > 0);
    }

    #[tokio::test]
    async fn md5_is_consistent() {
        let dir = tempfile::tempdir().unwrap();
        let sink = ChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));
        let mut rx = sink.subscribe();

        let data = vec![0xDD; 500];
        sink.write_data(&data).await;
        sink.flush().await;

        let chunk = rx.recv().await.unwrap();

        // Read file and verify MD5 matches
        let file_data = std::fs::read(&chunk.path).unwrap();
        let mut hasher = Md5::new();
        hasher.update(&file_data);
        let expected_md5 = format!("{:x}", hasher.finalize());
        assert_eq!(chunk.md5, expected_md5);
    }

    #[tokio::test]
    async fn reset_clears_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let sink = ChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        sink.write_data(&vec![0xEE; 500]).await;
        sink.reset().await;
        sink.flush().await;

        // No chunk should be produced after reset + flush
        assert_eq!(sink.chunk_count().await, 0);
    }
}
