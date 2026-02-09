use md5::{Digest, Md5};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, broadcast};
use tracing::info;

/// Receives pre-muxed MPEG-TS data and produces time-based chunk files.
///
/// Chunks are accumulated for `chunk_duration` and then flushed to disk
/// as `.bin` files with MD5 checksums. The MPEG-TS muxing is done upstream
/// by the media receiver; this module only handles buffering and file I/O.
pub struct ChunkSink {
    inner: Mutex<ChunkSinkInner>,
    chunk_tx: broadcast::Sender<ChunkInfo>,
}

struct ChunkSinkInner {
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

/// Data extracted from the buffer, ready to be written to disk outside the lock.
struct PendingChunkWrite {
    data: Vec<u8>,
    path: PathBuf,
    size: usize,
    md5: String,
    index: u64,
}

impl ChunkSink {
    pub fn new(chunk_dir: PathBuf, chunk_duration: Duration) -> Self {
        let (chunk_tx, _) = broadcast::channel(256);
        Self {
            inner: Mutex::new(ChunkSinkInner {
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

    /// Write pre-muxed MPEG-TS data into the chunker.
    pub async fn write_data(&self, data: &[u8]) {
        let pending = {
            let mut inner = self.inner.lock().await;
            if inner.null_mode {
                return;
            }

            // Start chunk timer on first data
            if inner.chunk_start.is_none() {
                inner.chunk_start = Some(Instant::now());
            }

            // Buffer the pre-muxed MPEG-TS data directly
            inner.buffer.extend_from_slice(data);

            // Check if chunk duration has elapsed
            if let Some(start) = inner.chunk_start {
                if start.elapsed() >= inner.chunk_duration {
                    Self::extract_chunk(&mut inner)
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(pending) = pending {
            if let Some(chunk_info) = Self::write_chunk_to_disk(pending).await {
                if let Err(e) = self.chunk_tx.send(chunk_info) {
                    tracing::debug!("No chunk subscribers: {e}");
                }
            }
        }
    }

    /// Force flush any buffered data as a final chunk.
    pub async fn flush(&self) {
        let pending = {
            let mut inner = self.inner.lock().await;
            if inner.null_mode || inner.buffer.is_empty() {
                None
            } else {
                Self::extract_chunk(&mut inner)
            }
        };

        if let Some(pending) = pending {
            if let Some(chunk_info) = Self::write_chunk_to_disk(pending).await {
                if let Err(e) = self.chunk_tx.send(chunk_info) {
                    tracing::debug!("No chunk subscribers: {e}");
                }
            }
        }
    }

    /// Extract chunk data from the buffer without performing I/O.
    fn extract_chunk(inner: &mut ChunkSinkInner) -> Option<PendingChunkWrite> {
        if inner.buffer.is_empty() {
            return None;
        }

        let index = inner.chunk_index;
        inner.chunk_index += 1;

        // Compute MD5
        let mut hasher = Md5::new();
        hasher.update(&inner.buffer);
        let md5 = format!("{:x}", hasher.finalize());

        // Build file path with timestamp to avoid collisions across restarts
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let filename = format!("chunk_{timestamp}_{index:06}.bin");
        let path = inner.chunk_dir.join(&filename);

        let size = inner.buffer.len();
        let data = std::mem::replace(&mut inner.buffer, Vec::with_capacity(128 * 1024));

        inner.chunk_start = None;

        Some(PendingChunkWrite {
            data,
            path,
            size,
            md5,
            index,
        })
    }

    /// Write chunk data to disk (async I/O, no lock held).
    async fn write_chunk_to_disk(pending: PendingChunkWrite) -> Option<ChunkInfo> {
        if let Some(parent) = pending.path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::error!("Failed to create chunk dir: {e}");
                return None;
            }
        }
        if let Err(e) = tokio::fs::write(&pending.path, &pending.data).await {
            tracing::error!("Failed to write chunk file: {e}");
            return None;
        }

        info!(
            "Chunk {} written: {} bytes, md5={}",
            pending.index, pending.size, pending.md5
        );

        Some(ChunkInfo {
            path: pending.path,
            size: pending.size,
            md5: pending.md5,
            index: pending.index,
        })
    }

    /// Reset the chunker state.
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.buffer.clear();
        inner.chunk_start = None;
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

    #[tokio::test]
    async fn write_data_accepts_ts_packets() {
        let dir = tempfile::tempdir().unwrap();
        let sink = ChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));
        let mut rx = sink.subscribe();

        // Simulate pre-muxed MPEG-TS data (188-byte packets with sync byte)
        let mut ts_data = Vec::new();
        for _ in 0..10 {
            let mut packet = vec![0x47]; // TS sync byte
            packet.extend_from_slice(&vec![0x00; 187]);
            ts_data.extend_from_slice(&packet);
        }
        sink.write_data(&ts_data).await;
        sink.flush().await;

        let chunk = rx.recv().await.unwrap();
        assert_eq!(chunk.size, 1880); // 10 * 188
        assert!(chunk.path.exists());

        // Verify the file contains valid TS-like data
        let file_data = std::fs::read(&chunk.path).unwrap();
        assert_eq!(file_data[0], 0x47);
        assert_eq!(file_data[188], 0x47);
    }
}
