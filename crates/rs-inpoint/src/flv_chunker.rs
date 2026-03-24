use bytes::BytesMut;
use md5::{Digest, Md5};
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info};

use crate::chunker::ChunkInfo;

/// Maximum buffer size before a forced flush (50 MB).
const MAX_BUFFER_SIZE: usize = 50 * 1024 * 1024;

/// FLV tag type constants.
const FLV_TAG_AUDIO: u8 = 8;
const FLV_TAG_VIDEO: u8 = 9;

/// FLV file header: "FLV" version 1, has audio+video, data offset = 9.
const FLV_HEADER: [u8; 9] = [0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];

/// Receives raw FLV tag bodies from xiu and produces FLV chunk files.
///
/// Unlike ChunkSink which receives pre-muxed MPEG-TS, this writes valid
/// FLV files directly. Each chunk starts with an FLV header and codec
/// sequence headers, making it independently playable.
pub struct FlvChunkSink {
    inner: Mutex<FlvChunkSinkInner>,
    chunk_tx: broadcast::Sender<ChunkInfo>,
}

struct FlvChunkSinkInner {
    buffer: Vec<u8>,
    chunk_dir: PathBuf,
    chunk_duration: Duration,
    chunk_start: Option<Instant>,
    chunk_index: u64,
    null_mode: bool,
    /// Saved codec sequence headers for writing at chunk start.
    video_sequence_header: Option<BytesMut>,
    audio_sequence_header: Option<BytesMut>,
    /// Base timestamp for the current chunk (subtracted from absolute timestamps).
    base_timestamp: Option<u32>,
}

/// Data extracted from the buffer, ready to be written to disk outside the lock.
struct PendingChunkWrite {
    data: Vec<u8>,
    path: PathBuf,
    size: usize,
    md5: String,
    index: u64,
}

impl FlvChunkSink {
    pub fn new(chunk_dir: PathBuf, chunk_duration: Duration) -> Self {
        let (chunk_tx, _) = broadcast::channel(256);
        Self {
            inner: Mutex::new(FlvChunkSinkInner {
                buffer: Vec::with_capacity(128 * 1024),
                chunk_dir,
                chunk_duration,
                chunk_start: None,
                chunk_index: 0,
                null_mode: false,
                video_sequence_header: None,
                audio_sequence_header: None,
                base_timestamp: None,
            }),
            chunk_tx,
        }
    }

    /// Create a null sink that discards all data (for testing).
    pub fn new_null() -> Self {
        let (chunk_tx, _) = broadcast::channel(1);
        Self {
            inner: Mutex::new(FlvChunkSinkInner {
                buffer: Vec::new(),
                chunk_dir: PathBuf::new(),
                chunk_duration: Duration::from_secs(1),
                chunk_start: None,
                chunk_index: 0,
                null_mode: true,
                video_sequence_header: None,
                audio_sequence_header: None,
                base_timestamp: None,
            }),
            chunk_tx,
        }
    }

    /// Subscribe to chunk completion events.
    pub fn subscribe(&self) -> broadcast::Receiver<ChunkInfo> {
        self.chunk_tx.subscribe()
    }

    /// Process a video frame from xiu's FrameData::Video.
    ///
    /// `data` is the FLV tag body (codec header + payload) as provided by xiu.
    /// `timestamp` is the absolute timestamp in milliseconds.
    pub async fn write_video(&self, timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && data[1] == 0x00;

        let pending = {
            let mut inner = self.inner.lock().await;
            if inner.null_mode {
                return;
            }

            if is_sequence_header {
                inner.video_sequence_header = Some(data.clone());
                debug!("FLV video sequence header saved ({} bytes)", data.len());
                return;
            }

            // Check if we need to start a new chunk (at keyframe boundary)
            let is_keyframe = !data.is_empty() && (data[0] >> 4) == 1;
            let should_flush = inner
                .chunk_start
                .map(|s| s.elapsed() >= inner.chunk_duration)
                .unwrap_or(false);

            let mut pending = None;

            if should_flush && is_keyframe {
                pending = Self::extract_chunk(&mut inner);
                // Reset base timestamp for new chunk
                inner.base_timestamp = Some(timestamp);
                Self::write_chunk_header(&mut inner, timestamp);
            } else if inner.chunk_start.is_none() {
                // First chunk — wait for a keyframe to start
                if !is_keyframe {
                    return;
                }
                inner.base_timestamp = Some(timestamp);
                Self::write_chunk_header(&mut inner, timestamp);
            }

            let relative_ts = timestamp.wrapping_sub(inner.base_timestamp.unwrap_or(0));
            Self::write_tag(&mut inner, FLV_TAG_VIDEO, relative_ts, data);

            // Force-flush if buffer exceeds max size
            if inner.buffer.len() >= MAX_BUFFER_SIZE {
                tracing::warn!(
                    "FLV chunk buffer exceeded {}MB limit, force-flushing",
                    MAX_BUFFER_SIZE / (1024 * 1024)
                );
                if pending.is_none() {
                    pending = Self::extract_chunk(&mut inner);
                }
            }

            pending
        };

        if let Some(pending) = pending {
            self.write_and_notify(pending).await;
        }
    }

    /// Process an audio frame from xiu's FrameData::Audio.
    pub async fn write_audio(&self, timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && (data[0] >> 4) == 0x0A && data[1] == 0x00;

        let pending = {
            let mut inner = self.inner.lock().await;
            if inner.null_mode {
                return;
            }

            if is_sequence_header {
                inner.audio_sequence_header = Some(data.clone());
                debug!("FLV audio sequence header saved ({} bytes)", data.len());
                return;
            }

            // Only write audio if a chunk has been started (by a video keyframe)
            if inner.chunk_start.is_none() {
                return;
            }

            let relative_ts = timestamp.wrapping_sub(inner.base_timestamp.unwrap_or(0));
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, relative_ts, data);
            None
        };

        if let Some(pending) = pending {
            self.write_and_notify(pending).await;
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
            self.write_and_notify(pending).await;
        }
    }

    /// Reset the chunker state.
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.buffer.clear();
        inner.chunk_start = None;
        inner.base_timestamp = None;
    }

    /// Get the total number of chunks produced.
    pub async fn chunk_count(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.chunk_index
    }

    /// Write FLV file header + sequence headers at the start of a new chunk.
    fn write_chunk_header(inner: &mut FlvChunkSinkInner, _timestamp: u32) {
        // FLV file header (9 bytes)
        inner.buffer.extend_from_slice(&FLV_HEADER);
        // Previous tag size 0 (4 bytes)
        inner.buffer.extend_from_slice(&[0, 0, 0, 0]);

        // Clone sequence headers to avoid borrowing inner immutably while writing
        let vsh = inner.video_sequence_header.clone();
        let ash = inner.audio_sequence_header.clone();

        if let Some(ref vsh) = vsh {
            Self::write_tag(inner, FLV_TAG_VIDEO, 0, vsh);
        }
        if let Some(ref ash) = ash {
            Self::write_tag(inner, FLV_TAG_AUDIO, 0, ash);
        }

        inner.chunk_start = Some(Instant::now());
    }

    /// Write an FLV tag (11-byte header + data + 4-byte previous tag size).
    fn write_tag(inner: &mut FlvChunkSinkInner, tag_type: u8, timestamp: u32, data: &[u8]) {
        let data_size = data.len() as u32;

        // Tag header (11 bytes)
        inner.buffer.push(tag_type);
        // DataSize (3 bytes, big-endian)
        inner.buffer.extend_from_slice(&[
            (data_size >> 16) as u8,
            (data_size >> 8) as u8,
            data_size as u8,
        ]);
        // Timestamp (3 bytes lower + 1 byte upper)
        inner.buffer.extend_from_slice(&[
            (timestamp >> 16) as u8,
            (timestamp >> 8) as u8,
            timestamp as u8,
        ]);
        inner.buffer.push((timestamp >> 24) as u8);
        // StreamID (always 0)
        inner.buffer.extend_from_slice(&[0, 0, 0]);

        // Tag body
        inner.buffer.extend_from_slice(data);

        // Previous tag size (11 + data_size)
        let tag_size = 11 + data_size;
        inner.buffer.extend_from_slice(&tag_size.to_be_bytes());
    }

    /// Extract chunk data from the buffer without performing I/O.
    fn extract_chunk(inner: &mut FlvChunkSinkInner) -> Option<PendingChunkWrite> {
        if inner.buffer.is_empty() {
            return None;
        }

        let index = inner.chunk_index;
        inner.chunk_index += 1;

        let mut hasher = Md5::new();
        hasher.update(&inner.buffer);
        let md5 = format!("{:x}", hasher.finalize());

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

    /// Write chunk to disk and send notification.
    async fn write_and_notify(&self, pending: PendingChunkWrite) {
        if let Some(parent) = pending.path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::error!("Failed to create chunk dir: {e}");
                return;
            }
        }
        if let Err(e) = tokio::fs::write(&pending.path, &pending.data).await {
            tracing::error!("Failed to write FLV chunk file: {e}");
            return;
        }

        info!(
            "FLV chunk {} written: {} bytes, md5={}",
            pending.index, pending.size, pending.md5
        );

        let chunk_info = ChunkInfo {
            path: pending.path,
            size: pending.size,
            md5: pending.md5,
            index: pending.index,
        };

        if let Err(e) = self.chunk_tx.send(chunk_info) {
            tracing::debug!("No FLV chunk subscribers: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test]
    async fn null_sink_discards_data() {
        let sink = FlvChunkSink::new_null();
        let data = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &data).await;
        assert_eq!(sink.chunk_count().await, 0);
    }

    #[tokio::test]
    async fn saves_video_sequence_header() {
        let sink = FlvChunkSink::new_null();
        // Sequence header: keyframe AVC + packet type 0
        let seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &seq).await;
        let inner = sink.inner.lock().await;
        assert!(inner.video_sequence_header.is_some());
    }

    #[tokio::test]
    async fn saves_audio_sequence_header() {
        let sink = FlvChunkSink::new_null();
        // AAC sequence header: 0xAF = AAC + stereo, 0x00 = seq header
        let seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
        sink.write_audio(0, &seq).await;
        let inner = sink.inner.lock().await;
        assert!(inner.audio_sequence_header.is_some());
    }

    #[tokio::test]
    async fn produces_chunk_after_duration() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FlvChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_millis(50),
        ));
        let mut rx = sink.subscribe();

        // First: send sequence headers
        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;

        // Send a keyframe to start first chunk
        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &keyframe).await;

        // Wait for chunk duration
        tokio::time::sleep(Duration::from_millis(60)).await;

        // Send another keyframe to trigger flush
        let keyframe2 = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xCC, 0xDD][..]);
        sink.write_video(1000, &keyframe2).await;

        let chunk = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(chunk.path.exists());
        assert!(chunk.size > 0);

        // Verify FLV header
        let file_data = std::fs::read(&chunk.path).unwrap();
        assert_eq!(&file_data[0..3], b"FLV");
        assert_eq!(file_data[3], 0x01); // version
        assert_eq!(file_data[4], 0x05); // audio + video flags
    }

    #[tokio::test]
    async fn flush_produces_final_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let sink = Arc::new(FlvChunkSink::new(
            dir.path().to_path_buf(),
            Duration::from_secs(60),
        ));
        let mut rx = sink.subscribe();

        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;

        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &keyframe).await;

        sink.flush().await;

        let chunk = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert!(chunk.path.exists());
        assert!(chunk.size > 0);
    }

    #[tokio::test]
    async fn reset_clears_buffer() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;

        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
        sink.write_video(0, &keyframe).await;

        sink.reset().await;
        sink.flush().await;

        assert_eq!(sink.chunk_count().await, 0);
    }

    #[tokio::test]
    async fn ignores_non_keyframe_before_first_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        // Inter frame (0x27 = non-keyframe AVC)
        let inter = BytesMut::from(&[0x27, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
        sink.write_video(0, &inter).await;

        let inner = sink.inner.lock().await;
        assert!(inner.buffer.is_empty());
        assert!(inner.chunk_start.is_none());
    }

    #[tokio::test]
    async fn flv_tag_structure_is_correct() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));
        let mut rx = sink.subscribe();

        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;

        let payload = vec![0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];
        let keyframe = BytesMut::from(&payload[..]);
        sink.write_video(100, &keyframe).await;

        sink.flush().await;

        let chunk = rx.recv().await.unwrap();
        let file_data = std::fs::read(&chunk.path).unwrap();

        // FLV header (9) + prev tag size 0 (4) = 13 bytes
        // Then video sequence header tag
        let offset = 13;
        assert_eq!(file_data[offset], FLV_TAG_VIDEO); // tag type = video

        // Find the actual keyframe tag after sequence header
        let seq_data_size = 7u32; // our sequence header is 7 bytes
        let seq_tag_total = 11 + seq_data_size as usize + 4; // header + data + prev_tag_size
        let kf_offset = offset + seq_tag_total;

        assert_eq!(file_data[kf_offset], FLV_TAG_VIDEO); // tag type = video
        // Data size (3 bytes)
        let data_size = ((file_data[kf_offset + 1] as u32) << 16)
            | ((file_data[kf_offset + 2] as u32) << 8)
            | (file_data[kf_offset + 3] as u32);
        assert_eq!(data_size, payload.len() as u32);
    }
}
