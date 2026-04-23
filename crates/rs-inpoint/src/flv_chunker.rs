use bytes::BytesMut;
use md5::{Digest, Md5};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info};

/// Information about a completed chunk.
#[derive(Debug, Clone)]
pub struct ChunkInfo {
    pub path: PathBuf,
    pub size: usize,
    pub md5: String,
    pub index: u64,
    pub duration_ms: u64,
    /// Unix epoch milliseconds at which the producer wrote the chunk to disk.
    pub wall_clock_written_at_ms: i64,
}

/// Maximum buffer size before a forced flush (50 MB).
const MAX_BUFFER_SIZE: usize = 50 * 1024 * 1024;

/// Maximum pending disk writes before dropping chunks.
const MAX_PENDING_WRITES: u32 = 20;

/// FLV tag type constants.
const FLV_TAG_AUDIO: u8 = 8;
const FLV_TAG_VIDEO: u8 = 9;

/// FLV file header: "FLV" version 1, has audio+video, data offset = 9.
const FLV_HEADER: [u8; 9] = [0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09];

/// Receives raw FLV tag bodies from xiu and produces FLV chunk files.
///
/// Writes valid FLV files directly. Each chunk starts with an FLV header
/// and codec sequence headers, making it independently playable.
pub struct FlvChunkSink {
    inner: Mutex<FlvChunkSinkInner>,
    chunk_tx: broadcast::Sender<ChunkInfo>,
    /// Track pending disk writes to prevent unbounded task spawning.
    pending_writes: Arc<AtomicU32>,
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
    /// RTMP timestamp of the first frame in current chunk (milliseconds).
    chunk_first_ts: u32,
    /// RTMP timestamp of the last frame written to current chunk (milliseconds).
    chunk_last_ts: u32,
    /// Unix epoch ms when the first frame of the current chunk arrived.
    /// Used to compute wall-clock span for timestamp rescaling.
    chunk_first_wall_clock_ms: i64,
}

/// Data extracted from the buffer, ready to be written to disk outside the lock.
struct PendingChunkWrite {
    data: Vec<u8>,
    path: PathBuf,
    size: usize,
    md5: String,
    index: u64,
    duration_ms: u64,
    /// Unix epoch milliseconds stamped at the moment the chunk data was extracted.
    wall_clock_written_at_ms: i64,
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
                chunk_first_ts: 0,
                chunk_last_ts: 0,
                chunk_first_wall_clock_ms: 0,
            }),
            chunk_tx,
            pending_writes: Arc::new(AtomicU32::new(0)),
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
                chunk_first_ts: 0,
                chunk_last_ts: 0,
                chunk_first_wall_clock_ms: 0,
            }),
            chunk_tx,
            pending_writes: Arc::new(AtomicU32::new(0)),
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

            // Always save sequence headers (even in null mode, for state tracking)
            if is_sequence_header {
                inner.video_sequence_header = Some(data.clone());
                debug!("FLV video sequence header saved ({} bytes)", data.len());
                return;
            }

            if inner.null_mode {
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
                Self::write_chunk_header(&mut inner, timestamp);
            } else if inner.chunk_start.is_none() {
                // First chunk — wait for a keyframe to start
                if !is_keyframe {
                    return;
                }
                Self::write_chunk_header(&mut inner, timestamp);
            }

            // Write the xiu-assigned absolute timestamp. The delivery-side
            // FlvStreamNormalizer rebases across chunk boundaries so the
            // combined output to ffmpeg stays monotonic even when xiu's
            // counter resets (OBS reconnect, session reset).
            inner.chunk_last_ts = timestamp;
            Self::write_tag(&mut inner, FLV_TAG_VIDEO, timestamp, data);

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
            if self.spawn_write(pending) {
                // Commit the chunk_index advance now that the write is accepted
                let mut inner = self.inner.lock().await;
                inner.chunk_index += 1;
            }
        }
    }

    /// Process an audio frame from xiu's FrameData::Audio.
    pub async fn write_audio(&self, timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && (data[0] >> 4) == 0x0A && data[1] == 0x00;

        let pending = {
            let mut inner = self.inner.lock().await;

            // Always save sequence headers (even in null mode, for state tracking)
            if is_sequence_header {
                inner.audio_sequence_header = Some(data.clone());
                debug!("FLV audio sequence header saved ({} bytes)", data.len());
                return;
            }

            if inner.null_mode {
                return;
            }

            // Only write audio if a chunk has been started (by a video keyframe)
            if inner.chunk_start.is_none() {
                return;
            }

            inner.chunk_last_ts = timestamp;
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, timestamp, data);
            None
        };

        if let Some(pending) = pending {
            self.spawn_write(pending);
        }
    }

    /// Force flush any buffered data as a final chunk.
    /// Unlike write_video/write_audio, this awaits the write to ensure
    /// all data is on disk before the process exits.
    pub async fn flush(&self) {
        let pending = {
            let mut inner = self.inner.lock().await;
            if inner.null_mode || inner.buffer.is_empty() {
                None
            } else {
                let p = Self::extract_chunk(&mut inner);
                if p.is_some() {
                    inner.chunk_index += 1;
                }
                p
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
    }

    /// Get the total number of chunks produced.
    pub async fn chunk_count(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.chunk_index
    }

    /// Write FLV file header + sequence headers at the start of a new chunk.
    /// `timestamp` is the RTMP timestamp of the first frame — used for content duration tracking.
    /// Note: `chunk_start` (Instant) is for wall-clock flush timing decisions,
    /// while `chunk_first_ts`/`chunk_last_ts` track RTMP content duration.
    fn write_chunk_header(inner: &mut FlvChunkSinkInner, timestamp: u32) {
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

        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        inner.chunk_start = Some(Instant::now());
        inner.chunk_first_ts = timestamp;
        inner.chunk_last_ts = timestamp;
        inner.chunk_first_wall_clock_ms = now_ms;
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
    /// Does NOT increment chunk_index -- the caller must do so only after
    /// confirming the chunk will actually be written (not dropped by backpressure).
    ///
    /// Before handing off the data, rewrites FLV tag timestamps so that the
    /// span from first to last data tag matches the wall-clock duration of the
    /// chunk. This ensures consumer ffmpeg `-re` draining at 1.000× nominal
    /// rate correctly matches the actual wall-clock fill rate, eliminating
    /// cache drift caused by OBS encoding at ~30.30 fps while stamping tags
    /// at 1/30 s (33.33 ms) each. See `rescale_flv_timestamps` and #135.
    fn extract_chunk(inner: &mut FlvChunkSinkInner) -> Option<PendingChunkWrite> {
        if inner.buffer.is_empty() {
            return None;
        }

        let index = inner.chunk_index;

        // FLV header is 9 bytes + 4 bytes PreviousTagSize0 = 13 bytes.
        // Sequence headers (written at ts=0 by write_chunk_header) come next,
        // but rescale_flv_timestamps skips script tags and sequence headers
        // so only data tags are rescaled.
        let header_size: usize = 13;

        // Compute wall-clock span: now minus the moment the first frame arrived.
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        let wall_span_ms = (now_ms - inner.chunk_first_wall_clock_ms).max(0) as u32;

        // Rescale tag timestamps in-place so the FLV content duration matches
        // wall-clock span. No-op when chunk_first_ts == chunk_last_ts (single tag).
        rescale_flv_timestamps(&mut inner.buffer, header_size, wall_span_ms);

        // After rescaling, update the recorded last timestamp so duration_ms
        // below reflects the rescaled span rather than the raw RTMP span.
        let new_last_ts = inner.chunk_first_ts.saturating_add(wall_span_ms);
        inner.chunk_last_ts = new_last_ts;

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

        // Use wall-clock-aligned duration (after rescaling)
        let duration_ms = wall_span_ms as u64;
        inner.chunk_start = None;

        let wall_clock_written_at_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Some(PendingChunkWrite {
            data,
            path,
            size,
            md5,
            index,
            duration_ms,
            wall_clock_written_at_ms,
        })
    }

    /// Spawn a background task to write chunk to disk.
    /// This decouples disk I/O from frame processing -- the calling task
    /// returns immediately and never blocks on file writes.
    /// Returns true if the chunk was accepted for writing, false if dropped
    /// due to backpressure. The caller must increment chunk_index only on true.
    fn spawn_write(&self, pending: PendingChunkWrite) -> bool {
        let current = self.pending_writes.fetch_add(1, Ordering::Relaxed);
        if current >= MAX_PENDING_WRITES {
            self.pending_writes.fetch_sub(1, Ordering::Relaxed);
            tracing::error!(
                pending = current,
                index = pending.index,
                "Disk too slow: {current} pending writes, dropping chunk"
            );
            return false;
        }

        let chunk_tx = self.chunk_tx.clone();
        let pending_counter = Arc::clone(&self.pending_writes);
        tokio::spawn(async move {
            Self::do_write_and_notify(pending, chunk_tx).await;
            pending_counter.fetch_sub(1, Ordering::Relaxed);
        });
        true
    }

    /// Write chunk to disk and send notification (used by both spawn_write and flush).
    async fn do_write_and_notify(
        pending: PendingChunkWrite,
        chunk_tx: broadcast::Sender<ChunkInfo>,
    ) {
        if let Some(parent) = pending.path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::error!("Failed to create chunk dir: {e}");
                return;
            }
        }

        let write_start = Instant::now();
        if let Err(e) = tokio::fs::write(&pending.path, &pending.data).await {
            tracing::error!("Failed to write FLV chunk file: {e}");
            return;
        }
        let write_ms = write_start.elapsed().as_millis();

        if write_ms > 500 {
            tracing::warn!(
                index = pending.index,
                size = pending.size,
                write_ms,
                "Slow chunk write"
            );
        }

        info!(
            "FLV chunk {} written: {} bytes, md5={}, write_ms={}",
            pending.index, pending.size, pending.md5, write_ms
        );

        let chunk_info = ChunkInfo {
            path: pending.path,
            size: pending.size,
            md5: pending.md5,
            index: pending.index,
            duration_ms: pending.duration_ms,
            wall_clock_written_at_ms: pending.wall_clock_written_at_ms,
        };

        if let Err(e) = chunk_tx.send(chunk_info) {
            tracing::warn!("Chunk broadcast failed, no subscribers: {e}");
        }
    }

    /// Write chunk to disk synchronously (used by flush for shutdown correctness).
    async fn write_and_notify(&self, pending: PendingChunkWrite) {
        let chunk_tx = self.chunk_tx.clone();
        Self::do_write_and_notify(pending, chunk_tx).await;
    }
}

/// Rewrite FLV tag timestamps in `buf[header_size..]` so the span from the
/// first to the last data tag (non-script, non-sequence-header) equals
/// `target_span_ms`, preserving relative inter-tag spacing via linear
/// interpolation.
///
/// **Motivation** (Phase 3, #135): OBS encodes at ~30.30 fps wall-clock but
/// stamps FLV tags at 1/30 s (33.33 ms) each. xiu records
/// `duration_ms = last_ts - first_ts`, so chunks carry ~0.6% fewer ms than
/// the real time they took to produce. ffmpeg `-re` (= 1.000×) drains faster
/// than the producer fills, causing cache to shrink at ~20 s/hour.
///
/// By rescaling data-tag timestamps to match the wall-clock span at chunk
/// emit time, `duration_ms` equals the real wall-clock duration and ffmpeg
/// `-re` drains at exactly the correct rate.
///
/// **Rules**:
/// - Script tags (0x12, e.g. onMetaData) are skipped — they typically carry
///   ts=0 and must not be touched.
/// - Sequence headers (video AVC config ts=0, audio AAC config ts=0) keep
///   ts=0 via the `first_ts + (orig_ts - first_ts) * …` formula (they are
///   usually the reference point, so `orig_ts - first_ts == 0`).
/// - No-op when `first_ts == last_ts` (single-tag or zero-span chunk).
/// - No-op when `target_span_ms == last_ts - first_ts` (already correct).
/// - Performs two passes over `buf[header_size..]`: first to find bounds,
///   second to rewrite. No heap allocation; all mutation is in-place.
pub(crate) fn rescale_flv_timestamps(buf: &mut [u8], header_size: usize, target_span_ms: u32) {
    if buf.len() <= header_size {
        return;
    }

    // --- Pass 1: find first_ts and last_ts of data (non-script) tags ---
    let mut first_ts: Option<u32> = None;
    let mut last_ts: u32 = 0;

    let mut ofs = header_size;
    while ofs + 11 <= buf.len() {
        let tag_type = buf[ofs];
        if tag_type != 8 && tag_type != 9 && tag_type != 18 {
            break;
        }
        let data_size =
            ((buf[ofs + 1] as u32) << 16) | ((buf[ofs + 2] as u32) << 8) | (buf[ofs + 3] as u32);
        let tag_total = 11 + data_size as usize + 4;
        if ofs + tag_total > buf.len() {
            break;
        }
        // Skip script tags — they carry metadata (e.g. onMetaData at ts=0)
        // and must not be rescaled.
        if tag_type != 18 {
            let ts = read_flv_ts(&buf[ofs..]);
            if first_ts.is_none() {
                first_ts = Some(ts);
            }
            last_ts = ts;
        }
        ofs += tag_total;
    }

    let first_ts = match first_ts {
        Some(f) => f,
        None => return, // no data tags found
    };

    let orig_span = last_ts.saturating_sub(first_ts);
    // No-op: single tag, zero span, or target already matches.
    if orig_span == 0 || orig_span == target_span_ms {
        return;
    }

    // --- Pass 2: rewrite timestamps via linear interpolation ---
    let mut ofs = header_size;
    while ofs + 11 <= buf.len() {
        let tag_type = buf[ofs];
        if tag_type != 8 && tag_type != 9 && tag_type != 18 {
            break;
        }
        let data_size =
            ((buf[ofs + 1] as u32) << 16) | ((buf[ofs + 2] as u32) << 8) | (buf[ofs + 3] as u32);
        let tag_total = 11 + data_size as usize + 4;
        if ofs + tag_total > buf.len() {
            break;
        }
        if tag_type != 18 {
            let orig_ts = read_flv_ts(&buf[ofs..]);
            let delta = orig_ts.saturating_sub(first_ts) as u64;
            // new_ts = first_ts + delta * target_span_ms / orig_span
            let new_ts = first_ts + ((delta * target_span_ms as u64) / orig_span as u64) as u32;
            write_flv_ts(&mut buf[ofs..], new_ts);
        }
        ofs += tag_total;
    }
}

/// Read the 32-bit FLV tag timestamp from a tag slice starting at offset 0.
/// Layout: bytes [4..7] = lower 24 bits (big-endian), byte [7] = upper 8 bits.
#[inline]
fn read_flv_ts(tag: &[u8]) -> u32 {
    ((tag[4] as u32) << 16) | ((tag[5] as u32) << 8) | (tag[6] as u32) | ((tag[7] as u32) << 24)
}

/// Write a 32-bit FLV tag timestamp into a tag slice starting at offset 0.
#[inline]
fn write_flv_ts(tag: &mut [u8], ts: u32) {
    tag[4] = (ts >> 16) as u8;
    tag[5] = (ts >> 8) as u8;
    tag[6] = ts as u8;
    tag[7] = (ts >> 24) as u8;
}

#[cfg(test)]
impl FlvChunkSinkInner {
    fn new_for_test(chunk_dir: PathBuf) -> Self {
        Self {
            buffer: Vec::new(),
            chunk_dir,
            chunk_duration: Duration::from_secs(60),
            chunk_start: None,
            chunk_index: 0,
            null_mode: false,
            video_sequence_header: None,
            audio_sequence_header: None,
            chunk_first_ts: 0,
            chunk_last_ts: 0,
            chunk_first_wall_clock_ms: 0,
        }
    }
}

#[cfg(test)]
mod wall_clock_tests {
    use super::*;

    #[test]
    fn pending_chunk_write_carries_wall_clock_ms() {
        let before_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let mut inner = FlvChunkSinkInner::new_for_test(std::path::PathBuf::from("/tmp/x"));
        // Seed inner with a non-empty buffer so extract_chunk emits.
        inner.buffer = vec![0x46, 0x4C, 0x56]; // "FLV"
        inner.chunk_first_ts = 0;
        inner.chunk_last_ts = 1000;

        let pending = FlvChunkSink::extract_chunk(&mut inner).expect("chunk emitted");
        let after_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        assert!(
            pending.wall_clock_written_at_ms >= before_ms
                && pending.wall_clock_written_at_ms <= after_ms,
            "wall_clock_written_at_ms {} outside [{before_ms}, {after_ms}]",
            pending.wall_clock_written_at_ms
        );
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

        let payload = [0x17, 0x01, 0x00, 0x00, 0x00, 0xDE, 0xAD, 0xBE, 0xEF];
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

#[cfg(test)]
mod rescale_tests {
    use super::*;

    /// Build a minimal FLV chunk with the given data-tag timestamps.
    /// tag_type=9 (video NALU), 4-byte payload so it is NOT a sequence header
    /// (sequence headers have payload[1]==0x00; we use 0x01).
    fn build_flv_chunk_with_data_timestamps(timestamps: Vec<u32>) -> Vec<u8> {
        let mut buf = Vec::new();
        // FLV header (9 bytes)
        buf.extend_from_slice(&[0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09]);
        // PreviousTagSize0 (4 bytes)
        buf.extend_from_slice(&[0, 0, 0, 0]);
        for ts in timestamps {
            push_video_nalu_tag(&mut buf, ts);
        }
        buf
    }

    /// Build an FLV chunk with a script tag at ts=0, followed by data tags.
    fn build_flv_chunk_mixed(script_ts: u32, data_timestamps: Vec<u32>) -> Vec<u8> {
        let mut buf = Vec::new();
        // FLV header
        buf.extend_from_slice(&[0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09]);
        buf.extend_from_slice(&[0, 0, 0, 0]);
        // Script tag (type 0x12) — onMetaData placeholder
        push_script_tag(&mut buf, script_ts);
        for ts in data_timestamps {
            push_video_nalu_tag(&mut buf, ts);
        }
        buf
    }

    /// Append a video NALU tag (non-sequence-header) at the given timestamp.
    fn push_video_nalu_tag(buf: &mut Vec<u8>, ts: u32) {
        // payload: [0x27=inter AVC, 0x01=NALU, 0x00, 0x00] — 4 bytes
        let payload: [u8; 4] = [0x27, 0x01, 0x00, 0x00];
        let data_size = payload.len() as u32;
        buf.push(9); // tag_type = video
        buf.push((data_size >> 16) as u8);
        buf.push((data_size >> 8) as u8);
        buf.push(data_size as u8);
        buf.push((ts >> 16) as u8);
        buf.push((ts >> 8) as u8);
        buf.push(ts as u8);
        buf.push((ts >> 24) as u8);
        buf.extend_from_slice(&[0, 0, 0]); // StreamID
        buf.extend_from_slice(&payload);
        let prev_sz = 11 + data_size;
        buf.extend_from_slice(&prev_sz.to_be_bytes());
    }

    /// Append a script tag (type 0x12) at the given timestamp.
    fn push_script_tag(buf: &mut Vec<u8>, ts: u32) {
        let payload: [u8; 2] = [0x02, 0x00]; // minimal onMetaData stub
        let data_size = payload.len() as u32;
        buf.push(0x12); // tag_type = script
        buf.push((data_size >> 16) as u8);
        buf.push((data_size >> 8) as u8);
        buf.push(data_size as u8);
        buf.push((ts >> 16) as u8);
        buf.push((ts >> 8) as u8);
        buf.push(ts as u8);
        buf.push((ts >> 24) as u8);
        buf.extend_from_slice(&[0, 0, 0]); // StreamID
        buf.extend_from_slice(&payload);
        let prev_sz = 11 + data_size;
        buf.extend_from_slice(&prev_sz.to_be_bytes());
    }

    /// Read the timestamp of the Nth tag in `buf[header_size..]`.
    fn read_tag_timestamp_at(buf: &[u8], header_size: usize, nth: usize) -> u32 {
        let mut ofs = header_size;
        let mut count = 0;
        while ofs + 11 <= buf.len() {
            let tag_type = buf[ofs];
            if tag_type != 8 && tag_type != 9 && tag_type != 18 {
                break;
            }
            let data_size = ((buf[ofs + 1] as u32) << 16)
                | ((buf[ofs + 2] as u32) << 8)
                | (buf[ofs + 3] as u32);
            let tag_total = 11 + data_size as usize + 4;
            if ofs + tag_total > buf.len() {
                break;
            }
            if count == nth {
                return read_flv_ts(&buf[ofs..]);
            }
            count += 1;
            ofs += tag_total;
        }
        panic!("tag {nth} not found in buffer");
    }

    #[test]
    fn rescale_expands_tag_timestamps_to_wall_span() {
        // Three video data tags at ts 0, 33, 66.
        // Rescale to target_span_ms=100: first stays 0, last becomes 100,
        // middle becomes 50 (linear interpolation).
        let mut buf = build_flv_chunk_with_data_timestamps(vec![0, 33, 66]);
        rescale_flv_timestamps(&mut buf, 13, 100);
        assert_eq!(
            read_tag_timestamp_at(&buf, 13, 0),
            0,
            "first tag must stay at 0"
        );
        assert_eq!(
            read_tag_timestamp_at(&buf, 13, 1),
            50,
            "middle tag must be scaled to 50"
        );
        assert_eq!(
            read_tag_timestamp_at(&buf, 13, 2),
            100,
            "last tag must be scaled to 100"
        );
    }

    #[test]
    fn rescale_preserves_script_tag_timestamps() {
        // Script tag at ts=0, data tags at 33, 66.
        // Script tag must stay at 0 after rescale.
        let mut buf = build_flv_chunk_mixed(0, vec![33, 66]);
        rescale_flv_timestamps(&mut buf, 13, 100);
        // Tag 0 is the script tag — must stay at 0.
        assert_eq!(
            read_tag_timestamp_at(&buf, 13, 0),
            0,
            "script tag timestamp must not be changed"
        );
        // Data tags: first_ts=33 is preserved as the reference; rescale
        // only stretches the gap between first and last. So tag 1 stays
        // at 33, tag 2 becomes 33 + target_span_ms = 33 + 100 = 133.
        assert_eq!(
            read_tag_timestamp_at(&buf, 13, 1),
            33,
            "first data tag must stay at its original timestamp (reference point)"
        );
        assert_eq!(
            read_tag_timestamp_at(&buf, 13, 2),
            133,
            "last data tag must be at first_ts + target_span_ms"
        );
    }

    #[test]
    fn rescale_is_noop_when_span_matches() {
        // target_span_ms == orig_span → no bytes should change.
        let original = build_flv_chunk_with_data_timestamps(vec![0, 33, 66]);
        let mut rescaled = original.clone();
        rescale_flv_timestamps(&mut rescaled, 13, 66);
        assert_eq!(original, rescaled, "rescale to same span must be identity");
    }

    #[test]
    fn rescale_is_noop_for_single_tag() {
        // Single data tag — first_ts == last_ts, orig_span == 0 → no-op.
        let original = build_flv_chunk_with_data_timestamps(vec![500]);
        let mut rescaled = original.clone();
        rescale_flv_timestamps(&mut rescaled, 13, 100);
        assert_eq!(
            original, rescaled,
            "single-tag chunk must be unchanged (no-op)"
        );
    }

    #[test]
    fn rescale_compresses_timestamps_when_target_smaller() {
        // Tags at 0, 100, 200 → compress to target=50.
        let mut buf = build_flv_chunk_with_data_timestamps(vec![0, 100, 200]);
        rescale_flv_timestamps(&mut buf, 13, 50);
        assert_eq!(read_tag_timestamp_at(&buf, 13, 0), 0);
        assert_eq!(read_tag_timestamp_at(&buf, 13, 1), 25);
        assert_eq!(read_tag_timestamp_at(&buf, 13, 2), 50);
    }
}
