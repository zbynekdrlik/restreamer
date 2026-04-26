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
    /// Wall-clock timestamp of the first frame in current chunk (milliseconds).
    chunk_first_ts: u32,
    /// Wall-clock timestamp of the last frame written to current chunk (milliseconds).
    chunk_last_ts: u32,
    /// Unix epoch ms when write_chunk_header was called for the current chunk.
    /// Used to compute wall-clock span vs FLV tag span for drift diagnostics.
    chunk_first_wall_clock_ms: i64,
    /// Unix epoch ms at first tag of current session. 0 = not yet set.
    /// Reset when reset() is called (RTMP disconnect / new session).
    session_start_wall_clock_ms: i64,
    /// Highest output timestamp emitted in the current session. Enforces
    /// monotonic stamping under OS clock skew or wall-clock anomalies.
    last_session_ts: u32,
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
                session_start_wall_clock_ms: 0,
                last_session_ts: 0,
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
                session_start_wall_clock_ms: 0,
                last_session_ts: 0,
            }),
            chunk_tx,
            pending_writes: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Subscribe to chunk completion events.
    pub fn subscribe(&self) -> broadcast::Receiver<ChunkInfo> {
        self.chunk_tx.subscribe()
    }

    /// Compute a wall-clock-derived session timestamp in milliseconds.
    ///
    /// On the first call after a session reset, records `now` as the session
    /// anchor and returns 0. On subsequent calls, returns `now - anchor`,
    /// clamped to be non-decreasing across the session.
    ///
    /// This replaces OBS's declared-fps-based timestamps (which produce 994ms
    /// of tag time per wall-clock second at 30fps) with actual arrival timing,
    /// fixing the cache-drift described in issue #135.
    ///
    /// **A/V sync note:** audio and video tags get stamped with their respective
    /// arrival wall-clock, so network jitter between the two streams (typically
    /// <50ms on TCP RTMP) maps directly to A/V offset in the output. This is
    /// well below the perceptible threshold (~150ms) and acceptable for our
    /// use case of OBS-on-LAN ingest. A future revision may introduce
    /// OBS-PTS-relative stamping with a smoothed rate factor for environments
    /// where ingest jitter is higher (e.g. cellular bonded encoders).
    fn current_session_ts(inner: &mut FlvChunkSinkInner) -> u32 {
        let now_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        if inner.session_start_wall_clock_ms == 0 {
            inner.session_start_wall_clock_ms = now_ms;
            inner.last_session_ts = 0;
            return 0;
        }
        let delta = (now_ms - inner.session_start_wall_clock_ms).max(0);
        // u32 overflows after ~49 days — clamp rather than truncate.
        let candidate = delta.min(u32::MAX as i64) as u32;
        // Monotonic guard: if the OS clock jumped backward (NTP step,
        // suspend/resume) or `now_ms` regressed, clamp to the highest stamp
        // we've already emitted so the FLV stream stays monotonic.
        let out = candidate.max(inner.last_session_ts);
        inner.last_session_ts = out;
        out
    }

    /// Process a video frame from xiu's FrameData::Video.
    ///
    /// `data` is the FLV tag body (codec header + payload) as provided by xiu.
    /// `_xiu_timestamp` is the OBS-declared timestamp — intentionally ignored.
    /// Frames are stamped using wall-clock time since session start instead,
    /// which eliminates the 0.994× producer drift (#135).
    pub async fn write_video(&self, _xiu_timestamp: u32, data: &BytesMut) {
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

            // Drop non-keyframes before the first chunk has started.
            // Do this BEFORE calling current_session_ts so we don't burn the
            // session anchor on a frame we're about to discard.
            if inner.chunk_start.is_none() && !is_keyframe {
                return;
            }

            // Stamp this frame with wall-clock time since session start.
            let ts = Self::current_session_ts(&mut inner);

            let mut pending = None;

            if should_flush && is_keyframe {
                pending = Self::extract_chunk(&mut inner);
                Self::write_chunk_header(&mut inner, ts);
            } else if inner.chunk_start.is_none() {
                // First keyframe — start the chunk.
                Self::write_chunk_header(&mut inner, ts);
            }

            inner.chunk_last_ts = ts;
            Self::write_tag(&mut inner, FLV_TAG_VIDEO, ts, data);

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
    ///
    /// `_xiu_timestamp` is the OBS-declared timestamp — intentionally ignored.
    /// See write_video for the reasoning.
    pub async fn write_audio(&self, _xiu_timestamp: u32, data: &BytesMut) {
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

            let ts = Self::current_session_ts(&mut inner);
            inner.chunk_last_ts = ts;
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, ts, data);
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
    ///
    /// Resets the session wall-clock anchor so timestamps restart from 0
    /// on the next incoming frame. Call this on RTMP disconnect or when a
    /// new streaming session begins.
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.buffer.clear();
        inner.chunk_start = None;
        inner.chunk_first_ts = 0;
        inner.chunk_last_ts = 0;
        inner.chunk_first_wall_clock_ms = 0;
        inner.session_start_wall_clock_ms = 0;
        inner.last_session_ts = 0;
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

        inner.chunk_start = Some(Instant::now());
        inner.chunk_first_ts = timestamp;
        inner.chunk_last_ts = timestamp;
        inner.chunk_first_wall_clock_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
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
    fn extract_chunk(inner: &mut FlvChunkSinkInner) -> Option<PendingChunkWrite> {
        if inner.buffer.is_empty() {
            return None;
        }

        let index = inner.chunk_index;

        // Diagnostic logging for drift analysis (#135).
        {
            let now_ms = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let wall_span_ms = (now_ms - inner.chunk_first_wall_clock_ms).max(0);
            let tag_span_ms = (inner.chunk_last_ts as i64) - (inner.chunk_first_ts as i64);
            // debug! (not info!) — the chunk-emit cadence is hot-path-frequent
            // and only of interest when investigating drift; operators enable
            // it via RUST_LOG=drift_debug=debug.
            tracing::debug!(
                target: "drift_debug",
                chunk_index = index,
                tag_span_ms,
                wall_span_ms,
                buffer_size = inner.buffer.len(),
                "FLV chunk emit"
            );
        }

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

        // Use RTMP frame timestamps for accurate content duration
        let duration_ms = if inner.chunk_last_ts >= inner.chunk_first_ts {
            (inner.chunk_last_ts - inner.chunk_first_ts) as u64
        } else {
            // Timestamp wrapped around (u32 overflow after ~49 days)
            0
        };
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
            session_start_wall_clock_ms: 0,
            last_session_ts: 0,
        }
    }
}

#[cfg(test)]
mod session_ts_tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn first_call_returns_zero_and_anchors() {
        let mut inner = FlvChunkSinkInner::new_for_test(PathBuf::from("/tmp/x"));
        let t = FlvChunkSink::current_session_ts(&mut inner);
        assert_eq!(t, 0, "first call must return 0");
        assert_ne!(
            inner.session_start_wall_clock_ms, 0,
            "anchor must be recorded"
        );
        assert_eq!(inner.last_session_ts, 0, "monotonic guard initialized");
    }

    #[test]
    fn subsequent_calls_advance_with_wall_clock() {
        let mut inner = FlvChunkSinkInner::new_for_test(PathBuf::from("/tmp/x"));
        let t0 = FlvChunkSink::current_session_ts(&mut inner);
        std::thread::sleep(std::time::Duration::from_millis(20));
        let t1 = FlvChunkSink::current_session_ts(&mut inner);
        assert_eq!(t0, 0);
        assert!(
            t1 >= 20 && t1 < 1000,
            "second stamp ({t1}) must reflect ~20ms elapsed"
        );
    }

    #[test]
    fn monotonic_guard_clamps_backward_jumps() {
        // Simulate an OS clock step backward by manipulating the anchor:
        // set anchor in the future, so the next call would compute a negative
        // delta. The guard must return last_session_ts (5000), not regress.
        let mut inner = FlvChunkSinkInner::new_for_test(PathBuf::from("/tmp/x"));
        FlvChunkSink::current_session_ts(&mut inner); // anchor
        inner.last_session_ts = 5000;
        // Move the anchor far into the future to simulate `now < anchor`.
        let future = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 60_000;
        inner.session_start_wall_clock_ms = future;

        let t = FlvChunkSink::current_session_ts(&mut inner);
        assert_eq!(t, 5000, "monotonic guard must hold output at last value");
        assert_eq!(inner.last_session_ts, 5000);
    }

    #[test]
    fn reset_clears_session_anchor_and_monotonic_state() {
        // First session: stamp a few values.
        let mut inner = FlvChunkSinkInner::new_for_test(PathBuf::from("/tmp/x"));
        FlvChunkSink::current_session_ts(&mut inner);
        std::thread::sleep(std::time::Duration::from_millis(10));
        let t1 = FlvChunkSink::current_session_ts(&mut inner);
        assert!(t1 > 0);

        // Manually reset (mirrors what reset() does on the public sink).
        inner.session_start_wall_clock_ms = 0;
        inner.last_session_ts = 0;

        // New session: first stamp must be 0 again.
        let t0 = FlvChunkSink::current_session_ts(&mut inner);
        assert_eq!(t0, 0, "post-reset first stamp must restart at 0");
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

    /// First video frame after session reset must get timestamp = 0.
    #[tokio::test]
    async fn write_video_first_frame_stamps_ts_zero() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        // Seed sequence header so the chunk starts immediately.
        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(999_999, &video_seq).await;

        // First keyframe — xiu timestamp ignored, wall-clock anchor is set.
        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(999_999, &keyframe).await;

        let inner = sink.inner.lock().await;
        // chunk_first_ts is set by write_chunk_header from current_session_ts.
        // The very first call sets anchor and returns 0.
        assert_eq!(
            inner.chunk_first_ts, 0,
            "first frame must anchor session and produce ts=0"
        );
    }

    /// After reset(), next frame must restart timestamp from 0.
    #[tokio::test]
    async fn session_start_resets_on_reset() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        // Seed sequence header.
        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;

        // Write first frame to establish session anchor.
        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
        sink.write_video(0, &keyframe).await;

        // Small delay so next frame would have a non-zero wall-clock ts.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Reset session — anchor must be cleared.
        sink.reset().await;

        {
            let inner = sink.inner.lock().await;
            assert_eq!(
                inner.session_start_wall_clock_ms, 0,
                "reset() must clear session_start_wall_clock_ms"
            );
        }

        // Write the next keyframe — its ts should be 0 again.
        sink.write_video(0, &keyframe).await;
        let inner = sink.inner.lock().await;
        assert_eq!(
            inner.chunk_first_ts, 0,
            "first frame after reset must produce ts=0"
        );
    }

    /// Timestamps must be strictly non-decreasing across successive frames.
    #[tokio::test]
    async fn wall_clock_ts_monotonic_across_frames() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;

        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA][..]);
        let interframe = BytesMut::from(&[0x27, 0x01, 0x00, 0x00, 0x00, 0xBB][..]);

        // First keyframe — starts chunk.
        sink.write_video(0, &keyframe).await;
        let ts0 = sink.inner.lock().await.chunk_last_ts;

        tokio::time::sleep(Duration::from_millis(5)).await;

        // Inter-frame — same chunk.
        sink.write_video(0, &interframe).await;
        let ts1 = sink.inner.lock().await.chunk_last_ts;

        tokio::time::sleep(Duration::from_millis(5)).await;

        sink.write_video(0, &interframe).await;
        let ts2 = sink.inner.lock().await.chunk_last_ts;

        assert!(ts1 >= ts0, "ts1={ts1} should be >= ts0={ts0}");
        assert!(ts2 >= ts1, "ts2={ts2} should be >= ts1={ts1}");
    }

    /// Audio FLV tags must carry the xiu-supplied RTMP timestamp verbatim,
    /// not a wall-clock-derived value. AAC at 48 kHz has fixed-cadence frames
    /// (1024 samples / 48000 Hz = 21.333 ms). Wall-clock stamping introduces
    /// RTMP jitter into PTS, which the downstream decoder interprets as
    /// resampling cues — producing chipmunk pitch shift and glitches.
    /// Regression test for the live-event failure on 2026-04-26.
    #[tokio::test]
    async fn audio_uses_xiu_timestamp_not_wall_clock() {
        let dir = tempfile::tempdir().unwrap();
        let sink = FlvChunkSink::new(dir.path().to_path_buf(), Duration::from_secs(60));

        // Seed sequence headers (audio + video) so the chunk machinery is ready.
        let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
        sink.write_video(0, &video_seq).await;
        let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
        sink.write_audio(0, &audio_seq).await;

        // Start the chunk with a video keyframe.
        let keyframe = BytesMut::from(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB][..]);
        sink.write_video(0, &keyframe).await;

        // Sleep long enough that wall-clock stamping would produce a clearly
        // different value than the xiu timestamp we pass in.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Write two AAC payload tags with explicit xiu timestamps.
        // Byte 0 = 0xAF (AAC + stereo + 16-bit + 44.1k indicator),
        // byte 1 = 0x01 (raw frame, NOT sequence header).
        let aac_frame = BytesMut::from(&[0xAF, 0x01, 0x12, 0x34, 0x56][..]);
        sink.write_audio(21, &aac_frame).await;

        tokio::time::sleep(Duration::from_millis(50)).await;

        sink.write_audio(42, &aac_frame).await;

        // chunk_last_ts is updated by every write_audio call. After the second
        // call it must equal 42, the xiu timestamp we just supplied — NOT
        // ~100 (wall-clock since session start) and NOT 0.
        let inner = sink.inner.lock().await;
        assert_eq!(
            inner.chunk_last_ts, 42,
            "audio FLV tag must carry xiu timestamp 42, got {}",
            inner.chunk_last_ts
        );
    }
}
