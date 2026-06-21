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
    /// xiu RTMP timestamp of the first audio tag of the current session.
    /// Audio output ts = `xiu_ts - audio_session_origin_xiu`, putting audio on
    /// the SAME 0-based per-session epoch as video (which restarts at 0 via
    /// `current_session_ts`). `None` until the first audio tag of a session;
    /// re-captured on `start_new_session()` or a backward-jump self-heal (#255).
    /// Preserves the #142 chipmunk fix: the inter-tag deltas (xiu cadence) are
    /// untouched, only the per-session offset is removed.
    audio_session_origin_xiu: Option<u32>,
    /// Last raw xiu audio ts seen — used to detect a backward jump (a missed
    /// republish) so the audio epoch can self-heal even if `start_new_session()`
    /// was never called (#255).
    last_audio_xiu_ts: Option<u32>,
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
                audio_session_origin_xiu: None,
                last_audio_xiu_ts: None,
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
                audio_session_origin_xiu: None,
                last_audio_xiu_ts: None,
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
    /// `timestamp` is the xiu-forwarded RTMP timestamp (in milliseconds).
    /// Audio is stamped on the SAME 0-based per-session epoch as video
    /// (`audio_out = xiu_ts - audio_session_origin_xiu`) — unlike video, which
    /// derives its session ts from wall-clock to fix #135 cache drift. Audio
    /// keeps the xiu inter-tag DELTAS (AAC cadence: 1024 samples => 21.3 ms at
    /// 48 kHz, 23.2 ms at 44.1 kHz, drift-free) and only removes the constant
    /// per-session offset. This preserves the #142 chipmunk fix (no wall-clock
    /// jitter in PTS, no resampling artefacts) while keeping audio aligned with
    /// video across an OBS mid-stream republish (#255) — both restart at 0 on
    /// each new session.
    ///
    /// Internally, this function writes the re-based timestamp into the FLV tag
    /// only — it must NOT touch `chunk_last_ts`, which is an accounting
    /// field owned by `write_video` for tracking wall-clock chunk duration.
    /// Mixing the two domains causes `duration_ms` to underflow and produce
    /// 0 for every chunk (#146, regression introduced by PR #144).
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

            // Defensive self-heal (#255): if the incoming xiu ts jumps backward
            // vs the last audio ts we saw, an OBS republish happened without a
            // start_new_session() re-anchor reaching us. Re-capture the audio
            // origin so audio re-zeroes with the (also-restarting) video epoch
            // instead of baking the dead-air gap into the A/V skew. Mirrors the
            // pusher-side guard (pusher.rs).
            if let Some(prev) = inner.last_audio_xiu_ts {
                if timestamp < prev {
                    let old_origin = inner.audio_session_origin_xiu;
                    inner.audio_session_origin_xiu = Some(timestamp);
                    tracing::warn!(
                        prev_xiu_ts = prev,
                        new_xiu_ts = timestamp,
                        old_origin = ?old_origin,
                        "flv_chunker: AUDIO xiu ts jumped backward -- self-healing audio session origin to new ts (#255)"
                    );
                }
            }
            inner.last_audio_xiu_ts = Some(timestamp);

            // Capture the per-session audio origin on the first real audio tag
            // of the session (set to None by new()/start_new_session()).
            let origin = *inner.audio_session_origin_xiu.get_or_insert(timestamp);

            // Re-base audio onto the shared 0-based session epoch:
            //   audio_out = xiu_ts - origin
            // Video already restarts at 0 each session via current_session_ts,
            // so both tracks share one 0-based epoch and stay aligned across an
            // OBS republish (#255). This PRESERVES the #142 chipmunk fix: only
            // the constant per-session offset is removed, the inter-tag deltas
            // (xiu AAC cadence) are untouched, so PTS still carries correct
            // sample timing (no wall-clock jitter, no resampling artefacts).
            //
            // chunk_last_ts is owned by write_video (wall-clock span for
            // chunk-duration accounting) and MUST NOT be touched here — mixing
            // the domains underflows duration_ms (#146).
            let audio_out = timestamp.saturating_sub(origin);
            Self::write_tag(&mut inner, FLV_TAG_AUDIO, audio_out, data);
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

    /// Start a new ingest session at an OBS mid-stream republish boundary.
    ///
    /// Flushes the partial chunk FIRST, then re-zeros the shared per-session
    /// epoch for BOTH tracks so the next session restarts video and audio at a
    /// common 0 — fixing the ~25.5s A/V skew that was baked into the chunk bytes
    /// after each OBS restart at live event 9315 (#255). Video re-zeroes via
    /// `session_start_wall_clock_ms = 0` (next `current_session_ts` returns 0);
    /// audio re-zeroes via `audio_session_origin_xiu = None` (next audio tag
    /// captures the new origin).
    ///
    /// Deliberately does NOT clear `chunk_index` (chunk numbering must stay
    /// monotonic across republishes) or the saved `video_sequence_header` /
    /// `audio_sequence_header` (the next chunk must remain independently
    /// playable). This is distinct from `reset()`, which is the full-disconnect
    /// teardown.
    pub async fn start_new_session(&self) {
        // Flush the partial chunk on the OLD epoch before re-anchoring.
        self.flush().await;

        let mut inner = self.inner.lock().await;
        let old_anchor = inner.session_start_wall_clock_ms;
        let old_audio_origin = inner.audio_session_origin_xiu;
        // Re-zero the per-session epoch fields for both domains. Keep
        // chunk_index and the saved sequence headers.
        inner.chunk_start = None;
        inner.chunk_first_ts = 0;
        inner.chunk_last_ts = 0;
        inner.chunk_first_wall_clock_ms = 0;
        inner.session_start_wall_clock_ms = 0;
        inner.last_session_ts = 0;
        inner.audio_session_origin_xiu = None;
        inner.last_audio_xiu_ts = None;
        info!(
            chunk_index = inner.chunk_index,
            old_video_anchor_ms = old_anchor,
            old_audio_origin_xiu = ?old_audio_origin,
            "flv_chunker: start_new_session -- re-anchored audio+video to a shared 0 epoch on republish (#255)"
        );
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
        inner.audio_session_origin_xiu = None;
        inner.last_audio_xiu_ts = None;
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
            audio_session_origin_xiu: None,
            last_audio_xiu_ts: None,
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
            (20..1000).contains(&t1),
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
#[path = "flv_chunker_tests.rs"]
mod tests;
