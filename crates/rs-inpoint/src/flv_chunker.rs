use bytes::BytesMut;
use md5::{Digest, Md5};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info};

use rs_core::models::InpointState;

use crate::ingest_report::{BoundaryReport, publish_boundary, publish_reanchor};
use crate::ingest_skew::IngestSkewMonitor;
use crate::src_track::{SrcTrack, Track};
use crate::wall_clock::{WallClock, system_clock};
use rs_rtmp_push::{AvInvariantEvent, AvInvariantGuard};

#[path = "flv_chunker_ingest.rs"]
mod ingest;

/// Default ingest A/V-skew alert threshold (ms) when no `InpointState` /
/// config-driven threshold is wired (tests, null sink). Mirrors
/// `rs_core::config::default_skew_threshold_ms` (#354).
const DEFAULT_SKEW_THRESHOLD_MS: i64 = 2_000;

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
    /// Optional shared ingest state (#354): the chunker publishes the live
    /// ingest A/V skew + latched banner flag here and emits the skew and
    /// A/V-invariant (#367) audit rows through its `audit_tx`. `None` in tests / the null sink (no
    /// surfacing). Wired via `with_ingest_state`.
    ingest_state: Option<InpointState>,
    /// The operator alert threshold (ms) the monitor was built with (#354).
    /// Mirrored here (outside the `inner` mutex) so `publish_boundary` can stamp
    /// it onto the audit row without re-acquiring the lock it was just
    /// dropped from.
    skew_threshold_ms: i64,
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
    /// Output ts (session source domain) of the first VIDEO frame in the
    /// current chunk. VIDEO tags only (#146).
    chunk_first_ts: u32,
    /// Output ts of the last VIDEO frame written to the current chunk.
    chunk_last_ts: u32,
    /// Unix epoch ms when write_chunk_header was called for the current chunk.
    /// Used to compute wall-clock span vs FLV tag span for drift diagnostics.
    chunk_first_wall_clock_ms: i64,
    /// #367 shared session origin: the publisher's SOURCE ts of the session's
    /// first video keyframe. BOTH tracks are stamped `src - session_origin`
    /// (one common transform), so the publisher's A/V relationship survives
    /// any arrival pattern. `None` until a session's first keyframe; cleared
    /// by `start_new_session()`, `reset()` and a backward source-ts jump.
    session_origin_src: Option<u32>,
    /// Source-ts history per track (#367, `src_track`): a real backward jump
    /// (a new publisher reusing the identifier with no Publish event reaching
    /// us) on EITHER track re-anchors the session for BOTH; a tiny jitter
    /// step or a lone glitch does not.
    video_src: SrcTrack,
    audio_src: SrcTrack,
    /// A far-backward tag held until the next tag decides whether it starts
    /// a new timeline or was a lone glitch (#367, `flv_chunker_ingest`).
    held: Option<ingest::HeldTag>,
    /// Ingest A/V-skew monitor (#354): observes the SAME chunker-stamped
    /// content-PTS the VPS pusher's `SkewTracker` consumes downstream, and
    /// latches a sustained-over-threshold state at each chunk boundary. Reset
    /// on `start_new_session()` / `reset()` alongside the epoch fields.
    skew_monitor: IngestSkewMonitor,
    /// #367 absolute A/V invariant guard (no baseline): for the latest tag
    /// of each track, `(a_out - v_out) == (a_src - v_src)` within 50 ms.
    /// Holds by construction with the one source-ts transform; it catches
    /// ANY future stage code that breaks it. Evaluated at every chunk flush.
    av_invariant: AvInvariantGuard,
    /// Wall-clock seam (#367): every Unix-epoch read in the chunker goes
    /// through here so tests can replay arrival bursts deterministically.
    clock: Arc<dyn WallClock>,
}

impl FlvChunkSinkInner {
    fn new(chunk_dir: PathBuf, chunk_duration: Duration, null_mode: bool) -> Self {
        Self {
            buffer: if null_mode {
                Vec::new()
            } else {
                Vec::with_capacity(128 * 1024)
            },
            chunk_dir,
            chunk_duration,
            chunk_start: None,
            chunk_index: 0,
            null_mode,
            video_sequence_header: None,
            audio_sequence_header: None,
            chunk_first_ts: 0,
            chunk_last_ts: 0,
            chunk_first_wall_clock_ms: 0,
            session_origin_src: None,
            video_src: SrcTrack::default(),
            audio_src: SrcTrack::default(),
            held: None,
            skew_monitor: IngestSkewMonitor::new(DEFAULT_SKEW_THRESHOLD_MS),
            av_invariant: AvInvariantGuard::default(),
            clock: system_clock(),
        }
    }

    /// The source-ts history of `track`.
    fn src_track(&self, track: Track) -> SrcTrack {
        match track {
            Track::Video => self.video_src,
            Track::Audio => self.audio_src,
        }
    }
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
            inner: Mutex::new(FlvChunkSinkInner::new(chunk_dir, chunk_duration, false)),
            chunk_tx,
            pending_writes: Arc::new(AtomicU32::new(0)),
            ingest_state: None,
            skew_threshold_ms: DEFAULT_SKEW_THRESHOLD_MS,
        }
    }

    /// Create a null sink that discards all data (for testing).
    pub fn new_null() -> Self {
        let (chunk_tx, _) = broadcast::channel(1);
        Self {
            inner: Mutex::new(FlvChunkSinkInner::new(
                PathBuf::new(),
                Duration::from_secs(1),
                true,
            )),
            chunk_tx,
            pending_writes: Arc::new(AtomicU32::new(0)),
            ingest_state: None,
            skew_threshold_ms: DEFAULT_SKEW_THRESHOLD_MS,
        }
    }

    /// Replace the wall-clock source (#367). Consuming builder — call before
    /// the sink is `Arc`-wrapped. Production keeps the default system clock;
    /// tests inject a manual clock to replay arrival bursts deterministically.
    pub fn with_wall_clock(mut self, clock: Arc<dyn WallClock>) -> Self {
        self.inner.get_mut().clock = clock;
        self
    }

    /// Subscribe to chunk completion events.
    pub fn subscribe(&self) -> broadcast::Receiver<ChunkInfo> {
        self.chunk_tx.subscribe()
    }

    /// Wire the shared ingest state + operator skew threshold (#354). The
    /// chunker then publishes the live ingest A/V skew into `state`, latches
    /// the banner flag, and emits the skew audit row via `state`'s audit
    /// channel. Consuming builder — call before the sink is `Arc`-wrapped.
    pub fn with_ingest_state(mut self, state: InpointState, threshold_ms: i64) -> Self {
        // Rebuild the monitor with the config-driven threshold (the inner's
        // default is DEFAULT_SKEW_THRESHOLD_MS for the null/test paths).
        self.inner.get_mut().skew_monitor = IngestSkewMonitor::new(threshold_ms);
        self.ingest_state = Some(state);
        self.skew_threshold_ms = threshold_ms;
        self
    }

    /// Process a video frame from xiu's FrameData::Video.
    ///
    /// `data` is the FLV tag body (codec header + payload) as provided by xiu.
    /// `xiu_timestamp` is the publisher's SOURCE ts. The frame is stamped
    /// `xiu_timestamp - session_origin`, the SAME transform audio gets, so
    /// the publisher's A/V relationship survives into the chunk bytes
    /// whatever the arrival pattern is (#367).
    ///
    /// Arrival time is never used as content time. #135/#140 stamped video by
    /// arrival wall-clock while audio kept source ts, so every burst arrival
    /// (a GOP-cache replay to a late subscriber, a process freeze, TCP
    /// backlog) became a constant A/V offset: the 2026-10-01 incident
    /// (+1430 ms on YouTube). The #135 rate concern was re-measured for #367:
    /// on the current rig |src/wall - 1| <= 0.0005 %, so no rate correction
    /// is applied.
    pub async fn write_video(&self, xiu_timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && data[1] == 0x00;
        let fx = {
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
            Self::ingest_tag(&mut inner, Track::Video, xiu_timestamp, data)
        };
        self.finish_tag(fx).await;
    }

    /// Process an audio frame from xiu's FrameData::Audio.
    ///
    /// `timestamp` is the publisher's SOURCE ts (xiu forwards the RTMP ts).
    /// Audio is stamped `timestamp - session_origin`: the SAME shared origin
    /// as video, the source ts of the session's first keyframe (#367).
    pub async fn write_audio(&self, timestamp: u32, data: &BytesMut) {
        let is_sequence_header = data.len() > 1 && (data[0] >> 4) == 0x0A && data[1] == 0x00;
        let fx = {
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
            Self::ingest_tag(&mut inner, Track::Audio, timestamp, data)
        };
        self.finish_tag(fx).await;
    }

    /// Stamp a video frame into the session's source-ts domain, anchoring the
    /// shared session origin on the session's first (key)frame.
    fn video_out_ts(inner: &mut FlvChunkSinkInner, src_ts: u32) -> u32 {
        let origin = match inner.session_origin_src {
            Some(origin) => origin,
            None => {
                inner.session_origin_src = Some(src_ts);
                info!(
                    chunk_index = inner.chunk_index,
                    session_origin_src = src_ts,
                    "flv_chunker: session origin anchored on the first keyframe -- both tracks \
                     are stamped src_ts - origin (#367)"
                );
                src_ts
            }
        };
        // Never below the origin: a later video frame is >= the previous one
        // (a backward jump re-anchors, jitter and a lone glitch are clamped up
        // to the last ts), and the origin IS the session's first video frame.
        src_ts.saturating_sub(origin)
    }

    /// Force flush any buffered data as a final chunk.
    /// Unlike write_video/write_audio, this awaits the write to ensure
    /// all data is on disk before the process exits.
    pub async fn flush(&self) {
        let (pending, invariant) = {
            let mut inner = self.inner.lock().await;
            // A null sink never buffers anything (its writes return early).
            if inner.buffer.is_empty() {
                (None, BoundaryReport::default())
            } else {
                // #367: a flush is a chunk boundary for the absolute A/V
                // invariant too (the skew monitor keeps its keyframe-boundary
                // cadence, unchanged).
                let invariant = inner.av_invariant.evaluate();
                let report = BoundaryReport::capture(
                    &inner.skew_monitor,
                    &inner.av_invariant,
                    None,
                    invariant,
                );
                let p = Self::extract_chunk(&mut inner);
                if p.is_some() {
                    inner.chunk_index += 1;
                }
                (p, report)
            }
        };

        publish_boundary(
            self.ingest_state.as_ref(),
            self.skew_threshold_ms,
            invariant,
        );
        if let Some(pending) = pending {
            self.write_and_notify(pending).await;
        }
    }

    /// Start a new ingest session at a (re)publish / re-subscribe boundary.
    ///
    /// Flushes the partial chunk FIRST, then clears the shared session origin
    /// so the next keyframe re-anchors BOTH tracks onto a new common origin
    /// (#255, #367).
    ///
    /// Deliberately does NOT clear `chunk_index` (chunk numbering must stay
    /// monotonic across republishes) or the saved `video_sequence_header` /
    /// `audio_sequence_header` (the next chunk must remain independently
    /// playable). This is distinct from `reset()`, which is the full-disconnect
    /// teardown.
    pub async fn start_new_session(&self) {
        // Flush the partial chunk on the OLD session before re-anchoring.
        self.flush().await;

        let mut inner = self.inner.lock().await;
        let old_origin = inner.session_origin_src;
        let invariant = Self::clear_session_epoch(&mut inner);
        info!(
            chunk_index = inner.chunk_index,
            old_session_origin_src = ?old_origin,
            "flv_chunker: start_new_session -- cleared the shared session origin; the next \
             keyframe re-anchors audio+video together (#255, #367)"
        );
        drop(inner);
        publish_reanchor(self.ingest_state.as_ref(), invariant);
    }

    /// Reset the chunker state.
    ///
    /// Discards the buffered partial chunk and clears the session origin so
    /// timestamps restart from 0 on the next keyframe. Call this on RTMP
    /// disconnect or when a new streaming session begins.
    pub async fn reset(&self) {
        let mut inner = self.inner.lock().await;
        inner.buffer.clear();
        let invariant = Self::clear_session_epoch(&mut inner);
        drop(inner);
        publish_reanchor(self.ingest_state.as_ref(), invariant);
    }

    /// Evaluate both A/V guards at a chunk boundary (under the lock).
    fn evaluate_boundary(inner: &mut FlvChunkSinkInner) -> BoundaryReport {
        let skew = inner.skew_monitor.evaluate_chunk();
        let invariant = inner.av_invariant.evaluate();
        BoundaryReport::capture(
            &inner.skew_monitor,
            &inner.av_invariant,
            Some(skew),
            invariant,
        )
    }

    /// Re-zero the per-session time state: the shared source origin, the
    /// last source ts of BOTH tracks, the skew monitor and the invariant
    /// guard. Keeps `chunk_index` and the saved sequence headers. Returns the
    /// guard's `Restored` edge if a violation was latched.
    fn clear_session_epoch(inner: &mut FlvChunkSinkInner) -> Option<AvInvariantEvent> {
        inner.chunk_start = None;
        inner.chunk_first_ts = 0;
        inner.chunk_last_ts = 0;
        inner.chunk_first_wall_clock_ms = 0;
        inner.session_origin_src = None;
        inner.video_src.clear();
        inner.audio_src.clear();
        // A held far-backward tag belongs to no session anymore.
        inner.held = None;
        // #354: a new session is a new common origin, and the operator banner
        // must clear on it.
        inner.skew_monitor.reset();
        // #367: a new session is a new transform.
        inner.av_invariant.reset()
    }

    /// Hand an extracted chunk to the background writer, and commit the
    /// `chunk_index` advance only once the write was accepted.
    async fn commit_chunk(&self, pending: PendingChunkWrite) {
        if self.spawn_write(pending) {
            let mut inner = self.inner.lock().await;
            inner.chunk_index += 1;
        }
    }

    /// Get the total number of chunks produced.
    pub async fn chunk_count(&self) -> u64 {
        let inner = self.inner.lock().await;
        inner.chunk_index
    }

    /// Write FLV file header + sequence headers at the start of a new chunk.
    /// `timestamp` is the output ts (session source domain) of the chunk's
    /// first video frame — used for content duration tracking.
    /// Note: `chunk_start` (Instant) is for wall-clock flush timing decisions,
    /// while `chunk_first_ts`/`chunk_last_ts` track the VIDEO content span.
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
        inner.chunk_first_wall_clock_ms = inner.clock.now_ms();
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

        // Diagnostic logging for drift analysis (#135). Since #367 the tag
        // span is the VIDEO span in the publisher's source-ts domain, so
        // tag_span_ms vs wall_span_ms measures the src-vs-wall rate again.
        {
            let now_ms = inner.clock.now_ms();
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

        let timestamp = inner.clock.now_ms();
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

        let wall_clock_written_at_ms = inner.clock.now_ms();

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
        Self::new(chunk_dir, Duration::from_secs(60), false)
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
