//! #367 time-model regression tests for `FlvChunkSink`.
//!
//! INVARIANT under test: the A/V relationship is defined ONLY by the
//! publisher's source timestamps. Whatever ARRIVAL pattern the frames have
//! (a GOP-cache replay burst at one wall instant, a dead-air gap, a new
//! publisher reusing the stream identifier), coincident A/V content must stay
//! coincident in the chunk bytes.
//!
//! Child of `flv_chunker_tests.rs` (`#[path]`): `super::super` is the chunker
//! module, `super` the FLV tag readers.

use super::super::{ChunkInfo, FLV_TAG_AUDIO, FLV_TAG_VIDEO, FlvChunkSink};
use super::first_flv_tag_timestamp;
use crate::wall_clock::WallClock;
use bytes::BytesMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::time::Duration;
use tokio::sync::broadcast;

/// Deterministic wall clock: the test decides when every frame "arrives".
struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(ms: i64) -> Arc<Self> {
        Arc::new(Self(AtomicI64::new(ms)))
    }
    fn set(&self, ms: i64) {
        self.0.store(ms, AtomicOrdering::SeqCst);
    }
}

impl WallClock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(AtomicOrdering::SeqCst)
    }
}

/// Arbitrary Unix-epoch base for the manual clock.
const T0: i64 = 1_790_000_000_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    KeyFrame,
    InterFrame,
    Audio,
}

/// One frame as the hub delivers it: source ts + the wall instant it arrives.
#[derive(Clone, Copy)]
struct Frame {
    kind: Kind,
    src_ts: u32,
    wall_ms: i64,
}

/// Tag body carrying the frame's source ts, so a test can find the exact
/// tag of a given content instant in the chunk bytes.
fn body(kind: Kind, src_ts: u32) -> BytesMut {
    let ts = src_ts.to_be_bytes();
    match kind {
        Kind::KeyFrame => {
            BytesMut::from(&[0x17, 0x01, 0, 0, 0, 0xEE, ts[0], ts[1], ts[2], ts[3]][..])
        }
        Kind::InterFrame => {
            BytesMut::from(&[0x27, 0x01, 0, 0, 0, 0xEE, ts[0], ts[1], ts[2], ts[3]][..])
        }
        Kind::Audio => BytesMut::from(&[0xAF, 0x01, 0xEE, ts[0], ts[1], ts[2], ts[3]][..]),
    }
}

/// Video every 40 ms (25 fps) and audio every 20 ms over `[from, to]`, the
/// first video frame a keyframe. Both grids hit every multiple of 40, so a
/// content instant on that grid has an exactly coincident A and V frame.
/// `arrival(src)` maps a frame's source ts to its arrival wall instant.
fn av_frames(from: u32, to: u32, arrival: impl Fn(u32) -> i64) -> Vec<Frame> {
    let mut frames = Vec::new();
    let mut v = from;
    while v <= to {
        let kind = if v == from {
            Kind::KeyFrame
        } else {
            Kind::InterFrame
        };
        frames.push(Frame {
            kind,
            src_ts: v,
            wall_ms: arrival(v),
        });
        v += 40;
    }
    let mut a = from;
    while a <= to {
        frames.push(Frame {
            kind: Kind::Audio,
            src_ts: a,
            wall_ms: arrival(a),
        });
        a += 20;
    }
    // Delivery order = source order; at equal ts video goes first, so the
    // keyframe opens the chunk before its coincident audio frame.
    frames.sort_by_key(|f| (f.src_ts, if f.kind == Kind::Audio { 1 } else { 0 }));
    frames
}

async fn seed_sequence_headers(sink: &FlvChunkSink) {
    let video_seq = BytesMut::from(&[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64][..]);
    sink.write_video(0, &video_seq).await;
    let audio_seq = BytesMut::from(&[0xAF, 0x00, 0x12, 0x10][..]);
    sink.write_audio(0, &audio_seq).await;
}

async fn feed(sink: &FlvChunkSink, clock: &ManualClock, frames: &[Frame]) {
    for f in frames {
        clock.set(f.wall_ms);
        let data = body(f.kind, f.src_ts);
        match f.kind {
            Kind::Audio => sink.write_audio(f.src_ts, &data).await,
            _ => sink.write_video(f.src_ts, &data).await,
        }
    }
}

/// Flush and collect the bytes of every chunk the sink emitted.
async fn drain_chunks(
    sink: &FlvChunkSink,
    rx: &mut broadcast::Receiver<ChunkInfo>,
) -> Vec<Vec<u8>> {
    sink.flush().await;
    let mut chunks = Vec::new();
    while let Ok(Ok(info)) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
        chunks.push(std::fs::read(&info.path).expect("chunk file readable"));
    }
    chunks
}

/// Output (chunk) ts of the tag carrying `kind`/`src_ts`, across all chunks.
fn out_ts(chunks: &[Vec<u8>], kind: Kind, src_ts: u32) -> u32 {
    let marker = body(kind, src_ts);
    let tag_type = if kind == Kind::Audio {
        FLV_TAG_AUDIO
    } else {
        FLV_TAG_VIDEO
    };
    chunks
        .iter()
        .find_map(|c| first_flv_tag_timestamp(c, tag_type, &marker))
        .unwrap_or_else(|| panic!("no {tag_type} tag for source ts {src_ts} in any chunk"))
}

fn new_sink(dir: &std::path::Path, clock: &Arc<ManualClock>) -> FlvChunkSink {
    FlvChunkSink::new(dir.to_path_buf(), Duration::from_secs(60))
        .with_wall_clock(Arc::clone(clock) as Arc<dyn WallClock>)
}

/// Design test 1 (#367): the Thursday late-join. A subscriber that joins
/// late gets xiu's 1-GOP cache replayed as a BURST at one wall instant (here
/// a keyframe + 1.4 s of A+V), then live frames at real-time pace. Before the
/// fix, video was stamped by ARRIVAL (the burst compressed to ~0 ms) while
/// audio kept its source spacing, which baked a constant audio-late offset
/// equal to the GOP age at join (measured +1430 ms on the YouTube VOD).
/// Coincident A/V content must stay within 50 ms.
#[tokio::test]
async fn gop_cache_burst_keeps_coincident_av_aligned() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    const GOP_START: u32 = 10_000;
    const BURST_END: u32 = GOP_START + 1_400;
    // Everything up to the live edge arrives at T0 (the replay burst); later
    // content arrives at real-time pace after it.
    let frames = av_frames(GOP_START, GOP_START + 3_000, |src| {
        if src <= BURST_END {
            T0
        } else {
            T0 + i64::from(src - BURST_END)
        }
    });
    feed(&sink, &clock, &frames).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    // A coincident content instant well after the burst (source 12_000).
    let mark = GOP_START + 2_000;
    let v = out_ts(&chunks, Kind::InterFrame, mark);
    let a = out_ts(&chunks, Kind::Audio, mark);
    let offset = i64::from(a) - i64::from(v);
    assert!(
        offset.abs() <= 50,
        "coincident A/V content at source {mark} must stay within 50 ms after a GOP-cache \
         burst; got video_out={v} audio_out={a} (audio {offset:+} ms vs video)"
    );
}

/// Design test 4 (#367): a new publisher reusing the stream identifier
/// restarts its source ts near 0 while the chunker never saw a Publish event
/// (the lost-event case). The backward source-ts jump must re-anchor BOTH
/// tracks onto one new session origin, never self-heal one track only.
/// Here the new publisher's VIDEO keyframe arrives first.
#[tokio::test]
async fn backward_source_jump_video_first_reanchors_both_tracks() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    // Session A: 2 s of live A/V at source 600_000.., real-time arrival.
    let a = av_frames(600_000, 602_000, |src| T0 + i64::from(src - 600_000));
    feed(&sink, &clock, &a).await;
    // 5 s of dead air, then a NEW publisher starting at source 0.
    let restart = T0 + 7_000;
    let b = av_frames(0, 1_000, |src| restart + i64::from(src));
    feed(&sink, &clock, &b).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    let v = out_ts(&chunks, Kind::InterFrame, 400);
    let a = out_ts(&chunks, Kind::Audio, 400);
    assert!(
        (i64::from(a) - i64::from(v)).abs() <= 50,
        "after a backward source-ts jump coincident A/V must stay aligned; got \
         video_out={v} audio_out={a}"
    );
    assert_eq!(
        v, 400,
        "the new publisher's keyframe (source 0) must become the new shared session origin"
    );
    assert_eq!(
        chunks.len(),
        2,
        "the old session's partial chunk, then the new one"
    );
    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 601_000),
        1_000,
        "the re-anchor flushes the OLD session's partial chunk to disk"
    );
}

/// Design test 4, other arrival order: the new publisher's AUDIO arrives
/// before its first keyframe. The audio backward jump alone must re-anchor
/// both tracks (audio before the new keyframe is dropped like any audio
/// before a session's first keyframe), so the shared origin is the new
/// keyframe and coincident content stays aligned.
#[tokio::test]
async fn backward_source_jump_audio_first_reanchors_both_tracks() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    let a = av_frames(600_000, 602_000, |src| T0 + i64::from(src - 600_000));
    feed(&sink, &clock, &a).await;

    let restart = T0 + 7_000;
    // New publisher: audio from source 0, first video keyframe at source 100.
    let mut b: Vec<Frame> = (0..100u32)
        .step_by(20)
        .map(|src| Frame {
            kind: Kind::Audio,
            src_ts: src,
            wall_ms: restart + i64::from(src),
        })
        .collect();
    b.extend(av_frames(100, 1_100, |src| restart + i64::from(src)));
    feed(&sink, &clock, &b).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    let v = out_ts(&chunks, Kind::InterFrame, 500);
    let a = out_ts(&chunks, Kind::Audio, 500);
    assert!(
        (i64::from(a) - i64::from(v)).abs() <= 50,
        "after an audio-first backward jump coincident A/V must stay aligned; got \
         video_out={v} audio_out={a}"
    );
    assert_eq!(
        v, 400,
        "the new publisher's first keyframe (source 100) must become the shared session origin"
    );
}

// ---------------------------------------------------------------------------
// Design test 5 (ingest): the ABSOLUTE A/V invariant guard at the chunker.
// ---------------------------------------------------------------------------

/// All audit rows emitted so far.
fn drain_audit(
    rx: &mut tokio::sync::mpsc::Receiver<rs_core::audit::AuditRow>,
) -> Vec<rs_core::audit::AuditRow> {
    let mut rows = Vec::new();
    while let Ok(row) = rx.try_recv() {
        rows.push(row);
    }
    rows
}

fn with_audit(
    sink: FlvChunkSink,
) -> (
    FlvChunkSink,
    rs_core::models::InpointState,
    tokio::sync::mpsc::Receiver<rs_core::audit::AuditRow>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(256);
    let state = rs_core::models::InpointState::new().with_audit_tx(tx);
    (sink.with_ingest_state(state.clone(), 2_000), state, rx)
}

/// The fixed chunker never trips the guard on the Thursday burst: one source
/// transform for both tracks keeps the invariant by construction.
#[tokio::test]
async fn gop_cache_burst_never_trips_the_av_invariant_guard() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let (sink, state, mut audit) = with_audit(new_sink(dir.path(), &clock));
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    let frames = av_frames(10_000, 13_000, |src| {
        if src <= 11_400 {
            T0
        } else {
            T0 + i64::from(src - 11_400)
        }
    });
    feed(&sink, &clock, &frames).await;
    drain_chunks(&sink, &mut rx).await;

    let violations: Vec<_> = drain_audit(&mut audit)
        .into_iter()
        .filter(|r| r.action == rs_core::audit::Action::AvInvariantViolated)
        .collect();
    assert!(
        violations.is_empty(),
        "the burst must not violate the A/V invariant, got {violations:?}"
    );
    assert!(
        !state.ingest_skew_active(),
        "no ingest banner on a healthy burst"
    );
}

/// A constructed violation (a stage that moved audio 700 ms vs video) is
/// LOUD at the next chunk flush: AvInvariantViolated audit row (stage
/// ingest, a_rel/v_rel/delta) plus the #354 ingest banner. A session
/// re-anchor closes the episode with AvInvariantRestored.
#[tokio::test]
async fn constructed_ingest_violation_raises_banner_audit_and_restores_on_reanchor() {
    use rs_core::audit::{Action, Severity, Source};
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let (sink, state, mut audit) = with_audit(new_sink(dir.path(), &clock));
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    let frames = av_frames(0, 400, |src| T0 + i64::from(src));
    feed(&sink, &clock, &frames).await;
    // Construct the violation: the latest audio tag left the stage 700 ms
    // later than the shared transform would have put it.
    sink.inner
        .lock()
        .await
        .av_invariant
        .observe_audio(400, 1_100);
    sink.flush().await;
    let _ = drain_chunks(&sink, &mut rx).await;

    let rows = drain_audit(&mut audit);
    let row = rows
        .iter()
        .find(|r| r.action == Action::AvInvariantViolated)
        .expect("a constructed violation must emit AvInvariantViolated");
    assert_eq!(row.severity, Severity::Warn);
    assert_eq!(row.source, Source::Inpoint);
    assert_eq!(row.detail["stage"], "ingest");
    assert_eq!(row.detail["a_rel_ms"], 700);
    assert_eq!(row.detail["v_rel_ms"], 0);
    assert_eq!(row.detail["delta_ms"], 700);
    assert!(
        state.ingest_skew_active(),
        "the violation must raise the #354 ingest banner"
    );
    assert_eq!(state.ingest_skew_ms(), 700);

    sink.start_new_session().await;
    let rows = drain_audit(&mut audit);
    assert!(
        rows.iter().any(|r| r.action == Action::AvInvariantRestored),
        "a session re-anchor must close the episode with AvInvariantRestored, got {rows:?}"
    );
    assert!(
        !state.ingest_skew_active(),
        "the banner clears on re-anchor"
    );
}

// ---------------------------------------------------------------------------
// Review findings (#367): a single odd source ts must not re-anchor a session.
// ---------------------------------------------------------------------------

/// Feed `frames` except the video frame of content `skip_video_at`.
fn without_video_at(frames: &[Frame], skip_video_at: u32) -> Vec<Frame> {
    frames
        .iter()
        .copied()
        .filter(|f| f.kind == Kind::Audio || f.src_ts != skip_video_at)
        .collect()
}

/// A video frame stamped 1 ms BEFORE its predecessor (timestamp jitter, not
/// a new publisher) must not re-anchor the session. A re-anchor flushes the
/// chunk and drops everything until the next keyframe.
#[tokio::test]
async fn tiny_backward_step_does_not_reanchor() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    let frames = without_video_at(&av_frames(0, 1_000, |src| T0 + i64::from(src)), 560);
    let (before, after): (Vec<Frame>, Vec<Frame>) =
        frames.into_iter().partition(|f| f.src_ts < 560);
    feed(&sink, &clock, &before).await;
    // Content 560's video frame arrives stamped 519, 1 ms before 520.
    clock.set(T0 + 560);
    sink.write_video(519, &body(Kind::InterFrame, 560)).await;
    feed(&sink, &clock, &after).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    assert_eq!(
        chunks.len(),
        1,
        "a 1 ms backward step must not re-anchor (flush) the session"
    );
    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 800),
        800,
        "origin unchanged"
    );
    assert_eq!(out_ts(&chunks, Kind::Audio, 800), 800);
    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 560),
        520,
        "the jitter frame is stamped at the track's last ts (monotonic per track)"
    );
}

/// ONE video frame whose source ts jumped 40 s ahead (a corrupt ts), with
/// its successors back on the original timeline, is an isolated outlier.
/// The successor must not be read as a backward jump that re-anchors the
/// session.
#[tokio::test]
async fn isolated_forward_glitch_does_not_reanchor() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    let frames = without_video_at(&av_frames(0, 1_000, |src| T0 + i64::from(src)), 400);
    let (before, after): (Vec<Frame>, Vec<Frame>) =
        frames.into_iter().partition(|f| f.src_ts < 400);
    feed(&sink, &clock, &before).await;
    clock.set(T0 + 400);
    sink.write_video(400 + 40_000, &body(Kind::InterFrame, 400))
        .await;
    feed(&sink, &clock, &after).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    assert_eq!(
        chunks.len(),
        1,
        "an isolated glitch must not re-anchor the session"
    );
    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 800),
        800,
        "origin unchanged"
    );
    assert_eq!(out_ts(&chunks, Kind::Audio, 800), 800);
}

/// Review finding (#367): ONE video frame whose source ts collapsed far
/// BACKWARD (a corrupt ts), with its successors back on the original
/// timeline, is an isolated outlier like the forward glitch above. It must
/// not re-anchor the session (a flush plus everything dropped until the next
/// keyframe). The frame itself is kept, stamped at the track's last ts.
#[tokio::test]
async fn isolated_low_glitch_does_not_reanchor() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    // Far from 0, so the glitch is a FAR backward step, not jitter.
    let frames = without_video_at(
        &av_frames(10_000, 11_000, |src| T0 + i64::from(src - 10_000)),
        10_400,
    );
    let (before, after): (Vec<Frame>, Vec<Frame>) =
        frames.into_iter().partition(|f| f.src_ts < 10_400);
    feed(&sink, &clock, &before).await;
    clock.set(T0 + 400);
    sink.write_video(5, &body(Kind::InterFrame, 10_400)).await;
    feed(&sink, &clock, &after).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    assert_eq!(
        chunks.len(),
        1,
        "an isolated low glitch must not re-anchor the session"
    );
    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 10_800),
        800,
        "origin unchanged"
    );
    assert_eq!(out_ts(&chunks, Kind::Audio, 10_800), 800);
    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 10_400),
        360,
        "the glitched frame is kept, stamped at the video track's last ts"
    );
}

/// The mirror of the constructed ingest violation above, with the VIDEO side
/// moved: the audio sample comes from the real write path, so the guard must
/// see real audio tags (a stage that skipped observing them would hide every
/// violation).
#[tokio::test]
async fn constructed_video_side_violation_is_caught_against_real_audio() {
    use rs_core::audit::Action;
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let (sink, state, mut audit) = with_audit(new_sink(dir.path(), &clock));
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    let frames = av_frames(0, 400, |src| T0 + i64::from(src));
    feed(&sink, &clock, &frames).await;
    // The latest video tag left the stage 700 ms later than the shared
    // transform would have put it.
    sink.inner
        .lock()
        .await
        .av_invariant
        .observe_video(400, 1_100);
    sink.flush().await;
    let _ = drain_chunks(&sink, &mut rx).await;

    let rows = drain_audit(&mut audit);
    let row = rows
        .iter()
        .find(|r| r.action == Action::AvInvariantViolated)
        .expect("a constructed video-side violation must emit AvInvariantViolated");
    assert_eq!(row.detail["a_rel_ms"], 0, "audio sample from the real path");
    assert_eq!(row.detail["v_rel_ms"], 700);
    assert_eq!(row.detail["delta_ms"], -700);
    assert_eq!(state.ingest_skew_ms(), -700);
}

/// Audio content OLDER than the session's first keyframe (a GOP-cache
/// replay can deliver it after the keyframe) has no place on the session
/// timeline: it is dropped, never stamped below the origin.
#[tokio::test]
async fn audio_older_than_the_session_origin_is_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    sink.write_video(1_000, &body(Kind::KeyFrame, 1_000)).await;
    sink.write_audio(980, &body(Kind::Audio, 980)).await;
    sink.write_audio(1_000, &body(Kind::Audio, 1_000)).await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    assert_eq!(out_ts(&chunks, Kind::Audio, 1_000), 0);
    let marker = body(Kind::Audio, 980);
    assert!(
        chunks
            .iter()
            .all(|c| first_flv_tag_timestamp(c, FLV_TAG_AUDIO, &marker).is_none()),
        "audio older than the session origin must not be written"
    );
}

/// A tag held as a far-backward candidate belongs to no session once the
/// session restarts (a Publish re-anchors): it must be dropped, never
/// released into the new session at the OLD timeline's ts.
#[tokio::test]
async fn a_held_tag_is_dropped_by_a_session_restart() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    feed(
        &sink,
        &clock,
        &av_frames(10_000, 11_000, |src| T0 + i64::from(src - 10_000)),
    )
    .await;
    // A far-backward KEYFRAME is held as a candidate...
    sink.write_video(5, &body(Kind::KeyFrame, 5)).await;
    // ...and the next thing is a new session (the receiver saw a Publish).
    sink.start_new_session().await;
    let restart = T0 + 5_000;
    feed(
        &sink,
        &clock,
        &av_frames(0, 1_000, |src| restart + i64::from(src)),
    )
    .await;
    let chunks = drain_chunks(&sink, &mut rx).await;

    assert_eq!(
        out_ts(&chunks, Kind::InterFrame, 400),
        400,
        "the new session's own keyframe is its origin"
    );
    assert_eq!(out_ts(&chunks, Kind::Audio, 400), 400);
}

/// Review finding (#367): a forward-glitched LAST video tag of a chunk must
/// not stretch the chunk's content duration. `duration_ms` feeds the S3
/// chunk metadata and the VPS buffer accounting; with source-ts stamping one
/// corrupt ts made it 40 s too long.
#[tokio::test]
async fn a_forward_glitch_does_not_stretch_the_chunk_duration() {
    let dir = tempfile::tempdir().unwrap();
    let clock = ManualClock::new(T0);
    let sink = new_sink(dir.path(), &clock);
    let mut rx = sink.subscribe();
    seed_sequence_headers(&sink).await;

    feed(
        &sink,
        &clock,
        &av_frames(0, 1_000, |src| T0 + i64::from(src)),
    )
    .await;
    clock.set(T0 + 1_040);
    sink.write_video(1_040 + 40_000, &body(Kind::InterFrame, 1_040))
        .await;
    sink.flush().await;
    let info = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("the flushed chunk")
        .expect("chunk info");
    assert_eq!(
        info.duration_ms, 1_000,
        "a glitched last tag must not stretch the chunk duration"
    );
}
