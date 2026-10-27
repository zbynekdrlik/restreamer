//! A freeze tail after a deliberate end of the source (#424).
//!
//! YouTube drops the last ~9-14 s it received before an unpublish. camera-box
//! measured it with OBS straight to YouTube too (26.10.2026, VOD
//! `wISCgipxLoo`: 14.3 s missing), so it is not our pusher. An endpoint that
//! played the source out to its end therefore keeps the publish alive for
//! `TAIL_HOLD` with the last picture frozen and silent audio, then ends it.
//! YouTube's loss falls on that padding, not on the program.
//!
//! Codec-homogeneous (#249): the picture is the last keyframe the live
//! encoder produced, sent with its own AVC sequence header, and the audio is
//! AAC-LC silence for the stream's own AudioSpecificConfig. Timestamps go on
//! after the last real tag and the pusher paces them in real time: the push
//! speed never changes (#424).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};

use crate::endpoint_task::PrefetchedChunk;
use crate::pushable::Pushable;

/// How long the frozen picture runs after the end. camera-box measured up
/// to 14.3 s lost; 20 s leaves a margin (their ask: at least 15 s).
pub const TAIL_HOLD: Duration = Duration::from_secs(20);

/// Each pushed segment covers this much tail, so a source that comes back
/// (or a stop) is noticed within about a second.
const SEGMENT_MS: u64 = 1000;
/// Repeat interval bounds for the frozen keyframe. The interval itself keeps
/// the tail's bitrate at or below the live one (`video_step_ms`).
const MIN_VIDEO_STEP_MS: u64 = 33;
const MAX_VIDEO_STEP_MS: u64 = 1000;
const AAC_FRAME_SAMPLES: f64 = 1024.0;

/// AAC-LC raw frames that decode to digital silence (the frames hls.js uses).
const SILENT_MONO: &[u8] = &[0x00, 0xc8, 0x00, 0x80, 0x23, 0x80];
const SILENT_STEREO: &[u8] = &[0x21, 0x00, 0x49, 0x90, 0x02, 0x19, 0x00, 0x23, 0x80];
const SAMPLE_RATES: [u32; 13] = [
    96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000,
    7_350,
];
const FLV_HEADER: &[u8] = &[b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
const AUDIO: u8 = 8;
const VIDEO: u8 = 9;

/// Why an endpoint ends without a tail (today's behaviour then).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoTail {
    /// No chunk delivered yet on this session.
    NoChunk,
    /// No AVC keyframe with its sequence header delivered yet.
    NoKeyframe,
    /// The keyframe was encoded with another AVC configuration than the last
    /// chunk (the source was republished in between): never mix them (#249).
    CodecChanged,
    /// The last chunk carries no AAC configuration.
    NoAudioConfig,
    /// Silence is known only for AAC-LC mono and stereo.
    UnsupportedAudio { object_type: u8, channels: u8 },
}

struct Tag<'a> {
    kind: u8,
    ts: u32,
    body: &'a [u8],
}

fn parse(bytes: &[u8]) -> Vec<Tag<'_>> {
    if bytes.len() < 9 || &bytes[..3] != b"FLV" {
        return Vec::new();
    }
    let mut i = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize + 4;
    let mut tags = Vec::new();
    while i + 11 <= bytes.len() {
        let len = u32::from_be_bytes([0, bytes[i + 1], bytes[i + 2], bytes[i + 3]]) as usize;
        let ts = u32::from_be_bytes([bytes[i + 7], bytes[i + 4], bytes[i + 5], bytes[i + 6]]);
        let Some(body) = bytes.get(i + 11..i + 11 + len) else {
            break;
        };
        tags.push(Tag {
            kind: bytes[i],
            ts,
            body,
        });
        i += 11 + len + 4;
    }
    tags
}

fn is_avc_seq(t: &Tag<'_>) -> bool {
    t.kind == VIDEO && t.body.len() >= 2 && t.body[0] & 0x0f == 7 && t.body[1] == 0
}

fn is_avc_keyframe(t: &Tag<'_>) -> bool {
    t.kind == VIDEO && t.body.len() >= 5 && t.body[0] == 0x17 && t.body[1] == 1
}

fn is_aac_seq(t: &Tag<'_>) -> bool {
    t.kind == AUDIO && t.body.len() >= 4 && t.body[0] >> 4 == 10 && t.body[1] == 0
}

fn is_media(t: &Tag<'_>) -> bool {
    matches!(t.kind, AUDIO | VIDEO) && t.body.len() >= 2 && t.body[1] != 0
}

/// Whether `chunk` carries an AVC keyframe the tail could freeze on.
pub fn has_keyframe(chunk: &[u8]) -> bool {
    parse(chunk).iter().any(is_avc_keyframe)
}

/// Sample rate and silent frame for an AAC AudioSpecificConfig.
fn silence_for(config: &[u8]) -> Result<(u32, &'static [u8]), NoTail> {
    let [a, b, ..] = *config else {
        return Err(NoTail::NoAudioConfig);
    };
    let object_type = a >> 3;
    let channels = (b >> 3) & 0x0f;
    let rate = SAMPLE_RATES.get(usize::from(((a & 0x07) << 1) | (b >> 7)));
    let silent = match channels {
        1 => Some(SILENT_MONO),
        2 => Some(SILENT_STEREO),
        _ => None,
    };
    match (object_type, rate, silent) {
        (2, Some(&rate), Some(silent)) => Ok((rate, silent)),
        _ => Err(NoTail::UnsupportedAudio {
            object_type,
            channels,
        }),
    }
}

/// Repeat interval of a keyframe of `key_len` bytes that keeps the tail at
/// or below the live bitrate (`chunk_len` bytes over `span_ms`).
fn video_step_ms(chunk_len: usize, key_len: usize, span_ms: u64) -> u64 {
    let span = if span_ms == 0 { SEGMENT_MS } else { span_ms };
    if chunk_len == 0 {
        return MAX_VIDEO_STEP_MS;
    }
    (key_len as u64 * span)
        .div_ceil(chunk_len as u64)
        .clamp(MIN_VIDEO_STEP_MS, MAX_VIDEO_STEP_MS)
}

fn media_span_ms(tags: &[Tag<'_>]) -> u64 {
    let ts = tags.iter().filter(|t| is_media(t)).map(|t| t.ts);
    match (ts.clone().min(), ts.max()) {
        (Some(lo), Some(hi)) => u64::from(hi - lo),
        _ => 0,
    }
}

fn write_tag(out: &mut Vec<u8>, kind: u8, ts: u64, body: &[u8]) {
    let ts = ts as u32;
    let len = body.len() as u32;
    out.push(kind);
    out.extend_from_slice(&len.to_be_bytes()[1..]);
    out.extend_from_slice(&ts.to_be_bytes()[1..]);
    out.push((ts >> 24) as u8);
    out.extend_from_slice(&[0, 0, 0]);
    out.extend_from_slice(body);
    out.extend_from_slice(&(len + 11).to_be_bytes());
}

/// The tail as FLV segments of about `SEGMENT_MS` each, in the chunk format
/// the pusher already takes: FLV header, both sequence headers at ts 0, then
/// the media tags. `last_chunk` is the last chunk delivered, `keyframe_chunk`
/// the last delivered chunk that carried a keyframe (it may be the same).
pub fn freeze_tail(
    last_chunk: &[u8],
    keyframe_chunk: &[u8],
    hold: Duration,
) -> Result<Vec<Vec<u8>>, NoTail> {
    let key_tags = parse(keyframe_chunk);
    let avc_seq = key_tags.iter().find(|t| is_avc_seq(t));
    let key = key_tags.iter().rev().find(|t| is_avc_keyframe(t));
    let (Some(avc_seq), Some(key)) = (avc_seq, key) else {
        return Err(NoTail::NoKeyframe);
    };
    let last_tags = parse(last_chunk);
    if last_tags
        .iter()
        .find(|t| is_avc_seq(t))
        .is_some_and(|s| s.body != avc_seq.body)
    {
        return Err(NoTail::CodecChanged);
    }
    let aac_seq = last_tags
        .iter()
        .find(|t| is_aac_seq(t))
        .ok_or(NoTail::NoAudioConfig)?;
    let (rate, silent) = silence_for(&aac_seq.body[2..])?;

    let last_ts = |kind: u8| {
        last_tags
            .iter()
            .filter(|t| t.kind == kind && is_media(t))
            .map(|t| u64::from(t.ts))
            .max()
    };
    let (last_video, last_audio) = (last_ts(VIDEO), last_ts(AUDIO));
    let end = last_video.max(last_audio).ok_or(NoTail::NoChunk)?;
    let tail_end = end + hold.as_millis() as u64;

    let mut events: Vec<(u64, u8, Vec<u8>)> = Vec::new();
    let mut frozen = key.body.to_vec();
    frozen[2..5].fill(0);
    let step = video_step_ms(
        keyframe_chunk.len(),
        key.body.len(),
        media_span_ms(&key_tags),
    );
    let first_video = last_video.unwrap_or(end) + MIN_VIDEO_STEP_MS;
    for v in (first_video..tail_end).step_by(step as usize) {
        events.push((v, VIDEO, frozen.clone()));
    }
    let frame_ms = AAC_FRAME_SAMPLES * 1000.0 / f64::from(rate);
    let mut silence = vec![aac_seq.body[0], 1];
    silence.extend_from_slice(silent);
    let audio_from = last_audio.unwrap_or(end) as f64;
    let frames = (hold.as_millis() as f64 / frame_ms).ceil() as u64 + 1;
    for n in 1..=frames {
        let a = (audio_from + n as f64 * frame_ms).round() as u64;
        if a < tail_end {
            events.push((a, AUDIO, silence.clone()));
        }
    }
    events.sort_by_key(|e| (e.0, e.1));

    let mut segments: Vec<Vec<u8>> = Vec::new();
    let mut current: Option<(u64, Vec<u8>)> = None;
    for (ts, kind, body) in events {
        let index = (ts - end) / SEGMENT_MS;
        if current.as_ref().is_none_or(|(i, _)| *i != index) {
            segments.extend(current.take().map(|(_, s)| s));
            let mut s = FLV_HEADER.to_vec();
            write_tag(&mut s, VIDEO, 0, avc_seq.body);
            write_tag(&mut s, AUDIO, 0, aac_seq.body);
            current = Some((index, s));
        }
        if let Some((_, s)) = current.as_mut() {
            write_tag(s, kind, ts, &body);
        }
    }
    segments.extend(current.map(|(_, s)| s));
    Ok(segments)
}

/// The tail for an endpoint that reached the end, or none (logged).
pub(crate) fn tail_for(
    alias: &str,
    last_chunk: &Option<Arc<Vec<u8>>>,
    keyframe_chunk: &Option<Arc<Vec<u8>>>,
) -> Vec<Vec<u8>> {
    let built = match (last_chunk, keyframe_chunk) {
        (Some(last), Some(key)) => freeze_tail(last, key, TAIL_HOLD),
        (None, _) => Err(NoTail::NoChunk),
        (Some(_), None) => Err(NoTail::NoKeyframe),
    };
    match built {
        Ok(segments) => segments,
        Err(reason) => {
            tracing::warn!(
                alias,
                ?reason,
                "source ended: no freeze tail, YouTube may drop the last seconds (#424)"
            );
            Vec::new()
        }
    }
}

/// How a held tail ended.
pub(crate) enum TailOutcome {
    /// Every segment was pushed (or a push failed: the publish ends anyway).
    Held,
    /// The source came back during the tail.
    Resumed(PrefetchedChunk),
    /// A stop, or the chunk channel closed.
    Stop,
}

impl TailOutcome {
    pub(crate) fn label(&self) -> &'static str {
        match self {
            TailOutcome::Held => "held",
            TailOutcome::Resumed(_) => "resumed",
            TailOutcome::Stop => "stop",
        }
    }
}

/// Push the tail segment by segment on the live publish. A chunk or a stop
/// is checked between segments, never in the middle of one, so the RTMP
/// stream is never cut inside a message before the publish ends.
pub(crate) async fn hold_tail<P: Pushable>(
    pusher: &mut P,
    segments: &[Vec<u8>],
    rx: &mut mpsc::Receiver<PrefetchedChunk>,
    stop_rx: &watch::Receiver<bool>,
    alias: &str,
) -> TailOutcome {
    for segment in segments {
        if *stop_rx.borrow() {
            return TailOutcome::Stop;
        }
        match rx.try_recv() {
            Ok(chunk) => return TailOutcome::Resumed(chunk),
            Err(mpsc::error::TryRecvError::Disconnected) => return TailOutcome::Stop,
            Err(mpsc::error::TryRecvError::Empty) => {}
        }
        if let Err(e) = pusher.push_flv_bytes(segment).await {
            tracing::warn!(alias, "freeze tail push failed: {e}; ending the publish");
            return TailOutcome::Held;
        }
    }
    TailOutcome::Held
}

#[cfg(test)]
#[path = "source_end_tail_tests.rs"]
mod tests;
