//! #424: the freeze tail — frozen keyframe + AAC silence after the end of the
//! source, paced as real-time content, pushed segment by segment.

use super::*;
use rs_rtmp_push::PushError;
use std::sync::Mutex as StdMutex;

const AVC_SEQ: &[u8] = &[0x17, 0, 0, 0, 0, 1, 0x64, 0, 0x28, 0xff];
const AVC_SEQ_OTHER: &[u8] = &[0x17, 0, 0, 0, 0, 1, 0x4d, 0, 0x1f, 0xff];
/// AAC-LC (2), 48 kHz (index 3), stereo.
const AAC_LC_48K_STEREO: [u8; 2] = [0x11, 0x90];
/// AAC-LC, 48 kHz, mono.
const AAC_LC_48K_MONO: [u8; 2] = [0x11, 0x88];
/// AAC-LC, 44.1 kHz (index 4), stereo.
const AAC_LC_44K_STEREO: [u8; 2] = [0x12, 0x10];
/// HE-AAC (5), 48 kHz, stereo.
const HE_AAC_48K_STEREO: [u8; 2] = [0x29, 0x90];
/// AAC-LC, 48 kHz, 6 channels.
const AAC_LC_48K_SIX: [u8; 2] = [0x11, 0xb0];

fn flv(tags: &[(u8, u32, Vec<u8>)]) -> Vec<u8> {
    let mut out = FLV_HEADER.to_vec();
    for (kind, ts, body) in tags {
        write_tag(&mut out, *kind, u64::from(*ts), body);
    }
    out
}

fn keyframe(len: usize) -> Vec<u8> {
    let mut b = vec![0x17, 1, 0, 0, 0x21];
    b.resize(len, 0xab);
    b
}

/// One 1 s chunk the way the chunker writes it: both sequence headers at
/// ts 0, then 30 fps video (keyframe first when `with_key`) and 48 kHz AAC
/// frames, source timestamps from `base`.
fn chunk(base: u32, config: [u8; 2], with_key: bool, avc_seq: &[u8]) -> Vec<u8> {
    let mut tags = vec![
        (VIDEO, 0, avc_seq.to_vec()),
        (AUDIO, 0, vec![0xaf, 0, config[0], config[1]]),
    ];
    let mut media = Vec::new();
    for i in 0..30u32 {
        let body = if i == 0 && with_key {
            keyframe(20_000)
        } else {
            let mut p = vec![0x27, 1, 0, 0, 0];
            p.resize(2_000, 0xcd);
            p
        };
        media.push((VIDEO, base + i * 33, body));
    }
    for i in 0..47u32 {
        media.push((
            AUDIO,
            base + i * 1024 * 1000 / 48_000,
            vec![0xaf, 1, 0x55, 0x66],
        ));
    }
    media.sort_by_key(|t| (t.1, t.0));
    tags.extend(media);
    flv(&tags)
}

fn last_media_ts(bytes: &[u8]) -> u64 {
    parse(bytes)
        .iter()
        .filter(|t| is_media(t))
        .map(|t| u64::from(t.ts))
        .max()
        .unwrap()
}

struct Owned {
    kind: u8,
    ts: u64,
    body: Vec<u8>,
}

fn media_of(segments: &[Vec<u8>]) -> Vec<Owned> {
    segments
        .iter()
        .flat_map(|s| {
            parse(s)
                .into_iter()
                .filter(|t| is_media(t))
                .map(|t| Owned {
                    kind: t.kind,
                    ts: u64::from(t.ts),
                    body: t.body.to_vec(),
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn the_tail_freezes_the_last_keyframe_with_silence_for_the_whole_hold() {
    let last = chunk(5_000, AAC_LC_48K_STEREO, true, AVC_SEQ);
    let end = last_media_ts(&last);
    let segments = freeze_tail(&last, &last, TAIL_HOLD).expect("a tail");
    let media = media_of(&segments);

    let video: Vec<&Owned> = media.iter().filter(|t| t.kind == VIDEO).collect();
    let audio: Vec<&Owned> = media.iter().filter(|t| t.kind == AUDIO).collect();
    assert!(!video.is_empty() && !audio.is_empty());

    let mut frozen = keyframe(20_000);
    frozen[2..5].fill(0);
    assert!(
        video.iter().all(|t| t.body == frozen),
        "every picture is the frozen keyframe"
    );
    let mut silence = vec![0xaf, 1];
    silence.extend_from_slice(SILENT_STEREO);
    assert!(
        audio.iter().all(|t| t.body == silence),
        "every audio frame is stereo silence"
    );

    let hold = TAIL_HOLD.as_millis() as u64;
    assert!(media.iter().all(|t| t.ts > end && t.ts < end + hold));
    let audio_end = audio.last().unwrap().ts;
    assert!(
        audio_end + 22 >= end + hold,
        "silence runs to the end of the hold: {audio_end}"
    );
    // 20 s of 1024-sample frames at 48 kHz.
    let expected = (hold as f64 / (1024.0 * 1000.0 / 48_000.0)).round() as i64;
    assert!(
        (audio.len() as i64 - expected).abs() <= 2,
        "{} audio frames",
        audio.len()
    );
    for track in [&video, &audio] {
        assert!(
            track.windows(2).all(|w| w[0].ts < w[1].ts),
            "monotonic per track"
        );
    }
}

#[test]
fn tags_round_trip_every_timestamp_byte() {
    let bytes = flv(&[
        (VIDEO, 0x0012_3456, vec![0x17, 1, 0, 0, 0]),
        (AUDIO, 0x7f65_4321, vec![0xaf, 1, 9]),
    ]);
    let tags = parse(&bytes);
    let got: Vec<(u8, u32, &[u8])> = tags.iter().map(|t| (t.kind, t.ts, t.body)).collect();
    assert_eq!(
        got,
        vec![
            (VIDEO, 0x0012_3456, &[0x17, 1, 0, 0, 0][..]),
            (AUDIO, 0x7f65_4321, &[0xaf, 1, 9][..]),
        ]
    );
    assert!(parse(b"FLV").is_empty(), "a short header is no FLV");
    assert!(parse(b"FLV\x01\x05\0\0\0").is_empty());
    assert!(!has_keyframe(b"FLV"));
}

#[test]
fn sequence_headers_are_told_apart_from_media() {
    let tag = |kind, body: &'static [u8]| Tag { kind, ts: 0, body };
    assert!(is_avc_seq(&tag(VIDEO, &[0x17, 0])));
    assert!(!is_avc_seq(&tag(VIDEO, &[0x17, 1])), "a keyframe");
    assert!(!is_avc_seq(&tag(VIDEO, &[0x12, 0])), "not AVC");
    assert!(!is_avc_seq(&tag(AUDIO, &[0x07, 0])), "not video");
    assert!(is_aac_seq(&tag(AUDIO, &[0xaf, 0, 0x11, 0x90])));
    assert!(!is_aac_seq(&tag(AUDIO, &[0xaf, 1, 0x11, 0x90])), "a frame");
    assert!(!is_aac_seq(&tag(AUDIO, &[0x2f, 0, 0x11, 0x90])), "not AAC");
    assert!(
        !is_aac_seq(&tag(VIDEO, &[0xaf, 0, 0x11, 0x90])),
        "not audio"
    );
}

/// The exact tags of a short tail: video goes on after the last tag of any
/// kind, audio after the last AUDIO tag, and nothing reaches the hold's end.
#[test]
fn a_short_tail_has_exactly_these_tags() {
    let mut inter = vec![0x27, 1, 0, 0, 0];
    inter.resize(50, 0xcd);
    let last = flv(&[
        (VIDEO, 0, AVC_SEQ.to_vec()),
        (AUDIO, 0, vec![0xaf, 0, 0x11, 0x90]),
        (VIDEO, 1000, keyframe(100)),
        (AUDIO, 1000, vec![0xaf, 1, 0x55]),
        (VIDEO, 1010, inter),
    ]);
    // Audio frames every 21.33 ms from 1000: 1021, 1043, then 1064 is the
    // hold's end itself and is left out.
    let segments = freeze_tail(&last, &last, Duration::from_millis(54)).unwrap();
    assert_eq!(segments.len(), 1);
    let got: Vec<(u8, u64)> = media_of(&segments).iter().map(|t| (t.kind, t.ts)).collect();
    assert_eq!(got, vec![(AUDIO, 1021), (AUDIO, 1043), (VIDEO, 1043)]);
}

#[test]
fn each_segment_covers_one_second_of_the_tail() {
    let last = chunk(5_000, AAC_LC_48K_STEREO, true, AVC_SEQ);
    let end = last_media_ts(&last);
    let segments = freeze_tail(&last, &last, TAIL_HOLD).unwrap();
    assert_eq!(segments.len(), 20);
    for (k, s) in segments.iter().enumerate() {
        let from = end + 1000 * k as u64;
        let media = media_of(std::slice::from_ref(s));
        assert!(!media.is_empty());
        assert!(
            media.iter().all(|t| (from..from + 1000).contains(&t.ts)),
            "segment {k} holds only [{from}, {})",
            from + 1000
        );
    }
}

/// A chunk whose audio stops more than one AAC frame before its video (an
/// encoder that ends the audio track early): the silence starts before the
/// last picture and belongs to the first segment.
fn chunk_with_early_audio_end(base: u32) -> Vec<u8> {
    let mut tags = vec![
        (VIDEO, 0, AVC_SEQ.to_vec()),
        (
            AUDIO,
            0,
            vec![0xaf, 0, AAC_LC_48K_STEREO[0], AAC_LC_48K_STEREO[1]],
        ),
    ];
    let mut media = vec![(VIDEO, base, keyframe(20_000))];
    for i in 1..31u32 {
        let mut p = vec![0x27, 1, 0, 0, 0];
        p.resize(2_000, 0xcd);
        media.push((VIDEO, base + i * 33, p));
    }
    for i in 0..44u32 {
        media.push((
            AUDIO,
            base + i * 1024 * 1000 / 48_000,
            vec![0xaf, 1, 0x55, 0x66],
        ));
    }
    media.sort_by_key(|t| (t.1, t.0));
    tags.extend(media);
    flv(&tags)
}

#[test]
fn silence_that_starts_before_the_last_picture_goes_into_the_first_segment() {
    let last = chunk_with_early_audio_end(5_000);
    let end = last_media_ts(&last);
    assert_eq!(end, 5_990, "fixture: video ends at 5990");
    let segments = freeze_tail(&last, &last, TAIL_HOLD).unwrap();
    assert_eq!(segments.len(), 20);
    let first = media_of(&segments[..1]);
    let audio: Vec<u64> = first
        .iter()
        .filter(|t| t.kind == AUDIO)
        .map(|t| t.ts)
        .collect();
    assert_eq!(
        audio[0],
        5_917 + 21,
        "silence continues the audio track (last frame 5917)"
    );
    assert!(audio[0] < end);
    let all = media_of(&segments);
    for kind in [AUDIO, VIDEO] {
        let ts: Vec<u64> = all
            .iter()
            .filter(|t| t.kind == kind)
            .map(|t| t.ts)
            .collect();
        assert!(
            ts.windows(2).all(|w| w[0] < w[1]),
            "track {kind} stays monotonic"
        );
    }
}

#[test]
fn every_segment_is_a_chunk_the_pusher_takes() {
    let last = chunk(5_000, AAC_LC_48K_STEREO, true, AVC_SEQ);
    let segments = freeze_tail(&last, &last, TAIL_HOLD).unwrap();
    assert!(
        segments.len() >= 19,
        "about one segment per second: {}",
        segments.len()
    );
    for s in &segments {
        assert_eq!(&s[..13], FLV_HEADER);
        let tags = parse(s);
        assert!(is_avc_seq(&tags[0]) && tags[0].ts == 0 && tags[0].body == AVC_SEQ);
        assert!(is_aac_seq(&tags[1]) && tags[1].ts == 0);
        let ts: Vec<u32> = tags[2..].iter().map(|t| t.ts).collect();
        assert!(!ts.is_empty());
        assert!(
            ts.last().unwrap() - ts[0] < SEGMENT_MS as u32,
            "one segment spans < 1 s"
        );
    }
}

#[test]
fn the_tail_never_sends_more_than_the_live_bitrate() {
    let last = chunk(5_000, AAC_LC_48K_STEREO, true, AVC_SEQ);
    let segments = freeze_tail(&last, &last, TAIL_HOLD).unwrap();
    let video_bytes: usize = media_of(&segments)
        .iter()
        .filter(|t| t.kind == VIDEO)
        .map(|t| t.body.len())
        .sum();
    // At most the live rate over the hold, plus the one frame the window's
    // edge may round in.
    let live_bps = last.len() as f64 / (media_span_ms(&parse(&last)) as f64 / 1000.0);
    let budget = live_bps * TAIL_HOLD.as_secs_f64() + 20_000.0;
    assert!(
        video_bytes as f64 <= budget,
        "tail {video_bytes} B > {budget} B"
    );
    assert!(
        video_bytes as f64 >= live_bps * TAIL_HOLD.as_secs_f64() * 0.9,
        "no needless thinning either: {video_bytes} B"
    );

    assert_eq!(video_step_ms(1_000_000, 100, 1000), MIN_VIDEO_STEP_MS);
    assert_eq!(video_step_ms(1_000, 900_000, 1000), MAX_VIDEO_STEP_MS);
    assert_eq!(video_step_ms(100_000, 20_000, 1000), 200);
    assert_eq!(
        video_step_ms(100_000, 20_000, 0),
        200,
        "an unknown span counts as 1 s"
    );
    assert_eq!(video_step_ms(0, 20_000, 1000), MAX_VIDEO_STEP_MS);
}

#[test]
fn the_keyframe_may_come_from_an_earlier_chunk() {
    let key_chunk = chunk(4_000, AAC_LC_48K_STEREO, true, AVC_SEQ);
    let last = chunk(5_000, AAC_LC_48K_STEREO, false, AVC_SEQ);
    assert!(has_keyframe(&key_chunk));
    assert!(!has_keyframe(&last));
    let media = media_of(&freeze_tail(&last, &key_chunk, TAIL_HOLD).unwrap());
    let end = last_media_ts(&last);
    assert!(
        media.iter().all(|t| t.ts > end),
        "the tail starts after the LAST chunk"
    );
    assert!(media.iter().any(|t| t.kind == VIDEO));
}

#[test]
fn mono_and_44k_get_their_own_silence_and_frame_spacing() {
    let mono = chunk(0, AAC_LC_48K_MONO, true, AVC_SEQ);
    let media = media_of(&freeze_tail(&mono, &mono, TAIL_HOLD).unwrap());
    let audio: Vec<&Owned> = media.iter().filter(|t| t.kind == AUDIO).collect();
    assert_eq!(&audio[0].body[2..], SILENT_MONO);

    let c44 = chunk(0, AAC_LC_44K_STEREO, true, AVC_SEQ);
    let media = media_of(&freeze_tail(&c44, &c44, TAIL_HOLD).unwrap());
    let audio: Vec<u64> = media
        .iter()
        .filter(|t| t.kind == AUDIO)
        .map(|t| t.ts)
        .collect();
    let mean = (audio.last().unwrap() - audio[0]) as f64 / (audio.len() - 1) as f64;
    assert!((mean - 23.22).abs() < 0.05, "44.1 kHz frame spacing {mean}");
}

#[test]
fn no_tail_without_a_safe_picture_or_known_silence() {
    let ok = chunk(0, AAC_LC_48K_STEREO, true, AVC_SEQ);
    let no_key = chunk(0, AAC_LC_48K_STEREO, false, AVC_SEQ);
    assert_eq!(
        freeze_tail(&no_key, &no_key, TAIL_HOLD),
        Err(NoTail::NoKeyframe)
    );
    let republished = chunk(0, AAC_LC_48K_STEREO, false, AVC_SEQ_OTHER);
    assert_eq!(
        freeze_tail(&republished, &ok, TAIL_HOLD),
        Err(NoTail::CodecChanged)
    );
    let he = chunk(0, HE_AAC_48K_STEREO, true, AVC_SEQ);
    assert_eq!(
        freeze_tail(&he, &he, TAIL_HOLD),
        Err(NoTail::UnsupportedAudio {
            object_type: 5,
            channels: 2
        })
    );
    let six = chunk(0, AAC_LC_48K_SIX, true, AVC_SEQ);
    assert_eq!(
        freeze_tail(&six, &six, TAIL_HOLD),
        Err(NoTail::UnsupportedAudio {
            object_type: 2,
            channels: 6
        })
    );
    let video_only = flv(&[(VIDEO, 0, AVC_SEQ.to_vec()), (VIDEO, 10, keyframe(100))]);
    assert_eq!(
        freeze_tail(&video_only, &ok, TAIL_HOLD),
        Err(NoTail::NoAudioConfig)
    );
    assert_eq!(
        freeze_tail(b"not flv", &ok, TAIL_HOLD),
        Err(NoTail::NoAudioConfig)
    );

    let some = |b: &Vec<u8>| Some(Arc::new(b.clone()));
    assert!(tail_for("ep", &None, &some(&ok)).is_empty());
    assert!(tail_for("ep", &some(&ok), &None).is_empty());
    assert!(!tail_for("ep", &some(&ok), &some(&ok)).is_empty());
}

/// Real encoder output through the tail, decoded by ffmpeg: no decode
/// error, the frozen frames and the silent frames are counted as frames,
/// and everything after the real end is digital silence.
#[test]
fn ffmpeg_decodes_the_tail_as_frozen_picture_and_silence() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.flv");
    let status = std::process::Command::new("ffmpeg")
        .args(["-loglevel", "error", "-y"])
        .args(["-f", "lavfi", "-i", "testsrc2=size=320x240:rate=30"])
        .args(["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000"])
        .args([
            "-t", "2", "-ac", "2", "-c:v", "libx264", "-g", "60", "-bf", "0",
        ])
        .args(["-c:a", "aac", "-b:a", "128k", "-f", "flv"])
        .arg(&src)
        .status()
        .expect("ffmpeg must be installed (CI installs it)");
    assert!(status.success());
    let source = std::fs::read(&src).unwrap();
    assert!(
        last_media_ts(&source) < 3_000,
        "the source parses to its 2 s of timestamps"
    );
    let hold = Duration::from_secs(3);
    let segments = freeze_tail(&source, &source, hold).expect("a tail for real AAC-LC + AVC");

    // One FLV: the source, then the tail's media tags (its per-segment
    // sequence headers are what the pusher de-duplicates on the wire).
    let mut joined = source.clone();
    for t in media_of(&segments) {
        write_tag(&mut joined, t.kind, t.ts, &t.body);
    }
    let out = dir.path().join("tail.flv");
    std::fs::write(&out, &joined).unwrap();

    let decode = std::process::Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&out)
        .args(["-f", "null", "-"])
        .output()
        .unwrap();
    assert!(decode.status.success());
    assert_eq!(
        String::from_utf8_lossy(&decode.stderr).trim(),
        "",
        "decode errors"
    );

    let end_s = last_media_ts(&source) as f64 / 1000.0;
    let volume = std::process::Command::new("ffmpeg")
        .args(["-v", "info", "-ss", &format!("{:.3}", end_s + 0.2), "-i"])
        .arg(&out)
        .args(["-vn", "-af", "volumedetect", "-f", "null", "-"])
        .output()
        .unwrap();
    let log = String::from_utf8_lossy(&volume.stderr);
    let max_db: f64 = log
        .lines()
        .find_map(|l| l.split("max_volume: ").nth(1))
        .and_then(|v| v.trim_end_matches(" dB").trim().parse().ok())
        .unwrap_or_else(|| panic!("no max_volume in: {log}"));
    assert!(
        max_db <= -90.0,
        "after the end the audio must be silence: {max_db} dB"
    );

    let frames = |stream: &str, path: &std::path::Path| -> u64 {
        let o = std::process::Command::new("ffprobe")
            .args(["-v", "error", "-count_frames", "-select_streams", stream])
            .args(["-show_entries", "stream=nb_read_frames", "-of", "csv=p=0"])
            .arg(path)
            .output()
            .unwrap();
        String::from_utf8_lossy(&o.stdout).trim().parse().unwrap()
    };
    assert!(
        frames("v", &out) > frames("v", &src),
        "the frozen frames decode"
    );
    let added_audio = frames("a", &out) - frames("a", &src);
    assert!(
        (138..=144).contains(&added_audio),
        "3 s of 48 kHz frames: {added_audio}"
    );
}

/// Records every pushed segment; optionally fails.
struct Recording {
    pushed: Arc<StdMutex<Vec<Vec<u8>>>>,
    fail: bool,
}

impl Pushable for Recording {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        self.pushed.lock().unwrap().push(data.to_vec());
        if self.fail {
            return Err(PushError::Timeout);
        }
        Ok(())
    }
    async fn close(&mut self) {}
    fn reconnect_count(&self) -> u32 {
        0
    }
    fn av_skew_ms(&self) -> i64 {
        0
    }
    async fn finish(&mut self) {}
    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

fn recording(fail: bool) -> (Recording, Arc<StdMutex<Vec<Vec<u8>>>>) {
    let pushed = Arc::new(StdMutex::new(Vec::new()));
    (
        Recording {
            pushed: Arc::clone(&pushed),
            fail,
        },
        pushed,
    )
}

fn segs() -> Vec<Vec<u8>> {
    (0..3u8).map(|i| vec![i]).collect()
}

#[tokio::test]
async fn every_segment_is_pushed_in_order() {
    assert_eq!(TailOutcome::Held.label(), "held");
    assert_eq!(TailOutcome::Stop.label(), "stop");
    let (mut p, pushed) = recording(false);
    let (_tx, mut rx) = mpsc::channel(1);
    let (_stop_tx, stop_rx) = watch::channel(false);
    let out = hold_tail(&mut p, &segs(), &mut rx, &stop_rx, "ep").await;
    assert!(matches!(out, TailOutcome::Held));
    assert_eq!(*pushed.lock().unwrap(), segs());
}

#[tokio::test]
async fn a_returning_source_cuts_the_tail_at_a_segment_boundary() {
    let (mut p, pushed) = recording(false);
    let (tx, mut rx) = mpsc::channel(1);
    let (_stop_tx, stop_rx) = watch::channel(false);
    tx.send(PrefetchedChunk {
        chunk_id: 9,
        data: vec![],
        duration_ms: 1000,
    })
    .await
    .unwrap();
    let out = hold_tail(&mut p, &segs(), &mut rx, &stop_rx, "ep").await;
    assert_eq!(out.label(), "resumed");
    assert!(matches!(out, TailOutcome::Resumed(c) if c.chunk_id == 9));
    assert!(pushed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_stop_or_a_closed_channel_ends_the_tail() {
    let (mut p, pushed) = recording(false);
    let (_tx, mut rx) = mpsc::channel(1);
    let (_stop_tx, stop_rx) = watch::channel(true);
    let out = hold_tail(&mut p, &segs(), &mut rx, &stop_rx, "ep").await;
    assert!(matches!(out, TailOutcome::Stop));

    let (tx, mut rx) = mpsc::channel::<PrefetchedChunk>(1);
    drop(tx);
    let (_stop_tx, stop_rx) = watch::channel(false);
    let out = hold_tail(&mut p, &segs(), &mut rx, &stop_rx, "ep").await;
    assert!(matches!(out, TailOutcome::Stop));
    assert!(pushed.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_failed_push_ends_the_tail_without_retrying() {
    let (mut p, pushed) = recording(true);
    let (_tx, mut rx) = mpsc::channel(1);
    let (_stop_tx, stop_rx) = watch::channel(false);
    let out = hold_tail(&mut p, &segs(), &mut rx, &stop_rx, "ep").await;
    assert!(matches!(out, TailOutcome::Held));
    assert_eq!(pushed.lock().unwrap().len(), 1);
}
