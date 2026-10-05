//! Unit tests for `pusher.rs`. Loaded via `#[cfg(test)] #[path = "pusher_tests.rs"] mod tests;`
//! to keep the production file under the 1000-line CI gate.
use super::*;

/// Test-only setter so unit tests can prove the getter reads from state.
/// Without this, the `RtmpPusher::last_output_ts_ms -> 0` mutation would
/// only be killed by integration tests (which mutation testing skips).
impl RtmpPusher {
    pub(crate) fn set_last_output_ts_ms_for_test(&mut self, v: u64) {
        self.state.last_output_ts_ms = v;
    }
}

#[test]
fn last_output_ts_ms_reads_state_field() {
    let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    assert_eq!(p.last_output_ts_ms(), 0);
    p.set_last_output_ts_ms_for_test(12_345);
    assert_eq!(p.last_output_ts_ms(), 12_345);
    p.set_last_output_ts_ms_for_test(u64::MAX);
    assert_eq!(p.last_output_ts_ms(), u64::MAX);
}

#[test]
fn reconnect_count_starts_zero() {
    let p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    assert_eq!(p.reconnect_count(), 0);
}

#[test]
fn url_returns_constructor_value() {
    let p = RtmpPusher::new(
        "rtmp://example.com/live/key".into(),
        PusherConfig::default(),
    );
    assert_eq!(p.url(), "rtmp://example.com/live/key");
}

/// Per-chunk pacing math (issue #103, run 25119429314): when wall-clock
/// is BEHIND the cumulative output timestamp, the chunk-end sleep
/// equals `last_output_ts_ms - wall_elapsed`. Uses `tokio::time::pause`
/// so the test is deterministic.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn per_chunk_pacing_sleeps_when_ahead_of_wall_clock() {
    let anchor = Instant::now();
    // Pretend we just sent a chunk worth 2000 ms of media…
    let target_ms: u64 = 2_000;
    // …in 500 ms of wall time (very fast TCP writes).
    tokio::time::advance(Duration::from_millis(500)).await;
    let actual_ms = anchor.elapsed().as_millis() as u64;
    assert_eq!(actual_ms, 500, "wall elapsed must be 500 ms");
    assert!(actual_ms < target_ms);
    let sleep_ms = target_ms - actual_ms;
    assert_eq!(
        sleep_ms, 1_500,
        "per-chunk pacing must sleep target - wall = 1500 ms"
    );
}

/// When wall-clock has already overrun the cumulative output timestamp
/// (cache overshoot during init, or a slow first chunk), pacing must
/// NOT sleep — the pusher needs to drain at TCP-write speed.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn per_chunk_pacing_no_sleep_when_behind() {
    let anchor = Instant::now();
    let target_ms: u64 = 1_000;
    tokio::time::advance(Duration::from_millis(5_000)).await;
    let actual_ms = anchor.elapsed().as_millis() as u64;
    assert!(
        actual_ms >= target_ms,
        "wall elapsed (5000) >= target (1000) must skip sleep"
    );
}

#[test]
fn pacing_anchor_starts_none_and_persists_after_first_set() {
    let mut state = PusherState::default();
    assert!(state.pacing_anchor.is_none());
    let anchor = *state.pacing_anchor.get_or_insert_with(Instant::now);
    assert!(state.pacing_anchor.is_some());
    // get_or_insert_with on an Option<Instant> is idempotent: a second
    // call must NOT overwrite the anchor (otherwise pacing math drifts
    // back to per-chunk wall-clock instead of cumulative).
    let anchor2 = *state.pacing_anchor.get_or_insert_with(Instant::now);
    assert_eq!(anchor, anchor2, "anchor must be stable after first set");
}

#[test]
fn seq_header_dedup_flags_default_false_and_independent() {
    // Regression for #103 cache-growth investigation: AVC and AAC
    // sequence-header flags must start FALSE so the FIRST seq header
    // of each codec is forwarded to the RTMP server (without it the
    // receiver can't decode subsequent tags). Once flipped, the SECOND
    // identical seq header is suppressed; the chunker re-emits it in
    // every S3 chunk and re-sending throttled YouTube ingestion.
    let state = PusherState::default();
    assert!(!state.avc_seq_header_sent);
    assert!(!state.aac_seq_header_sent);
}

// --- chunk_pacing_sleep_ms (issue #171 chunk-end rate cap) ---

#[test]
fn chunk_pacing_steady_state_no_extra_sleep() {
    // Per-tag pacing already paced at real-time → actual_wall ≈
    // chunk_media. target_wall = 2000*100/120 = 1666 < actual 2000
    // → 0 sleep. Steady-state path unaffected.
    assert_eq!(chunk_pacing_sleep_ms(2000, 2000, 120), 0);
}

#[test]
fn chunk_pacing_catchup_burst_is_bounded() {
    // Catch-up: per-tag pacing skipped, all 2000 ms media pushed in
    // 50 ms wallclock. Cap at 1.2× real-time → target_wall = 1666 ms,
    // sleep 1616 ms. Net: chunk push takes 1666 ms wallclock, push
    // rate = 2000/1666 = 1.2× real-time exactly.
    assert_eq!(chunk_pacing_sleep_ms(2000, 50, 120), 1616);
}

#[test]
fn chunk_pacing_disabled_at_zero() {
    // Zero factor disables the cap entirely (regression escape
    // hatch). Returns 0 regardless of inputs.
    assert_eq!(chunk_pacing_sleep_ms(2000, 50, 0), 0);
}

#[test]
fn chunk_pacing_factor_100_is_strict_real_time() {
    // 100% = exactly real-time. burst 50ms wallclock for 2000ms
    // media → sleep 1950 to bring total to 2000ms.
    assert_eq!(chunk_pacing_sleep_ms(2000, 50, 100), 1950);
}

#[test]
fn chunk_pacing_factor_500_aggressive_burst() {
    // 5x burst allowed: target_wall = 2000/5 = 400, sleep 350 from
    // actual 50. Used to verify factor scales correctly.
    assert_eq!(chunk_pacing_sleep_ms(2000, 50, 500), 350);
}

#[test]
fn chunk_pacing_actual_at_target_no_sleep() {
    // Boundary: actual_wall == target_wall → sleep 0 (kills off-by-one
    // mutant flipping <= to <).
    assert_eq!(chunk_pacing_sleep_ms(2400, 2000, 120), 0);
}

#[test]
fn chunk_pacing_empty_chunk_no_sleep() {
    // Edge case: chunk with no media (e.g. just seq headers).
    // chunk_media_ms = 0 → target_wall = 0 → sleep 0.
    assert_eq!(chunk_pacing_sleep_ms(0, 0, 120), 0);
    assert_eq!(chunk_pacing_sleep_ms(0, 100, 120), 0);
}

// --- local window / robust pin (#367 shared-origin pin) ---

fn tag(tag_type: u8, timestamp_ms: u32, body: &'static [u8]) -> crate::flv::FlvTag<'static> {
    crate::flv::FlvTag {
        tag_type,
        timestamp_ms,
        body,
    }
}

/// Media tags at the given ts (alternating video / audio), no headers.
fn media(ts: &[u32]) -> Vec<crate::flv::FlvTag<'static>> {
    use crate::flv::{FLV_TAG_AUDIO, FLV_TAG_VIDEO};
    ts.iter()
        .enumerate()
        .map(|(k, &t)| {
            if k % 2 == 0 {
                tag(FLV_TAG_VIDEO, t, &[0x27, 0x01])
            } else {
                tag(FLV_TAG_AUDIO, t, &[0xAF, 0x01])
            }
        })
        .collect()
}

#[test]
fn local_median_ignores_headers_and_script_tags() {
    use crate::flv::{FLV_TAG_AUDIO, FLV_TAG_SCRIPT, FLV_TAG_VIDEO};
    let tags = [
        tag(FLV_TAG_SCRIPT, 0, &[0x02, 0x00]),
        tag(FLV_TAG_VIDEO, 0, &[0x17, 0x00]), // AVC seq header
        tag(FLV_TAG_AUDIO, 0, &[0xAF, 0x00]), // AAC seq header
        tag(FLV_TAG_VIDEO, 1_000, &[0x17, 0x01]),
        tag(FLV_TAG_AUDIO, 990, &[0xAF, 0x01]),
        tag(FLV_TAG_VIDEO, 1_040, &[0x27, 0x01]),
    ];
    assert_eq!(local_median(&tags, 0), Some(1_000));
    assert_eq!(local_median(&tags, 5), Some(1_040));
    assert_eq!(
        local_median(&tags[..3], 0),
        None,
        "no media tag -> no median"
    );
}

#[test]
fn local_median_is_robust_to_one_glitch() {
    let tags = media(&[600_000, 5, 600_020, 600_040, 600_060]);
    assert_eq!(local_median(&tags, 0), Some(600_020));
}

#[test]
fn robust_pin_ignores_a_low_glitch_and_keeps_interleave() {
    // 599_990: audio interleaved slightly before the keyframe -> kept;
    // 5: corrupt -> excluded from the pin.
    let tags = media(&[600_000, 599_990, 600_040, 5, 600_080, 600_060]);
    assert_eq!(robust_pin(&tags, 0), Some(599_990));
}

#[test]
fn robust_pin_of_a_two_cluster_chunk_follows_the_local_cluster() {
    // A genuine 39 s gap inside the chunk: the pin at the head belongs to
    // the head's cluster, never to the later one.
    let mut ts: Vec<u32> = (0..10).map(|k| k * 20).collect();
    ts.extend((0..10).map(|k| 40_000 + k * 20));
    let tags = media(&ts);
    assert_eq!(robust_pin(&tags, 0), Some(0));
    assert_eq!(robust_pin(&tags, 10), Some(40_000));
}

// --- #367 push-side absolute A/V invariant guard ---

/// Design test 5 (push): a constructed violation (the wire relation 0
/// while the content relation is +700 ms, the per-track-origin bug) is
/// reported ONCE through the pusher's chunk-end evaluation and queued for
/// the consumer to audit.
#[test]
fn pusher_reports_a_constructed_wire_invariant_violation_once() {
    use crate::av_invariant::{AvInvariantEvent, AvInvariantViolation};
    let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    p.av_guard.observe_video(1_000, 5_000);
    p.av_guard.observe_audio(1_700, 5_000);
    p.evaluate_av_invariant();
    p.evaluate_av_invariant(); // still violated: no second event
    assert_eq!(
        p.take_av_invariant_events(),
        vec![AvInvariantEvent::Violated(AvInvariantViolation {
            a_rel_ms: 3_300,
            v_rel_ms: 4_000,
            delta_ms: -700,
        })]
    );
    assert!(
        p.take_av_invariant_events().is_empty(),
        "taking the events drains the queue"
    );
    assert_eq!(p.av_invariant_violation_count(), 1);
}

// --- track_input_ts (#367 outlier vs new timeline) ---

fn pusher_at(prev_audio: u32, prev_video: u32) -> RtmpPusher {
    let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    p.state.last_audio_xiu_ts = Some(prev_audio);
    p.state.last_video_xiu_ts = Some(prev_video);
    p.state.origin_ts = Some(0);
    p
}

/// A single BACKWARD-glitched tag the rest of the chunk does not follow
/// is clamped: no re-anchor, the mapping and the trackers stay put.
#[test]
fn isolated_backward_outlier_keeps_the_mapping() {
    let mut p = pusher_at(600_000, 600_010);
    let tags = media(&[5, 600_020, 600_040, 600_060]);
    assert!(p.track_input_ts(Track::Video, &tags, 0));
    assert_eq!(p.regression_reanchor_count(), 0);
    assert_eq!(p.state.origin_ts, Some(0), "mapping unchanged");
    assert_eq!(
        p.state.last_video_xiu_ts,
        Some(600_010),
        "the tracker keeps the previous ts so the next normal tag is no jump"
    );
}

/// A backward step the rest of the chunk FOLLOWS is a new timeline (new
/// chunker session): re-anchor both tracks onto a new mapping.
#[test]
fn followed_backward_step_starts_a_new_mapping() {
    let mut p = pusher_at(600_000, 600_010);
    let tags = media(&[0, 0, 40, 20]);
    assert!(!p.track_input_ts(Track::Video, &tags, 0));
    assert_eq!(p.regression_reanchor_count(), 1);
    assert!(
        p.state.origin_ts.is_none(),
        "the next tag re-pins the origin"
    );
    assert_eq!(p.state.last_video_xiu_ts, Some(0));
    assert!(
        p.state.last_audio_xiu_ts.is_none(),
        "the other track's tracker clears with the new mapping"
    );
}

/// An anomalous LAST media tag of a chunk (nothing after it to compare)
/// re-anchors, as before #367.
#[test]
fn anomalous_last_tag_reanchors() {
    let mut p = pusher_at(1_000, 1_000);
    let tags = media(&[900_000]);
    assert!(!p.track_input_ts(Track::Video, &tags, 0));
    assert_eq!(p.regression_reanchor_count(), 1);
}

/// The HEAD tag of a track in a new mapping (no tracker yet) that its
/// neighbourhood disagrees with is an outlier, not a pin source.
#[test]
fn head_tag_outlier_is_detected_without_a_tracker() {
    let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    let tags = media(&[720_020, 0, 40, 20, 80, 60]);
    assert!(p.track_input_ts(Track::Video, &tags, 0));
    assert!(p.state.last_video_xiu_ts.is_none());
    assert!(
        !p.track_input_ts(Track::Audio, &tags, 1),
        "a normal head tag maps"
    );
    assert_eq!(p.state.last_audio_xiu_ts, Some(0));
}

/// Two tags of one track with the SAME input ts are not a backward step.
#[test]
fn an_equal_ts_on_the_same_track_is_no_jump() {
    let mut p = pusher_at(1_000, 1_000);
    let tags = media(&[1_000, 1_020]);
    assert!(!p.track_input_ts(Track::Video, &tags, 0));
    assert_eq!(p.regression_reanchor_count(), 0);
    assert_eq!(p.state.origin_ts, Some(0), "mapping unchanged");
}

/// A forward step of exactly MAX_TAG_TS_JUMP_MS is still a normal step;
/// only a larger one is a jump.
#[test]
fn a_forward_step_of_exactly_the_jump_limit_is_no_jump() {
    let mut p = pusher_at(1_000, 1_000);
    let tags = media(&[1_000 + MAX_TAG_TS_JUMP_MS]);
    assert!(!p.track_input_ts(Track::Video, &tags, 0));
    assert_eq!(p.regression_reanchor_count(), 0);
    let tags = media(&[1_000 + 2 * MAX_TAG_TS_JUMP_MS + 1]);
    assert!(!p.track_input_ts(Track::Video, &tags, 0));
    assert_eq!(p.regression_reanchor_count(), 1, "one ms more is a jump");
}

/// A jumped tag is judged by the tags AFTER it, never by itself: with one
/// normal tag left in the chunk, a forward glitch is an outlier.
#[test]
fn a_jumped_tag_is_judged_by_the_tags_after_it() {
    let mut p = pusher_at(1_000, 1_000);
    // video 1_020, audio 1_040, video 900_000 (glitch), audio 1_060
    let tags = media(&[1_020, 1_040, 900_000, 1_060]);
    assert!(
        p.track_input_ts(Track::Video, &tags, 2),
        "the glitch is an outlier: the only tag after it is on the old timeline"
    );
    assert_eq!(p.regression_reanchor_count(), 0);
}

/// A clamped outlier lands on ITS OWN track's wire timeline.
#[test]
fn an_outlier_is_clamped_onto_its_own_track() {
    use crate::flv::{FLV_TAG_AUDIO, FLV_TAG_VIDEO};
    let mut p = pusher_at(600_000, 600_000);
    p.state.last_audio_output_ts_ms = 5_000;
    p.state.last_video_output_ts_ms = 7_000;
    let tags = [
        tag(FLV_TAG_AUDIO, 5, &[0xAF, 0x01]),
        tag(FLV_TAG_VIDEO, 6, &[0x27, 0x01]),
        tag(FLV_TAG_VIDEO, 600_020, &[0x27, 0x01]),
        tag(FLV_TAG_AUDIO, 600_040, &[0xAF, 0x01]),
        tag(FLV_TAG_VIDEO, 600_060, &[0x27, 0x01]),
    ];
    assert_eq!(p.map_media_tag(&tags, 0, false), 5_001, "audio outlier");
    assert_eq!(p.map_media_tag(&tags, 1, false), 7_001, "video outlier");
}

/// A fresh RTMP session starts a NEW shared mapping (#367): codec config is
/// re-sent, the base moves past everything already sent, the origin re-pins,
/// and no old-mapping guard sample is paired with a new one.
#[test]
fn a_fresh_session_starts_a_new_shared_mapping() {
    let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    p.state.avc_seq_header_sent = true;
    p.state.aac_seq_header_sent = true;
    p.state.last_audio_output_ts_ms = 9_000;
    p.state.last_video_output_ts_ms = 9_040;
    p.state.origin_ts = Some(1_000);
    p.state.last_audio_xiu_ts = Some(10_000);
    p.state.last_video_xiu_ts = Some(10_040);
    p.av_guard.observe_audio(0, 900);
    p.on_fresh_session();
    assert!(p.state.connected);
    assert!(
        !p.state.avc_seq_header_sent && !p.state.aac_seq_header_sent,
        "codec config is re-sent on every RTMP session"
    );
    assert_eq!(p.state.base_ms, 9_041);
    assert!(p.state.origin_ts.is_none());
    assert!(p.state.last_audio_xiu_ts.is_none() && p.state.last_video_xiu_ts.is_none());
    p.av_guard.observe_video(0, 0);
    assert_eq!(
        p.av_guard.check(),
        None,
        "an old-mapping sample is never paired with a new one"
    );
}

/// Codec sequence headers go out once per RTMP session, per codec (#103).
#[test]
fn codec_sequence_headers_go_out_once_per_session() {
    use crate::flv::{FLV_TAG_AUDIO, FLV_TAG_SCRIPT, FLV_TAG_VIDEO};
    let mut p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    assert!(
        !p.skip_repeated_seq_header(FLV_TAG_VIDEO, false),
        "media is never skipped"
    );
    assert!(
        !p.skip_repeated_seq_header(FLV_TAG_VIDEO, true),
        "first AVC header"
    );
    assert!(
        p.skip_repeated_seq_header(FLV_TAG_VIDEO, true),
        "repeated AVC header"
    );
    assert!(
        !p.skip_repeated_seq_header(FLV_TAG_AUDIO, true),
        "AAC is separate"
    );
    assert!(
        p.skip_repeated_seq_header(FLV_TAG_AUDIO, true),
        "repeated AAC header"
    );
    assert!(
        !p.skip_repeated_seq_header(FLV_TAG_SCRIPT, true),
        "never other tags"
    );
}

/// Per-tag pacing (#176/#178): nothing when the tag is due, the residual
/// otherwise, capped at 5 s; 2 s or more is a LONG sleep.
#[test]
fn tag_sleep_is_the_capped_residual() {
    assert_eq!(tag_sleep(1_000, 1_000), None, "due");
    assert_eq!(tag_sleep(1_001, 1_000), None, "overdue");
    let short = tag_sleep(1_000, 2_999).expect("ahead of wall");
    assert_eq!((short.raw_ms, short.sleep_ms), (1_999, 1_999));
    assert!(!short.is_long());
    let long = tag_sleep(1_000, 3_000).expect("ahead of wall");
    assert!(long.is_long(), "2 s is a LONG sleep");
    let corrupt = tag_sleep(0, 600_000).expect("far ahead");
    assert_eq!(
        (corrupt.raw_ms, corrupt.sleep_ms),
        (600_000, PACING_SLEEP_CAP_MS)
    );
}

/// `pace_tag` really waits for wall-clock (paused clock), capped.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn pace_tag_sleeps_until_the_tag_is_due() {
    let p = RtmpPusher::new("rtmp://x:1935/a/b".into(), PusherConfig::default());
    let anchor = Instant::now();
    p.pace_tag(anchor, crate::flv::FLV_TAG_VIDEO, 1_500).await;
    assert_eq!(anchor.elapsed(), Duration::from_millis(1_500));
    p.pace_tag(anchor, crate::flv::FLV_TAG_VIDEO, 1_000).await;
    assert_eq!(
        anchor.elapsed(),
        Duration::from_millis(1_500),
        "already due"
    );
    p.pace_tag(anchor, crate::flv::FLV_TAG_VIDEO, 600_000).await;
    assert_eq!(
        anchor.elapsed(),
        Duration::from_millis(1_500 + PACING_SLEEP_CAP_MS),
        "a corrupt far-future ts sleeps the cap only"
    );
}
