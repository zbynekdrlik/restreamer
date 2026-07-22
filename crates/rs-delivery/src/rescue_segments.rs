//! Pre-rendered Slovak rescue-clip segments + the ETA-bucket selection that
//! makes the viewer-facing recovery countdown genuinely LIVE on the pure-Rust
//! rescue path (#259).
//!
//! ## Why segments (and not a runtime text renderer)
//!
//! There is no ffmpeg on the delivery VPS by design — the rescue pusher can
//! only stream pre-encoded FLV bytes, it cannot composite text at runtime.
//! Before #259 the "countdown" was written to a temp file that the (long
//! since removed) ffmpeg drawtext filter was supposed to read; on the
//! pure-Rust path nothing ever read it, so viewers saw a single static
//! English clip with a dead countdown. This module replaces that with a set
//! of pre-rendered Slovak segments, one per ETA bucket, and the pusher swaps
//! WHICH segment it loops as the recovery ETA counts down — a real,
//! viewer-visible "Obnovujeme o ~…" countdown.
//!
//! ## Why swapping segments mid-session is safe (the codec landmine)
//!
//! A rescue clip is pushed on a FRESH RTMP session, and `rs_rtmp_push`
//! forwards exactly ONE AVC/AAC sequence header (codec config) per session —
//! every later segment's sequence header is skipped, so its frames decode
//! against the FIRST segment's SPS/PPS. All six segments are produced by
//! `gen_rescue_flv` with BYTE-IDENTICAL libx264/aac flags (only the drawtext
//! text differs), so their SPS/PPS are byte-identical (verified at generation
//! time: identical 798-byte parameter-set prelude across all six). Each
//! segment is a complete short clip that STARTS with an IDR keyframe, and the
//! pusher only ever swaps at a whole-segment boundary (a `push_flv_bytes`
//! call pushes an entire segment before the loop picks the next), so a swap
//! introduces no cross-segment P-frame reference. Swapping segments is
//! therefore mechanically identical to the existing production loop that
//! re-pushes ONE blob (which relies on the same backward-timestamp re-anchor)
//! — no decoder lock, no visual corruption.

use std::sync::Arc;

use crate::rust_rescue_push::RescuePushMode;

// Embedded segment assets. All produced by `gen_rescue_flv` with identical
// codec flags — see the module doc for why identical SPS/PPS is what makes
// mid-session swapping safe.
/// "Vysielanie sa o chvíľu spustí…" — initial buffer fill / stream startup.
pub const SEG_WARMUP: &[u8] = include_bytes!("../assets/rescue_warmup.flv");
/// "Prenos bol prerušený — o chvíľu pokračujeme" — outage in progress, no
/// genuine recovery under way yet. Doubles as `DEFAULT_RESCUE_FLV`.
pub const SEG_OUTAGE: &[u8] = include_bytes!("../assets/default_rescue.flv");
/// "Obnovujeme o ~2 min" — recovering, ~2 minutes of refill remaining.
pub const SEG_RECOVER_2MIN: &[u8] = include_bytes!("../assets/rescue_recover_2min.flv");
/// "Obnovujeme o ~1 min" — recovering, ~1 minute remaining.
pub const SEG_RECOVER_1MIN: &[u8] = include_bytes!("../assets/rescue_recover_1min.flv");
/// "Obnovujeme o ~30 s" — recovering, ~30 seconds remaining.
pub const SEG_RECOVER_30S: &[u8] = include_bytes!("../assets/rescue_recover_30s.flv");
/// "Obnovujeme o chvíľu" — recovering, resuming imminently.
pub const SEG_RECOVER_SOON: &[u8] = include_bytes!("../assets/rescue_recover_soon.flv");

/// Recovery-countdown ETA-bucket thresholds (seconds). The recovery ETA
/// counts down from `RESCUE_REFILL_TARGET_SECS` (120) to 0; each threshold is
/// the lower edge (inclusive) of the segment that shows above it.
const BUCKET_2MIN_SECS: u64 = 90;
const BUCKET_1MIN_SECS: u64 = 45;
const BUCKET_30S_SECS: u64 = 10;

/// Which pre-rendered rescue segment matches the current rescue state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RescueSegment {
    /// Initial buffer fill before the stream has started (warmup).
    Warmup,
    /// Outage in progress, no genuine recovery yet — static Slovak notice.
    Outage,
    /// Recovering, ~2 minutes remaining.
    Recover2Min,
    /// Recovering, ~1 minute remaining.
    Recover1Min,
    /// Recovering, ~30 seconds remaining.
    Recover30s,
    /// Recovering, resuming imminently.
    RecoverSoon,
}

impl RescueSegment {
    /// Embedded FLV bytes for this segment.
    pub fn bytes(self) -> &'static [u8] {
        match self {
            RescueSegment::Warmup => SEG_WARMUP,
            RescueSegment::Outage => SEG_OUTAGE,
            RescueSegment::Recover2Min => SEG_RECOVER_2MIN,
            RescueSegment::Recover1Min => SEG_RECOVER_1MIN,
            RescueSegment::Recover30s => SEG_RECOVER_30S,
            RescueSegment::RecoverSoon => SEG_RECOVER_SOON,
        }
    }
}

/// Choose the segment for the current rescue state.
///
/// * **Warmup mode** always shows the "stream starting" notice — the warmup
///   probe loop owns its own timing and there is no genuine "recovery ETA" to
///   count down (see `run_warmup_loop`).
/// * **Outage mode** shows the static Slovak outage notice until a GENUINE
///   refill is under way (`refilling`, per #289 — producer active AND fresh
///   chunks queued past the pre-outage live edge), then counts DOWN through
///   the ETA buckets so viewers see a live "Obnovujeme o ~…" countdown. This
///   deliberately mirrors `rust_rescue_push`'s `refilling` discriminator so
///   the viewer text never claims "recovering" on a bare `producer_active`
///   flag flap.
pub fn select_segment(mode: RescuePushMode, refilling: bool, eta_secs: u64) -> RescueSegment {
    match mode {
        RescuePushMode::Warmup => RescueSegment::Warmup,
        RescuePushMode::Outage => {
            if !refilling {
                RescueSegment::Outage
            } else if eta_secs >= BUCKET_2MIN_SECS {
                RescueSegment::Recover2Min
            } else if eta_secs >= BUCKET_1MIN_SECS {
                RescueSegment::Recover1Min
            } else if eta_secs >= BUCKET_30S_SECS {
                RescueSegment::Recover30s
            } else {
                RescueSegment::RecoverSoon
            }
        }
    }
}

/// Source of the FLV bytes pushed during a rescue loop.
#[derive(Clone)]
pub enum RescueClipSource {
    /// A single fixed blob: the operator's custom uploaded rescue video. We
    /// cannot composite a countdown onto an arbitrary operator clip without a
    /// runtime renderer, so it plays as-is — unchanged pre-#259 behavior.
    Fixed(Arc<Vec<u8>>),
    /// The embedded Slovak segment set. `pick` returns the segment matching
    /// the live rescue state each loop iteration, which is what makes the
    /// viewer-facing countdown genuinely move (#259).
    Countdown,
}

impl RescueClipSource {
    /// Bytes to push for the current rescue state. For `Countdown` this is the
    /// ETA-bucket segment; for `Fixed` it is always the same blob.
    pub fn pick(&self, mode: RescuePushMode, refilling: bool, eta_secs: u64) -> &[u8] {
        match self {
            RescueClipSource::Fixed(b) => b.as_slice(),
            RescueClipSource::Countdown => select_segment(mode, refilling, eta_secs).bytes(),
        }
    }

    /// Short human label for logs — the source kind + (for Countdown) the
    /// currently selected segment.
    pub fn describe(&self, mode: RescuePushMode, refilling: bool, eta_secs: u64) -> String {
        match self {
            RescueClipSource::Fixed(b) => format!("custom-flv ({} bytes)", b.len()),
            RescueClipSource::Countdown => {
                format!("countdown/{:?}", select_segment(mode, refilling, eta_secs))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL: &[RescueSegment] = &[
        RescueSegment::Warmup,
        RescueSegment::Outage,
        RescueSegment::Recover2Min,
        RescueSegment::Recover1Min,
        RescueSegment::Recover30s,
        RescueSegment::RecoverSoon,
    ];

    // --- Segment asset integrity (structural, matches gen_rescue_flv's 20KB
    // sanity floor; the DEFAULT_RESCUE_FLV blob has its own >50KB check in
    // rescue_default). ---

    #[test]
    fn every_segment_is_a_nontrivial_flv() {
        for seg in ALL {
            let b = seg.bytes();
            assert!(
                b.starts_with(b"FLV"),
                "{seg:?} is not an FLV (missing magic)"
            );
            assert_eq!(b[3], 0x01, "{seg:?} FLV version != 1");
            assert!(
                b.len() > 20_000,
                "{seg:?} too small: {} bytes (expected a real rendered clip)",
                b.len()
            );
        }
    }

    #[test]
    fn segments_carry_distinct_rendered_content() {
        // Different Slovak text → different encoded bytes. If any two are
        // byte-equal, a segment was mis-committed (e.g. a copy of another).
        for (i, a) in ALL.iter().enumerate() {
            for b in &ALL[i + 1..] {
                assert_ne!(
                    a.bytes(),
                    b.bytes(),
                    "{a:?} and {b:?} have identical bytes — distinct text expected"
                );
            }
        }
    }

    // --- Selection logic ---

    #[test]
    fn warmup_mode_always_warmup_segment() {
        for refilling in [false, true] {
            for eta in [0, 5, 30, 60, 120, 999] {
                assert_eq!(
                    select_segment(RescuePushMode::Warmup, refilling, eta),
                    RescueSegment::Warmup,
                    "warmup must ignore refilling/eta (refilling={refilling}, eta={eta})"
                );
            }
        }
    }

    #[test]
    fn outage_not_refilling_shows_static_notice() {
        // Not genuinely refilling (#289) → static outage notice regardless of
        // the reported eta.
        for eta in [0, 30, 120] {
            assert_eq!(
                select_segment(RescuePushMode::Outage, false, eta),
                RescueSegment::Outage
            );
        }
    }

    #[test]
    fn outage_refilling_counts_down_through_buckets() {
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 120),
            RescueSegment::Recover2Min
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 90),
            RescueSegment::Recover2Min
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 89),
            RescueSegment::Recover1Min
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 45),
            RescueSegment::Recover1Min
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 44),
            RescueSegment::Recover30s
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 10),
            RescueSegment::Recover30s
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 9),
            RescueSegment::RecoverSoon
        );
        assert_eq!(
            select_segment(RescuePushMode::Outage, true, 0),
            RescueSegment::RecoverSoon
        );
    }

    // --- The core #259 guarantee: the countdown is genuinely reflected in the
    // pushed bytes. Before #259 the viewer path pushed ONE static blob for the
    // whole outage; this proves the Countdown source returns DIFFERENT bytes as
    // the ETA changes, i.e. the countdown actually moves on the wire. ---

    #[test]
    fn countdown_source_pushes_different_bytes_as_eta_changes() {
        let src = RescueClipSource::Countdown;
        let far = src.pick(RescuePushMode::Outage, true, 120); // Recover2Min
        let near = src.pick(RescuePushMode::Outage, true, 5); // RecoverSoon
        let stat = src.pick(RescuePushMode::Outage, false, 120); // Outage
        assert_ne!(
            far, near,
            "countdown must push different bytes far vs near the recovery"
        );
        assert_ne!(
            stat, far,
            "static outage notice must differ from the recovering countdown"
        );
    }

    #[test]
    fn fixed_source_always_pushes_the_same_blob() {
        let blob = Arc::new(b"FLV\x01 custom operator clip padding".to_vec());
        let src = RescueClipSource::Fixed(blob.clone());
        // Regardless of mode/eta, a custom operator clip is pushed unchanged.
        assert_eq!(src.pick(RescuePushMode::Outage, true, 120), &blob[..]);
        assert_eq!(src.pick(RescuePushMode::Outage, false, 5), &blob[..]);
        assert_eq!(src.pick(RescuePushMode::Warmup, false, 0), &blob[..]);
    }
}
