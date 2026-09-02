//! Fast-endpoint keepalive: hold the EXISTING rtmp session alive during a
//! short producer gap by re-pushing the LAST DELIVERED CHUNK (freeze frame).
//!
//! FREEZE-ONLY, codec-homogeneous: the keepalive must never push any bytes
//! that were not produced by the live encoder. The RTMP pusher de-duplicates
//! AVC sequence headers per session, so pushing the rescue clip (different
//! SPS/PPS) onto a live session makes YouTube decode the real stream with
//! the wrong codec config -> solid green video for the entire session
//! (2026-06-11 streampp incident, KS-PP-TEST). If no chunk has been
//! delivered yet there is NOTHING codec-safe to push: the consumer waits
//! for the first real chunk instead of entering keepalive.
//!
//! Re-using the same session means the RTMP connection is never closed by
//! starvation — only a real socket error reconnects. `push_flv_bytes`
//! re-anchors timestamps across the repeated blob internally.
#![allow(dead_code)]
use std::sync::Arc;

/// Wait this long for a real chunk before starting keepalive frames. Far
/// below the 8s full-stall rescue threshold so the trickle regime (chunks
/// arriving late but often) is covered, not just total outages.
pub const FAST_KEEPALIVE_TRIGGER_SECS: u64 = 2;

/// The bytes a keepalive tick may push: ONLY the last delivered chunk
/// (same codec as the live stream). `None` when no chunk has been delivered
/// yet on this session — the caller must NOT push anything in that case.
pub fn keepalive_bytes(last_chunk: &Option<Arc<Vec<u8>>>) -> Option<&[u8]> {
    last_chunk.as_ref().map(|a| a.as_slice())
}

/// Whether this endpoint holds the live RTMP session with a codec-homogeneous
/// freeze-frame bridge during a producer gap (#124) instead of the old
/// drop-then-reconnect path. The bridge re-pushes the last real chunk on the
/// LIVE session (same SPS/PPS → no #249 green corruption, no CDN-visible
/// disconnect), so short gaps and trickle resolve with ZERO outage.
///
/// Only the pure-Rust pusher path can bridge (the ffmpeg path has no
/// `RtmpPusher` handle to keep alive). Before #124 this was scoped to FAST
/// endpoints only, leaving the non-fast production church stream with up-to-8s
/// of dead air + a premature disconnect at every drain — the "few seconds
/// outage" #124 fixes.
pub(crate) fn uses_keepalive_bridge(ep_cfg: &crate::api::EndpointConfig) -> bool {
    // RED placeholder: fast-only (pre-#124 behaviour). Flipped to all
    // rust-pusher endpoints in the GREEN commit.
    ep_cfg.is_fast && ep_cfg.pusher == rs_core::models::PusherKind::Rust
}

/// Escalation anchor passed to `keepalive_until_chunk`, measured from
/// keepalive ENTRY (which happens ~`FAST_KEEPALIVE_TRIGGER_SECS` after the
/// last real chunk). Keeping the fresh-reconnect rescue anchored to the LAST
/// REAL CHUNK is the "rescue never slower than today" invariant (#124):
/// non-fast subtracts the trigger delay so escalation still fires exactly
/// `RESCUE_STALL_THRESHOLD_SECS` after the last real chunk, while fast keeps
/// its prior 8s-from-entry timing byte-for-byte.
pub(crate) fn keepalive_escalate_after(
    _ep_cfg: &crate::api::EndpointConfig,
) -> std::time::Duration {
    // RED placeholder: flat threshold-from-entry for both (pre-#124). Flipped
    // to the per-fast/non-fast anchor in the GREEN commit.
    std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(is_fast: bool, pusher: rs_core::models::PusherKind) -> crate::api::EndpointConfig {
        crate::api::EndpointConfig {
            alias: "t".to_string(),
            service_type: "TEST_FILE".to_string(),
            stream_key: "k".to_string(),
            is_fast,
            chunk_format: "flv".to_string(),
            start_chunk_id: None,
            pusher,
        }
    }

    #[test]
    fn non_fast_rust_endpoint_uses_keepalive_bridge() {
        // #124: the non-fast production stream MUST bridge the gap on the live
        // session (no dead air, no premature disconnect), not just fast ones.
        use rs_core::models::PusherKind;
        assert!(
            uses_keepalive_bridge(&cfg(false, PusherKind::Rust)),
            "non-fast rust endpoint must use the freeze-frame bridge (#124)"
        );
        assert!(
            uses_keepalive_bridge(&cfg(true, PusherKind::Rust)),
            "fast rust endpoint still bridges"
        );
        assert!(
            !uses_keepalive_bridge(&cfg(false, PusherKind::Ffmpeg)),
            "ffmpeg endpoints have no RtmpPusher to bridge"
        );
    }

    #[test]
    fn escalate_anchor_is_last_real_chunk_never_slower() {
        // #124 invariant: the fresh-reconnect rescue must engage exactly
        // RESCUE_STALL_THRESHOLD_SECS after the LAST REAL CHUNK — never slower
        // than before. Keepalive is entered ~FAST_KEEPALIVE_TRIGGER_SECS after
        // the last chunk, so the non-fast anchor subtracts that trigger delay.
        use rs_core::models::PusherKind;
        let expected_non_fast = std::time::Duration::from_secs(
            crate::rescue::RESCUE_STALL_THRESHOLD_SECS - FAST_KEEPALIVE_TRIGGER_SECS,
        );
        assert_eq!(
            keepalive_escalate_after(&cfg(false, PusherKind::Rust)),
            expected_non_fast,
            "non-fast escalation must be anchored to the last real chunk (8s), \
             i.e. threshold minus the keepalive trigger delay"
        );
        // Fast keeps its prior 8s-from-entry timing byte-for-byte.
        assert_eq!(
            keepalive_escalate_after(&cfg(true, PusherKind::Rust)),
            std::time::Duration::from_secs(crate::rescue::RESCUE_STALL_THRESHOLD_SECS),
            "fast escalation timing unchanged by #124"
        );
    }

    #[test]
    fn bytes_none_when_no_chunk_delivered() {
        let none: Option<Arc<Vec<u8>>> = None;
        assert!(
            keepalive_bytes(&none).is_none(),
            "no codec-safe bytes exist before the first chunk"
        );
    }

    #[test]
    fn bytes_are_the_last_chunk() {
        let last = Some(Arc::new(vec![1u8, 2, 3]));
        assert_eq!(
            keepalive_bytes(&last),
            Some(&[1u8, 2, 3][..]),
            "keepalive must push the last delivered chunk verbatim"
        );
    }
}
