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

#[cfg(test)]
mod tests {
    use super::*;

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
