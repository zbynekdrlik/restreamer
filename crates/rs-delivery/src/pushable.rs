//! The `Pushable` RTMP-push abstraction.
//!
//! A minimal interface over `rs_rtmp_push::RtmpPusher` so the consumer's
//! normal-delivery push (`handle_rust_push`) AND the rescue push loop
//! (`rust_rescue_push`) can be driven by a recording mock in tests instead
//! of a concrete `RtmpPusher` that dials a real RTMP server.
//!
//! This trait used to live as `pub(super) trait Pushable` inside
//! `endpoint_task::consumer_helpers`. It was hoisted here (#239) so
//! `rust_rescue_push` — which sits OUTSIDE the `endpoint_task` module tree —
//! can be made generic over it and accept an injected recording pusher. The
//! existing `consumer_helpers::Pushable` name is preserved as a re-export so
//! the consumer-path code and its tests keep compiling unchanged.

use rs_rtmp_push::{PushError, RtmpPusher};

/// Minimal RTMP-push interface needed by the consumer push path and the
/// rescue push loop. Extracted as a trait so unit tests can substitute a
/// mock that records every pushed payload (proving rescue clip bytes are
/// actually pushed) without standing up a real RTMP server.
pub(crate) trait Pushable {
    fn push_flv_bytes(
        &mut self,
        data: &[u8],
    ) -> impl std::future::Future<Output = Result<(), PushError>> + Send;
    fn close(&mut self) -> impl std::future::Future<Output = ()> + Send;
    /// #424: end the publish for good without losing its tail (unpublish,
    /// wait for the queued bytes, close). `close` stays the reconnect hook.
    fn finish(&mut self) -> impl std::future::Future<Output = ()> + Send;
    fn reconnect_count(&self) -> u32;
    /// Current signed content-PTS A/V skew in ms (positive = audio behind
    /// video). Surfaced to per-endpoint telemetry (issue #257).
    fn av_skew_ms(&self) -> i64;
    /// Drain the absolute A/V invariant guard edges (#367) recorded by the
    /// pushes since the last call; the consumer audits each one. Required
    /// (no default): a pusher that silently returned nothing would make
    /// its violations invisible.
    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent>;
    /// #368: push a chunk AGAIN after its first push failed (the consumer's
    /// one re-send on the fresh session). A plain pusher pushes it like any
    /// chunk; a pusher that mirrors its chunks elsewhere (the YouTube backup
    /// tee) already mirrored this one and must send it to the primary only.
    fn repush_flv_bytes(
        &mut self,
        data: &[u8],
    ) -> impl std::future::Future<Output = Result<(), PushError>> + Send {
        self.push_flv_bytes(data)
    }
    /// #368: when the upstream last acknowledged our bytes on the current
    /// session (RTMP Acknowledgement), so the death row's
    /// `time_since_last_upstream_ack_ms` is measured. `None` = no ack seen.
    fn last_upstream_ack_at(&self) -> Option<std::time::Instant> {
        None
    }
    /// #424: first/last outgoing media ts of the last successful push, for
    /// the per-chunk push log. `None` = unknown (a mock) or nothing sent.
    fn last_wire_span(&self) -> Option<rs_rtmp_push::WireSpan> {
        None
    }
    /// #424: the S3 chunk id of the NEXT `push_flv_bytes` payload, so a
    /// mirror (the YouTube backup) can log which chunk it pushed. A plain
    /// pusher logs nothing itself and ignores it.
    fn label_chunk(&mut self, _chunk_id: i64) {}
    /// #437: the next payload does not continue the previous one (a chunk-id
    /// gap): start a new wire mapping so the content jump is a clean cut,
    /// never a wire-ts jump that pacing sleeps out. A mock ignores it.
    fn begin_new_mapping(&mut self) {}
}

impl Pushable for RtmpPusher {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        RtmpPusher::push_flv_bytes(self, data).await
    }

    async fn close(&mut self) {
        RtmpPusher::close(self).await
    }

    async fn finish(&mut self) {
        RtmpPusher::finish(self, rs_rtmp_push::FINISH_MAX_WAIT).await;
    }

    fn reconnect_count(&self) -> u32 {
        RtmpPusher::reconnect_count(self)
    }

    fn av_skew_ms(&self) -> i64 {
        RtmpPusher::av_skew_ms(self)
    }

    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        RtmpPusher::take_av_invariant_events(self)
    }

    fn last_upstream_ack_at(&self) -> Option<std::time::Instant> {
        RtmpPusher::last_upstream_ack_at(self)
    }

    fn last_wire_span(&self) -> Option<rs_rtmp_push::WireSpan> {
        RtmpPusher::last_wire_span(self)
    }

    fn begin_new_mapping(&mut self) {
        RtmpPusher::begin_new_mapping(self)
    }
}

/// #437: does the next payload continue the previous LIVE chunk?
///
/// Fed the S3 chunk id of every payload (`None` for a rescue / warmup clip
/// segment or a keepalive freeze, which never counts as a gap and leaves the
/// last live id alone). A live chunk whose id is not the previous live id + 1
/// is a gap: the pusher must start a new wire mapping before it.
#[derive(Debug, Default)]
pub(crate) struct ChunkContinuity {
    last_live_id: Option<i64>,
}

impl ChunkContinuity {
    /// Record `chunk_id` as the next payload; `true` when it is a gap.
    pub(crate) fn is_gap(&mut self, chunk_id: Option<i64>) -> bool {
        let Some(id) = chunk_id else {
            return false;
        };
        let gap = self
            .last_live_id
            .is_some_and(|last| id != last.saturating_add(1));
        self.last_live_id = Some(id);
        gap
    }
}

#[cfg(test)]
mod tests {
    use super::ChunkContinuity;

    #[test]
    fn only_a_live_id_that_does_not_follow_the_last_one_is_a_gap() {
        let mut c = ChunkContinuity::default();
        assert!(
            !c.is_gap(Some(10)),
            "the first live chunk has nothing to follow"
        );
        assert!(!c.is_gap(Some(11)));
        assert!(!c.is_gap(None), "a clip segment is never a gap");
        assert!(!c.is_gap(Some(12)), "a clip segment keeps the last live id");
        assert!(c.is_gap(Some(14)), "12 -> 14 skips a chunk");
        assert!(c.is_gap(Some(14)), "a repeated id is no continuation");
        assert!(c.is_gap(Some(3)), "a backward id is a gap");
        assert!(!c.is_gap(Some(4)));
    }
}
