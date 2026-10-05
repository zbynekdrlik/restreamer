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
    fn reconnect_count(&self) -> u32;
    /// Current signed content-PTS A/V skew in ms (positive = audio behind
    /// video). Surfaced to per-endpoint telemetry (issue #257).
    fn av_skew_ms(&self) -> i64;
    /// Drain the absolute A/V invariant guard edges (#367) recorded by the
    /// pushes since the last call; the consumer audits each one. Test
    /// pushers that do not model the guard keep the empty default.
    fn take_av_invariant_events(&mut self) -> Vec<rs_rtmp_push::AvInvariantEvent> {
        Vec::new()
    }
}

impl Pushable for RtmpPusher {
    async fn push_flv_bytes(&mut self, data: &[u8]) -> Result<(), PushError> {
        RtmpPusher::push_flv_bytes(self, data).await
    }

    async fn close(&mut self) {
        RtmpPusher::close(self).await
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
}
