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
}
