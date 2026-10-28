//! In-process RTMP push client backed by xiu `ClientSession` (Push mode).
//!
//! Replaces the ffmpeg subprocess that today pipes FLV chunks to YouTube/FB.
//! See `docs/superpowers/specs/2026-04-27-pure-rust-rtmp-push-design.md` for
//! the full design.

// `deny`, not `forbid`: the one exception is the TIOCOUTQ ioctl in
// `session_finish.rs` (#424), which has no safe wrapper in our dependencies.
#![deny(unsafe_code)]

mod av_invariant;
mod error;
mod flv;
mod pusher;
mod session;
mod skew;
mod state;
pub mod tls;
mod url;
mod wire_span;

pub use av_invariant::{
    AV_INVARIANT_TOLERANCE_MS, AvInvariantEvent, AvInvariantGuard, AvInvariantViolation,
};
pub use error::{
    FAST_RECONNECT_MIN_HEALTHY_MS, PushError, SHORT_RETRY_AFTER_FAST_MS, backoff_floor_ms,
    is_exponential, map_read_err, may_lose_chunk, reconnect_delay_ms,
};
pub use pusher::RtmpPusher;
pub use session::{DrainOutcome, FINISH_MAX_WAIT, FinishReport};
pub use skew::{GuardMode, MAX_AV_SKEW_MS, SKEW_DEBOUNCE_CHUNKS, SkewDecision, SkewTracker};
pub use state::{PusherConfig, PusherState, Track};
pub use wire_span::WireSpan;
