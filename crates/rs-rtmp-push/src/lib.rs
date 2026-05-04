//! In-process RTMP push client backed by xiu `ClientSession` (Push mode).
//!
//! Replaces the ffmpeg subprocess that today pipes FLV chunks to YouTube/FB.
//! See `docs/superpowers/specs/2026-04-27-pure-rust-rtmp-push-design.md` for
//! the full design.

#![forbid(unsafe_code)]

mod error;
mod flv;
mod pusher;
mod session;
mod state;
pub mod tls;

pub use error::{PushError, backoff_floor_ms, is_exponential, map_read_err};
pub use pusher::RtmpPusher;
pub use state::{PusherConfig, PusherState};
