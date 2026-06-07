//! rs-delivery library crate.
//!
//! Exposes modules used by integration tests and by other crates that need
//! to reason about delivery behaviour without depending on the full binary.
//! The `main.rs` binary declares its own `mod …` items for the runtime
//! modules (api, endpoint_task, …); this library only exports the pieces
//! that need to be shared.

pub mod audit_ring;
pub mod chunk_lifecycle;
pub mod clock_endpoint;
pub(crate) mod fast_delay;
pub(crate) mod fast_delay_audit;
pub(crate) mod fast_keepalive;
pub mod ffmpeg_reason;
// `fast_keepalive` references the embedded default rescue blob; expose the
// const-only module in the library target too so the helper compiles there.
pub mod rescue_default;
