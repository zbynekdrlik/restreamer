//! Per-chunk lifecycle telemetry for fast-endpoint zero-reconnect
//! (#184). See spec §3.1-3.2.
//!
//! Component map:
//! - `timings` — `ChunkLifecycleTimings` struct + gap math + worst-stage selection.
//! - `sampler` — `LifecycleSampler` decides when to emit which audit row (Task 12).
//! - `audit`   — emit_lifecycle_sample / breach / predeath helpers (Task 12).

// TODO(#184): narrow this allow once Tasks 11-12 add LifecycleSampler
// (the consumer of ChunkLifecycleTimings).
#![allow(dead_code, unused_imports)]

pub mod timings;
pub use timings::ChunkLifecycleTimings;

#[cfg(test)]
mod timings_tests;
