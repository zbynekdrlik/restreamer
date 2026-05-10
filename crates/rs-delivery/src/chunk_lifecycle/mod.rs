//! Per-chunk lifecycle telemetry for fast-endpoint zero-reconnect
//! (#184). See spec §3.1-3.2.
//!
//! Component map:
//! - `timings` — `ChunkLifecycleTimings` struct + gap math + worst-stage selection.
//! - `sampler` — `LifecycleSampler` decides when to emit which audit row.
//! - `audit`   — emit_lifecycle_sample / breach / predeath helpers.

pub mod audit;
pub mod sampler;
pub mod timings;
pub use sampler::LifecycleSampler;
pub use timings::ChunkLifecycleTimings;

#[cfg(test)]
mod sampler_tests;
#[cfg(test)]
mod timings_tests;
