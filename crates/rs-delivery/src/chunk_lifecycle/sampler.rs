//! LifecycleSampler — see spec §3.2. Implementation in Task 12.

#![allow(dead_code)]

use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct LifecycleSampler {
    pub sample_every_n: u64,
    pub breach_threshold_ms: u64,
    pub breach_rate_limit_window: Duration,
    pub last_breach_emit: Option<Instant>,
    pub pushed_count: u64,
    pub predeath_ring: VecDeque<ChunkLifecycleTimings>,
}

impl LifecycleSampler {
    pub fn new(sample_every_n: u64, breach_threshold_ms: u64) -> Self {
        Self {
            sample_every_n,
            breach_threshold_ms,
            breach_rate_limit_window: Duration::from_secs(5),
            last_breach_emit: None,
            pushed_count: 0,
            predeath_ring: VecDeque::with_capacity(5),
        }
    }

    pub fn observe(
        &mut self,
        _timings: &ChunkLifecycleTimings,
        _audit_ring: &Option<Arc<AuditRing>>,
    ) {
        unimplemented!("Task 12")
    }

    pub fn emit_predeath(&self, _audit_ring: &Option<Arc<AuditRing>>) {
        unimplemented!("Task 12")
    }
}
