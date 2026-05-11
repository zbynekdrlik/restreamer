//! LifecycleSampler — see spec §3.2.

use super::audit::{emit_lifecycle_breach, emit_lifecycle_predeath, emit_lifecycle_sample};
use super::timings::ChunkLifecycleTimings;
use crate::audit_ring::AuditRing;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PREDEATH_RING_CAP: usize = 5;

pub struct LifecycleSampler {
    sample_every_n: u64,
    breach_threshold_ms: u64,
    breach_rate_limit_window: Duration,
    last_breach_emit: Option<Instant>,
    pushed_count: u64,
    /// Public so tests can inspect cap + content. External (non-test)
    /// code should treat this as opaque and call `predeath_len()` instead.
    pub predeath_ring: VecDeque<ChunkLifecycleTimings>,
}

impl LifecycleSampler {
    pub fn new(sample_every_n: u64, breach_threshold_ms: u64) -> Self {
        Self {
            sample_every_n: sample_every_n.max(1),
            breach_threshold_ms,
            breach_rate_limit_window: Duration::from_secs(5),
            last_breach_emit: None,
            pushed_count: 0,
            predeath_ring: VecDeque::with_capacity(PREDEATH_RING_CAP),
        }
    }

    /// Observe one chunk's lifecycle. Decides whether to emit a sample row,
    /// a breach row, both, or neither. Always pushes to predeath_ring.
    pub fn observe(
        &mut self,
        timings: &ChunkLifecycleTimings,
        audit_ring: &Option<Arc<AuditRing>>,
    ) {
        // Push to predeath ring (cap at 5; evict oldest).
        if self.predeath_ring.len() == PREDEATH_RING_CAP {
            self.predeath_ring.pop_front();
        }
        self.predeath_ring.push_back(timings.clone());

        self.pushed_count = self.pushed_count.saturating_add(1);

        let Some(ring) = audit_ring.as_ref() else {
            return;
        };

        // Sample emission (counter-based): emits at chunk N where
        // pushed_count is a multiple of sample_every_n.
        if self.pushed_count % self.sample_every_n == 0 {
            emit_lifecycle_sample(ring, timings);
            return;
        }

        // Breach emission (rate-limited).
        let (_label, worst) = timings.worst_stage();
        if worst.as_millis() as u64 > self.breach_threshold_ms {
            let now = Instant::now();
            let allowed = match self.last_breach_emit {
                Some(t) => now.duration_since(t) >= self.breach_rate_limit_window,
                None => true,
            };
            if allowed {
                emit_lifecycle_breach(ring, timings);
                self.last_breach_emit = Some(now);
            }
        }
    }

    /// Emit a predeath row with the last (up to 5) chunks. Always emits;
    /// no rate-limit. Caller invokes this on endpoint death.
    pub fn emit_predeath(&self, audit_ring: &Option<Arc<AuditRing>>) {
        let Some(ring) = audit_ring.as_ref() else {
            return;
        };
        let snapshot: Vec<_> = self.predeath_ring.iter().cloned().collect();
        emit_lifecycle_predeath(ring, &snapshot);
    }

    /// Number of chunks currently held in the predeath ring (0..=5).
    pub fn predeath_len(&self) -> usize {
        self.predeath_ring.len()
    }
}
