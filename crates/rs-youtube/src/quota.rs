//! Per-project YouTube Data API quota tracker.
//! Token-bucket sliding window. Capacity = `daily_quota`, refill rate =
//! `daily_quota / 86_400` units per second. Single global instance per
//! process — Google's quota is per-project, not per-channel/endpoint.

use std::sync::Mutex;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

#[derive(Debug, Clone, Copy)]
pub struct QuotaExhausted;

impl std::fmt::Display for QuotaExhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "youtube quota exhausted")
    }
}

impl std::error::Error for QuotaExhausted {}

struct BucketState {
    units: f64,
    last_refill: Instant,
    #[cfg(test)]
    test_offset: Duration,
}

pub struct QuotaTracker {
    capacity: f64,
    refill_per_sec: f64,
    state: Mutex<BucketState>,
}

impl QuotaTracker {
    pub fn new(daily_quota: u32) -> Self {
        Self {
            capacity: daily_quota as f64,
            refill_per_sec: daily_quota as f64 / 86_400.0,
            state: Mutex::new(BucketState {
                units: daily_quota as f64,
                last_refill: Instant::now(),
                #[cfg(test)]
                test_offset: Duration::ZERO,
            }),
        }
    }

    fn refill_locked(&self, s: &mut BucketState) {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(s.last_refill);
        #[cfg(test)]
        let elapsed = elapsed + s.test_offset;
        #[cfg(test)]
        {
            s.test_offset = Duration::ZERO;
        }
        let refill = elapsed.as_secs_f64() * self.refill_per_sec;
        s.units = (s.units + refill).min(self.capacity);
        s.last_refill = now;
    }

    pub fn acquire(&self, units: u32) -> Result<(), QuotaExhausted> {
        let mut s = self.state.lock().expect("quota tracker mutex poisoned");
        self.refill_locked(&mut s);
        let cost = units as f64;
        if s.units >= cost {
            s.units -= cost;
            Ok(())
        } else {
            Err(QuotaExhausted)
        }
    }

    pub fn remaining(&self) -> u32 {
        let mut s = self.state.lock().expect("quota tracker mutex poisoned");
        self.refill_locked(&mut s);
        s.units.floor() as u32
    }

    #[cfg(test)]
    pub fn advance_for_test(&self, by: Duration) {
        let mut s = self.state.lock().expect("quota tracker mutex poisoned");
        s.test_offset += by;
    }
}
