//! In-memory upload metrics for the /uploads/stats API.
//!
//! Tracks successes + failures + durations in a bounded ring buffer per
//! worker event. Computes chunks/s (1-minute window) and p50/p95 latency.

use std::sync::Mutex;
use std::time::{Duration, Instant};

const RING_CAPACITY: usize = 2048;

#[derive(Clone, Copy, Debug)]
pub struct UploadEvent {
    pub at: Instant,
    pub duration_ms: u32,
    pub success: bool,
}

pub struct UploadMetrics {
    inner: Mutex<Inner>,
}

struct Inner {
    ring: Vec<UploadEvent>,
    head: usize,
    filled: bool,
    in_flight: usize,
    adaptive_target: usize,
}

impl Default for UploadMetrics {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                ring: Vec::with_capacity(RING_CAPACITY),
                head: 0,
                filled: false,
                in_flight: 0,
                adaptive_target: 4,
            }),
        }
    }
}

impl UploadMetrics {
    pub fn record(&self, event: UploadEvent) {
        let mut g = self.inner.lock().unwrap();
        if g.ring.len() < RING_CAPACITY {
            g.ring.push(event);
        } else {
            let h = g.head;
            g.ring[h] = event;
            g.head = (g.head + 1) % RING_CAPACITY;
            g.filled = true;
        }
    }

    pub fn set_in_flight(&self, n: usize) {
        self.inner.lock().unwrap().in_flight = n;
    }

    pub fn set_adaptive_target(&self, n: usize) {
        self.inner.lock().unwrap().adaptive_target = n;
    }

    pub fn snapshot(&self, window: Duration) -> Snapshot {
        let g = self.inner.lock().unwrap();
        let cutoff = Instant::now().checked_sub(window);
        let events: Vec<UploadEvent> = g
            .ring
            .iter()
            .copied()
            .filter(|e| cutoff.map(|c| e.at >= c).unwrap_or(true))
            .collect();

        let total = events.len();
        let successes = events.iter().filter(|e| e.success).count();
        let failures = total - successes;
        let mut durations: Vec<u32> = events
            .iter()
            .filter(|e| e.success)
            .map(|e| e.duration_ms)
            .collect();
        durations.sort_unstable();

        let median_ms = percentile(&durations, 50);
        let p95_ms = percentile(&durations, 95);
        let chunks_per_sec = if window.as_secs() == 0 {
            0.0
        } else {
            successes as f64 / window.as_secs_f64()
        };
        let error_rate = if total == 0 {
            0.0
        } else {
            failures as f64 / total as f64
        };

        Snapshot {
            chunks_per_sec,
            median_ms,
            p95_ms,
            error_rate,
            in_flight: g.in_flight,
            adaptive_target: g.adaptive_target,
        }
    }
}

fn percentile(sorted: &[u32], p: u32) -> u32 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as u64 * p as u64) / 100).min(sorted.len() as u64 - 1) as usize;
    sorted[idx]
}

#[derive(Debug, Clone, Copy, serde::Serialize, PartialEq)]
pub struct Snapshot {
    pub chunks_per_sec: f64,
    pub median_ms: u32,
    pub p95_ms: u32,
    pub error_rate: f64,
    pub in_flight: usize,
    pub adaptive_target: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_is_zero() {
        let m = UploadMetrics::default();
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.chunks_per_sec, 0.0);
        assert_eq!(s.median_ms, 0);
        assert_eq!(s.p95_ms, 0);
        assert_eq!(s.error_rate, 0.0);
    }

    #[test]
    fn percentile_of_empty_is_zero() {
        assert_eq!(percentile(&[], 50), 0);
    }

    #[test]
    fn percentile_is_monotonic() {
        let v: Vec<u32> = (0..100).collect();
        let median = percentile(&v, 50);
        let p95 = percentile(&v, 95);
        assert!(p95 > median);
    }

    #[test]
    fn snapshot_counts_successes_for_rate_and_error_rate_for_failures() {
        let m = UploadMetrics::default();
        let now = Instant::now();
        for _ in 0..4 {
            m.record(UploadEvent {
                at: now,
                duration_ms: 100,
                success: true,
            });
        }
        m.record(UploadEvent {
            at: now,
            duration_ms: 5000,
            success: false,
        });

        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.error_rate, 0.2, "1 of 5 failed");
        assert!(s.chunks_per_sec > 0.0, "at least one success is counted");
        assert_eq!(s.median_ms, 100, "median over successes only");
    }

    #[test]
    fn set_in_flight_and_target_are_reflected() {
        let m = UploadMetrics::default();
        m.set_in_flight(7);
        m.set_adaptive_target(16);
        let s = m.snapshot(Duration::from_secs(60));
        assert_eq!(s.in_flight, 7);
        assert_eq!(s.adaptive_target, 16);
    }
}
