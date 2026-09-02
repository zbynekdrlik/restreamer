//! In-memory historical time-series of OUTGOING (S3-upload) throughput for
//! the dashboard Mbps graph (issue #77).
//!
//! The box's real outgoing-to-internet traffic is the successful S3-upload
//! byte stream. This module accumulates those bytes into fixed-width
//! wall-clock buckets and keeps a bounded ring of per-bucket Mbps samples so
//! the operator can see how upload behaved over (at least) the last 3 hours.
//!
//! Design (see #77 design comment):
//! - Bucketing is EVENT-DRIVEN and finalized lazily at read time — no
//!   periodic sampler task and no service-startup wiring.
//! - The clock is injected as an explicit `now_ms` argument so the bucketing
//!   math is fully deterministic under unit test.
//! - Idle gaps between activity are zero-filled so the timeline stays
//!   contiguous and an idle stream reads as 0 Mbps rather than a hole.

use std::sync::Mutex;

/// Bucket width. 15 s buckets keep the payload small (720 points for 3 h)
/// while still resolving short upload stalls.
pub const SAMPLE_INTERVAL_MS: i64 = 15_000;

/// Ring capacity. 720 buckets x 15 s = 10_800 s = exactly 3 h of retention,
/// satisfying "at least the last 3 h".
pub const HISTORY_CAPACITY: usize = 720;

/// One finalized throughput bucket. `t_ms` is the bucket START (unix ms,
/// floored to the interval boundary); `mbps` is the average outgoing
/// megabits/sec over that 15 s window.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize)]
pub struct Sample {
    pub t_ms: i64,
    pub mbps: f64,
}

/// The API payload: the fixed bucket width plus the retained samples,
/// oldest-first.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ThroughputSeries {
    pub interval_ms: i64,
    pub samples: Vec<Sample>,
}

struct Inner {
    ring: Vec<Sample>,
    head: usize,
    filled: bool,
    /// Start (interval-floored unix ms) of the bucket currently
    /// accumulating bytes. `None` until the first byte is recorded.
    open_start_ms: Option<i64>,
    open_bytes: u64,
}

pub struct ThroughputHistory {
    inner: Mutex<Inner>,
}

impl Default for ThroughputHistory {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                ring: Vec::with_capacity(HISTORY_CAPACITY),
                head: 0,
                filled: false,
                open_start_ms: None,
                open_bytes: 0,
            }),
        }
    }
}

/// Floor a timestamp to its bucket-start boundary.
fn bucket_start(t_ms: i64) -> i64 {
    // Guard against a nonsensical negative clock; unix ms is always >= 0 in
    // practice.
    let t = t_ms.max(0);
    (t / SAMPLE_INTERVAL_MS) * SAMPLE_INTERVAL_MS
}

/// Convert bytes-per-bucket to average megabits/sec over one interval.
fn mbps_of(bytes: u64) -> f64 {
    let secs = SAMPLE_INTERVAL_MS as f64 / 1000.0;
    (bytes as f64) * 8.0 / secs / 1_000_000.0
}

impl Inner {
    fn push(&mut self, s: Sample) {
        if self.ring.len() < HISTORY_CAPACITY {
            self.ring.push(s);
        } else {
            self.ring[self.head] = s;
            self.head = (self.head + 1) % HISTORY_CAPACITY;
            self.filled = true;
        }
    }

    /// Finalize every bucket strictly before `b`: emit the open bucket's
    /// accumulated rate, zero-fill any idle buckets between it and `b`, and
    /// re-open an empty bucket at `b`. No-op when there is no open bucket or
    /// `b` is not strictly after the open bucket (same bucket / clock went
    /// backwards).
    fn finalize_up_to(&mut self, b: i64) {
        let Some(open) = self.open_start_ms else {
            return;
        };
        if b <= open {
            return;
        }
        // Emit the completed open bucket.
        self.push(Sample {
            t_ms: open,
            mbps: mbps_of(self.open_bytes),
        });
        // Zero-fill idle buckets strictly between `open` and `b`. Cap the
        // fill at the ring capacity so a multi-hour idle gap can't spin an
        // unbounded loop (older samples would be evicted anyway).
        let mut s = open + SAMPLE_INTERVAL_MS;
        let mut filled = 0usize;
        while s < b && filled < HISTORY_CAPACITY {
            self.push(Sample { t_ms: s, mbps: 0.0 });
            s += SAMPLE_INTERVAL_MS;
            filled += 1;
        }
        self.open_start_ms = Some(b);
        self.open_bytes = 0;
    }

    /// Ring contents oldest-first.
    fn ordered(&self) -> Vec<Sample> {
        if !self.filled {
            return self.ring.clone();
        }
        let mut out = Vec::with_capacity(self.ring.len());
        out.extend_from_slice(&self.ring[self.head..]);
        out.extend_from_slice(&self.ring[..self.head]);
        out
    }
}

impl ThroughputHistory {
    /// Record `bytes` of outgoing traffic observed at `now_ms`. Called from
    /// the upload success path with `chunk.data_size`.
    pub fn record_bytes(&self, bytes: u64, now_ms: i64) {
        let b = bucket_start(now_ms);
        let mut g = self.inner.lock().unwrap();
        match g.open_start_ms {
            None => {
                g.open_start_ms = Some(b);
                g.open_bytes = bytes;
            }
            Some(open) => {
                if b > open {
                    g.finalize_up_to(b);
                }
                // After finalize (or same/earlier bucket) accumulate into the
                // open bucket.
                g.open_bytes = g.open_bytes.saturating_add(bytes);
            }
        }
    }

    /// Snapshot the retained series as of `now_ms`. Finalizes any completed
    /// buckets up to (but not including) the current in-progress bucket so
    /// the returned series ends at the last full 15 s window.
    pub fn series(&self, now_ms: i64) -> ThroughputSeries {
        let b = bucket_start(now_ms);
        let mut g = self.inner.lock().unwrap();
        g.finalize_up_to(b);
        ThroughputSeries {
            interval_ms: SAMPLE_INTERVAL_MS,
            samples: g.ordered(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A fixed base far from the epoch, aligned to a bucket boundary.
    const T0: i64 = 1_700_000_010_000; // multiple of 15_000

    #[test]
    fn bucket_start_floors_to_interval() {
        assert_eq!(bucket_start(T0), T0);
        assert_eq!(bucket_start(T0 + 1), T0);
        assert_eq!(bucket_start(T0 + SAMPLE_INTERVAL_MS - 1), T0);
        assert_eq!(
            bucket_start(T0 + SAMPLE_INTERVAL_MS),
            T0 + SAMPLE_INTERVAL_MS
        );
    }

    #[test]
    fn mbps_conversion_is_bytes_times_8_over_15s() {
        // 15 MB in one 15 s bucket -> 15e6*8/15/1e6 = 8 Mbps.
        assert!((mbps_of(15_000_000) - 8.0).abs() < 1e-9);
        assert_eq!(mbps_of(0), 0.0);
    }

    #[test]
    fn single_bucket_not_emitted_until_next_bucket_read() {
        let h = ThroughputHistory::default();
        h.record_bytes(1_875_000, T0 + 100); // 1.875 MB -> 1 Mbps for the bucket
        // Reading within the SAME bucket: nothing finalized yet.
        let s = h.series(T0 + 200);
        assert!(
            s.samples.is_empty(),
            "current in-progress bucket is not emitted"
        );
        // Reading in the NEXT bucket finalizes the first one.
        let s = h.series(T0 + SAMPLE_INTERVAL_MS + 10);
        assert_eq!(s.samples.len(), 1);
        assert_eq!(s.samples[0].t_ms, T0);
        assert!((s.samples[0].mbps - 1.0).abs() < 1e-9);
        assert_eq!(s.interval_ms, SAMPLE_INTERVAL_MS);
    }

    #[test]
    fn bytes_in_same_bucket_accumulate() {
        let h = ThroughputHistory::default();
        h.record_bytes(1_000_000, T0 + 10);
        h.record_bytes(875_000, T0 + 20); // total 1.875 MB -> 1 Mbps
        let s = h.series(T0 + SAMPLE_INTERVAL_MS + 1);
        assert_eq!(s.samples.len(), 1);
        assert!((s.samples[0].mbps - 1.0).abs() < 1e-9);
    }

    #[test]
    fn idle_gap_is_zero_filled_contiguously() {
        let h = ThroughputHistory::default();
        h.record_bytes(1_875_000, T0 + 1); // bucket 0 -> 1 Mbps
        // Next activity 3 buckets later (buckets 1 and 2 idle).
        h.record_bytes(1_875_000, T0 + 3 * SAMPLE_INTERVAL_MS + 1);
        let s = h.series(T0 + 4 * SAMPLE_INTERVAL_MS + 1);
        let t: Vec<i64> = s.samples.iter().map(|x| x.t_ms).collect();
        assert_eq!(
            t,
            vec![
                T0,
                T0 + SAMPLE_INTERVAL_MS,
                T0 + 2 * SAMPLE_INTERVAL_MS,
                T0 + 3 * SAMPLE_INTERVAL_MS,
            ],
            "timeline is contiguous across the idle gap"
        );
        assert!((s.samples[0].mbps - 1.0).abs() < 1e-9);
        assert_eq!(s.samples[1].mbps, 0.0);
        assert_eq!(s.samples[2].mbps, 0.0);
        assert!((s.samples[3].mbps - 1.0).abs() < 1e-9);
    }

    #[test]
    fn empty_history_reads_empty() {
        let h = ThroughputHistory::default();
        let s = h.series(T0 + 10 * SAMPLE_INTERVAL_MS);
        assert!(s.samples.is_empty());
        assert_eq!(s.interval_ms, SAMPLE_INTERVAL_MS);
    }

    #[test]
    fn ring_wraps_and_retains_only_capacity_newest() {
        let h = ThroughputHistory::default();
        // Emit HISTORY_CAPACITY + 5 completed buckets, each 1 Mbps, one byte
        // batch per bucket. Read once at the very end to finalize.
        let n = HISTORY_CAPACITY + 5;
        for i in 0..n {
            h.record_bytes(1_875_000, T0 + (i as i64) * SAMPLE_INTERVAL_MS + 1);
        }
        // Finalize everything up through the last completed bucket.
        let s = h.series(T0 + (n as i64) * SAMPLE_INTERVAL_MS + 1);
        assert_eq!(s.samples.len(), HISTORY_CAPACITY, "capped at capacity");
        // Oldest retained sample is bucket #5 (the first 5 evicted).
        assert_eq!(s.samples[0].t_ms, T0 + 5 * SAMPLE_INTERVAL_MS);
        // Newest retained is bucket #(n-1).
        assert_eq!(
            s.samples[HISTORY_CAPACITY - 1].t_ms,
            T0 + (n as i64 - 1) * SAMPLE_INTERVAL_MS
        );
    }

    #[test]
    fn clock_going_backwards_does_not_panic_or_emit_negative() {
        let h = ThroughputHistory::default();
        h.record_bytes(1_000_000, T0 + SAMPLE_INTERVAL_MS + 1);
        // A record with an earlier timestamp (clock skew) accumulates into
        // the open bucket rather than finalizing.
        h.record_bytes(875_000, T0 + 1);
        let s = h.series(T0 + 3 * SAMPLE_INTERVAL_MS);
        // Only the one open bucket finalizes; total 1.875 MB -> 1 Mbps.
        assert_eq!(s.samples.len(), 1);
        assert!((s.samples[0].mbps - 1.0).abs() < 1e-9);
    }
}
