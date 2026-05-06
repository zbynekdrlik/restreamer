// S3 fetch quantile + bucket profile for diag dump.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.5.
// Issue #176.

use std::collections::BTreeMap;
use std::sync::Mutex;

pub struct S3FetchProfile {
    inner: Mutex<Inner>,
}

struct Inner {
    count: u64,
    bytes_total: u64,
    latency_buckets: Vec<u64>,
    fail_count_by_class: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct S3FetchProfileSnapshot {
    pub count: u64,
    pub bytes_total: u64,
    pub p50_latency_ms: u64,
    pub p99_latency_ms: u64,
    pub fail_count_by_class: BTreeMap<String, u64>,
}

impl S3FetchProfile {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                count: 0,
                bytes_total: 0,
                latency_buckets: vec![0u64; 65],
                fail_count_by_class: BTreeMap::new(),
            }),
        }
    }

    pub fn record_success(&self, latency_ms: u64, bytes: u64) {
        let mut g = self.inner.lock().unwrap();
        g.count += 1;
        g.bytes_total += bytes;
        let bucket = bucket_index(latency_ms);
        g.latency_buckets[bucket] += 1;
    }

    pub fn record_failure(&self, class: &str) {
        let mut g = self.inner.lock().unwrap();
        *g.fail_count_by_class.entry(class.to_string()).or_insert(0) += 1;
    }

    pub fn snapshot(&self) -> S3FetchProfileSnapshot {
        let g = self.inner.lock().unwrap();
        let p50 = quantile_from_buckets(&g.latency_buckets, 0.50);
        let p99 = quantile_from_buckets(&g.latency_buckets, 0.99);
        S3FetchProfileSnapshot {
            count: g.count,
            bytes_total: g.bytes_total,
            p50_latency_ms: p50,
            p99_latency_ms: p99,
            fail_count_by_class: g.fail_count_by_class.clone(),
        }
    }
}

impl Default for S3FetchProfile {
    fn default() -> Self {
        Self::new()
    }
}

fn bucket_index(latency_ms: u64) -> usize {
    // Log-spaced: bucket i covers [2^i, 2^(i+1)) ms; bucket 64 = "very large".
    let mut i = 0usize;
    let mut threshold = 1u64;
    while i < 64 && latency_ms >= threshold * 2 {
        threshold = threshold.saturating_mul(2);
        i += 1;
    }
    i
}

fn quantile_from_buckets(buckets: &[u64], q: f64) -> u64 {
    let total: u64 = buckets.iter().sum();
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * q).ceil() as u64;
    let mut acc = 0u64;
    for (i, &c) in buckets.iter().enumerate() {
        acc += c;
        if acc >= target {
            return (1u64 << i.min(63)).saturating_mul(2).saturating_sub(1);
        }
    }
    u64::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_records_count_bytes_and_buckets_latency() {
        let p = S3FetchProfile::new();
        p.record_success(45, 1024);
        p.record_success(50, 2048);
        p.record_success(320, 4096);
        let snap = p.snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.bytes_total, 1024 + 2048 + 4096);
        assert!(snap.p50_latency_ms >= 45);
        assert!(snap.p99_latency_ms >= 320);
    }

    #[test]
    fn profile_classifies_failures() {
        let p = S3FetchProfile::new();
        p.record_failure("504");
        p.record_failure("504");
        p.record_failure("timeout");
        let snap = p.snapshot();
        assert_eq!(*snap.fail_count_by_class.get("504").unwrap(), 2);
        assert_eq!(*snap.fail_count_by_class.get("timeout").unwrap(), 1);
        assert_eq!(snap.fail_count_by_class.get("503"), None);
    }
}
