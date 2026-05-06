// S3 fetch quantile + bucket profile for diag dump.
// See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.5.
// Issue #176.

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
