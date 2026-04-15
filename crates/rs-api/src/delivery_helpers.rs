//! Small pure helpers used by the delivery orchestrator.
//!
//! Kept in a separate file so `delivery.rs` stays under the 1000-line file-size gate.

/// Returns true if the DB-side status represents a live delivery instance
/// that we can talk to over HTTP. The orchestrator transitions instances
/// through `creating → booting → initializing → delivering → stopping →
/// deleted` (plus `failed` on error). The post-boot states all have rs-delivery
/// listening on :8000; before boot we have no IP, and after stopping/deleted
/// the VPS is gone. We keep `running` in the match for backwards-compatibility
/// with older rows that predate the fine-grained status states.
pub(crate) fn is_delivery_active(status: &str) -> bool {
    matches!(
        status,
        "booting" | "initializing" | "delivering" | "running"
    )
}

/// Compute the start_chunk_id for a fresh (non-resume) delivery session.
///
/// Returns `max_seq + 1` so that the VPS warmup loop walks forward from the
/// first chunk produced AFTER the operator clicked Start Delivering, rather
/// than walking historical chunks that are already on S3.  When no chunks
/// exist yet (`max_seq` is `None`), the function returns 1 — the very first
/// chunk of the event — which is correct because there is no history to skip.
pub(crate) fn compute_start_chunk_id(max_seq: Option<i64>) -> i64 {
    max_seq.unwrap_or(0) + 1
}
