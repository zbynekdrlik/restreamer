//! Small pure helpers used by the delivery orchestrator.
//!
//! Kept in a separate file so `delivery.rs` stays under the 1000-line file-size gate.

/// Returns true if the DB-side status represents a live delivery instance
/// that we can talk to over HTTP.
pub(crate) fn is_delivery_active(status: &str) -> bool {
    matches!(
        status,
        "booting" | "initializing" | "delivering" | "running"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_delivery_active_live_states() {
        assert!(is_delivery_active("booting"));
        assert!(is_delivery_active("initializing"));
        assert!(is_delivery_active("delivering"));
        assert!(is_delivery_active("running"));
    }

    #[test]
    fn is_delivery_active_dead_states() {
        assert!(!is_delivery_active("creating"));
        assert!(!is_delivery_active("stopping"));
        assert!(!is_delivery_active("deleted"));
        assert!(!is_delivery_active("failed"));
        assert!(!is_delivery_active(""));
    }
}
