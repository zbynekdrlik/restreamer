//! Diagnostic dump endpoint for stream.snv.
//! See docs/superpowers/specs/2026-05-06-soak-gate-and-telemetry-design.md §5.5.
//! Issue #176.

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn build_dump_with_full_sources_returns_complete_json() {
        let sources = MockSources::full().await;
        let dump = build_dump(&sources).await;
        assert!(dump["generated_at"].is_string());
        assert!(dump["audit_60min"].is_array());
        assert!(dump["endpoint_timeline"].is_object());
        assert!(dump["disk_cache_stats"].is_object());
        assert!(dump["s3_fetch_profile"].is_object());
        assert_eq!(dump["event_id"], 9289);
    }

    #[tokio::test]
    async fn build_dump_with_vps_unreachable_returns_partial() {
        let sources = MockSources::vps_unreachable().await;
        let dump = build_dump(&sources).await;
        // Failed sub-section replaced with { "error": "..." } per spec §7.
        assert!(dump["disk_cache_stats"]["error"].is_string());
        assert!(dump["s3_fetch_profile"]["error"].is_string());
        // Other sections still populated.
        assert!(dump["audit_60min"].is_array());
    }
}
