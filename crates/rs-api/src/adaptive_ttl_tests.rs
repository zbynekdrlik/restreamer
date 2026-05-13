//! `ttl_for_health` decides cache lifetime: 60s when fully healthy,
//! 15s otherwise. Drives the per-project quota math (see spec section 11).

use crate::delivery_status::ttl_for_health;
use rs_core::models::YoutubeHealth;
use std::time::Duration;

fn health(status: &str, top_issue: Option<&str>, error: Option<&str>) -> YoutubeHealth {
    YoutubeHealth {
        stream_status: "active".into(),
        health_status: status.into(),
        top_issue: top_issue.map(String::from),
        resolution: None,
        frame_rate: None,
        age_secs: 0,
        error: error.map(String::from),
    }
}

#[test]
fn good_and_no_issue_returns_60s() {
    assert_eq!(ttl_for_health(&health("good", None, None)), Duration::from_secs(60));
}

#[test]
fn bad_returns_15s() {
    assert_eq!(ttl_for_health(&health("bad", Some("videoIngestionStarved"), None)),
               Duration::from_secs(15));
}

#[test]
fn ok_returns_15s() {
    assert_eq!(ttl_for_health(&health("ok", Some("gopSizeLong"), None)), Duration::from_secs(15));
}

#[test]
fn good_with_top_issue_returns_15s() {
    // YT can report health=good with non-empty issues (warnings). Treat as degraded.
    assert_eq!(ttl_for_health(&health("good", Some("framerateHigh"), None)),
               Duration::from_secs(15));
}

#[test]
fn error_path_returns_15s() {
    assert_eq!(ttl_for_health(&health("unknown", None, Some("probe_error"))),
               Duration::from_secs(15));
}

#[test]
fn quota_throttled_returns_15s() {
    // Throttled probes shouldn't extend their own TTL (they'd never recover).
    assert_eq!(ttl_for_health(&health("unknown", None, Some("quota_throttled"))),
               Duration::from_secs(15));
}
