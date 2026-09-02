//! #209 — `/api/v1/youtube/status` exposes `configuration_issues` as structured
//! `{type, severity, reason, description?}` objects rather than
//! `"{type}: {reason} ({severity})"` strings. These tests lock the wire shape so
//! CI PowerShell + dashboard consumers can rely on property access.

use crate::youtube::{ConfigurationIssueOut, YouTubeStreamInfo};
use rs_youtube::streams::ConfigurationIssue;

#[test]
fn configuration_issue_serializes_as_structured_object() {
    let out = ConfigurationIssueOut::from(&ConfigurationIssue {
        issue_type: "videoIngestionFasterThanRealtime".to_string(),
        severity: "error".to_string(),
        reason: "Check video settings".to_string(),
        description: Some("Frames arriving too fast".to_string()),
    });
    let v: serde_json::Value = serde_json::to_value(&out).unwrap();

    // Field is `type` (not `issue_type`, not a flattened string).
    assert_eq!(v["type"], "videoIngestionFasterThanRealtime");
    assert_eq!(v["severity"], "error");
    assert_eq!(v["reason"], "Check video settings");
    assert_eq!(v["description"], "Frames arriving too fast");
    // No accidental leak of the internal parse-struct field name.
    assert!(v.get("issue_type").is_none());
    // Not a string projection.
    assert!(v.is_object());
}

#[test]
fn description_is_omitted_when_none() {
    let out = ConfigurationIssueOut::from(&ConfigurationIssue {
        issue_type: "bitrateLow".to_string(),
        severity: "info".to_string(),
        reason: "Bitrate is lower than recommended".to_string(),
        description: None,
    });
    let v: serde_json::Value = serde_json::to_value(&out).unwrap();

    assert_eq!(v["type"], "bitrateLow");
    assert_eq!(v["severity"], "info");
    // `skip_serializing_if` — the key must be entirely absent, not `null`.
    assert!(
        v.get("description").is_none(),
        "description must be omitted when None, got {v}"
    );
}

#[test]
fn stream_info_configuration_issues_is_array_of_objects() {
    let info = YouTubeStreamInfo {
        title: "e2e rtmp".to_string(),
        stream_status: "active".to_string(),
        health_status: Some("bad".to_string()),
        configuration_issues: vec![
            ConfigurationIssueOut::from(&ConfigurationIssue {
                issue_type: "videoIngestionStarved".to_string(),
                severity: "error".to_string(),
                reason: "No data".to_string(),
                description: None,
            }),
            ConfigurationIssueOut::from(&ConfigurationIssue {
                issue_type: "audioBitrateLow".to_string(),
                severity: "info".to_string(),
                reason: "Audio bitrate low".to_string(),
                description: None,
            }),
        ],
        cdn_resolution: None,
        cdn_frame_rate: None,
        cdn_ingestion_type: None,
    };
    let v: serde_json::Value = serde_json::to_value(&info).unwrap();
    let issues = v["configuration_issues"].as_array().expect("array");
    assert_eq!(issues.len(), 2);
    // Property access works — the exact thing CI's regex workaround replaces.
    assert_eq!(issues[0]["type"], "videoIngestionStarved");
    assert_eq!(issues[0]["severity"], "error");
    assert_eq!(issues[1]["type"], "audioBitrateLow");
    assert_eq!(issues[1]["severity"], "info");
}
