use crate::delivery_yt_health::{extract_top_issue, issue_changed_action};
use rs_core::audit::Action;
use rs_youtube::streams::{
    ConfigurationIssue, HealthStatus, IngestionInfo, LiveStream, StreamCdn, StreamSnippet,
    StreamStatus,
};

#[allow(non_snake_case)]
fn liveStream_with(top_issue: Option<&str>, health: &str) -> LiveStream {
    LiveStream {
        id: "s1".into(),
        snippet: StreamSnippet {
            title: "ytbb".into(),
            channel_id: None,
        },
        status: StreamStatus {
            stream_status: "active".into(),
            health_status: Some(HealthStatus {
                status: health.into(),
                configuration_issues: top_issue
                    .map(|t| {
                        vec![ConfigurationIssue {
                            issue_type: t.into(),
                            severity: "warning".into(),
                            reason: "videoIngestionStarved".into(),
                            description: None,
                        }]
                    })
                    .unwrap_or_default(),
                last_update_time_seconds: None,
            }),
        },
        cdn: Some(StreamCdn {
            ingestion_type: Some("rtmp".into()),
            resolution: Some("1920x1080".into()),
            frame_rate: Some("30.0".into()),
            ingestion_info: Some(IngestionInfo {
                stream_name: Some("KEY-BB".into()),
                ingestion_address: None,
                backup_ingestion_address: None,
            }),
        }),
    }
}

#[test]
fn extract_top_issue_uses_first_configuration_issue() {
    let s = liveStream_with(Some("videoIngestionStarved"), "bad");
    let h = extract_top_issue(&s);
    assert_eq!(h.stream_status, "active");
    assert_eq!(h.health_status, "bad");
    assert_eq!(h.top_issue.as_deref(), Some("videoIngestionStarved"));
    assert_eq!(h.resolution.as_deref(), Some("1920x1080"));
    assert_eq!(h.frame_rate.as_deref(), Some("30.0"));
    assert!(h.error.is_none());
}

#[test]
fn extract_top_issue_handles_no_issues() {
    let s = liveStream_with(None, "good");
    let h = extract_top_issue(&s);
    assert_eq!(h.health_status, "good");
    assert!(h.top_issue.is_none());
}

#[test]
fn issue_changed_action_emits_on_transition_none_to_some() {
    let out = issue_changed_action(None, Some("videoIngestionStarved"));
    let (action, from, to) = out.expect("transition None->Some must emit");
    assert_eq!(action, Action::YoutubeIssueChanged);
    assert!(from.is_none());
    assert_eq!(to.as_deref(), Some("videoIngestionStarved"));
}

#[test]
fn issue_changed_action_emits_on_transition_some_to_other() {
    let out = issue_changed_action(Some("bitrateLow"), Some("videoIngestionStarved"));
    let (_, from, to) = out.expect("transition must emit");
    assert_eq!(from.as_deref(), Some("bitrateLow"));
    assert_eq!(to.as_deref(), Some("videoIngestionStarved"));
}

#[test]
fn issue_changed_action_is_silent_on_same_value() {
    let out = issue_changed_action(Some("videoIngestionStarved"), Some("videoIngestionStarved"));
    assert!(out.is_none(), "no transition => no audit row");
}

#[test]
fn issue_changed_action_emits_on_recovery_some_to_none() {
    let out = issue_changed_action(Some("videoIngestionStarved"), None);
    let (_, from, to) = out.expect("recovery must emit");
    assert_eq!(from.as_deref(), Some("videoIngestionStarved"));
    assert!(to.is_none());
}
