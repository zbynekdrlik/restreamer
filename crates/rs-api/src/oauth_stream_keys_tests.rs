//! Unit tests for the pure auto-suggest helpers (#199).

use crate::oauth_stream_keys::{OauthStreamKeys, stream_names_of, suggest_oauth_for_key};
use rs_youtube::streams::LiveStream;

fn stream_with_name(name: Option<&str>) -> LiveStream {
    let cdn = match name {
        Some(n) => serde_json::json!({ "ingestionInfo": { "streamName": n } }),
        None => serde_json::json!({}),
    };
    serde_json::from_value(serde_json::json!({
        "id": "s1",
        "snippet": { "title": "t" },
        "status": { "streamStatus": "active" },
        "cdn": cdn,
    }))
    .expect("valid LiveStream")
}

#[test]
fn stream_names_of_extracts_nonempty_names() {
    let streams = vec![
        stream_with_name(Some("aaaa-bbbb")),
        stream_with_name(None),     // no ingestionInfo => skipped
        stream_with_name(Some("")), // empty => skipped
        stream_with_name(Some("cccc-dddd")),
    ];
    assert_eq!(stream_names_of(&streams), vec!["aaaa-bbbb", "cccc-dddd"]);
}

#[test]
fn stream_names_of_missing_cdn_is_empty() {
    let mut s = stream_with_name(Some("x"));
    s.cdn = None;
    assert!(stream_names_of(&[s]).is_empty());
}

#[test]
fn suggest_returns_unique_owner() {
    let grants = vec![
        OauthStreamKeys {
            oauth_id: 10,
            stream_names: vec!["key-A".into(), "key-B".into()],
        },
        OauthStreamKeys {
            oauth_id: 20,
            stream_names: vec!["key-C".into()],
        },
    ];
    assert_eq!(suggest_oauth_for_key("key-C", &grants), Some(20));
    assert_eq!(suggest_oauth_for_key("key-A", &grants), Some(10));
}

#[test]
fn suggest_none_when_no_owner() {
    let grants = vec![OauthStreamKeys {
        oauth_id: 10,
        stream_names: vec!["key-A".into()],
    }];
    assert_eq!(suggest_oauth_for_key("key-Z", &grants), None);
}

#[test]
fn suggest_none_when_ambiguous() {
    // Same key owned by two grants => ambiguous => no auto-suggest.
    let grants = vec![
        OauthStreamKeys {
            oauth_id: 10,
            stream_names: vec!["dup".into()],
        },
        OauthStreamKeys {
            oauth_id: 20,
            stream_names: vec!["dup".into()],
        },
    ];
    assert_eq!(suggest_oauth_for_key("dup", &grants), None);
}

#[test]
fn suggest_none_for_empty_key() {
    let grants = vec![OauthStreamKeys {
        oauth_id: 10,
        stream_names: vec!["".into()],
    }];
    assert_eq!(suggest_oauth_for_key("", &grants), None);
}
