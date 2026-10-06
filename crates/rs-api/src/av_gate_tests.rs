//! Wire types, the registry and the pure guards of the A/V-gate API (#357).

use super::*;

fn holder(id: &str) -> Holder {
    Holder {
        session_id: id.to_string(),
        requester: "camera-box".to_string(),
    }
}

#[test]
fn state_strings_match_the_wire_contract() {
    for (state, wire) in [
        (SessionState::Starting, "starting"),
        (SessionState::Ready, "ready"),
        (SessionState::Processing, "processing"),
        (SessionState::Done, "done"),
        (SessionState::Failed, "failed"),
    ] {
        assert_eq!(state.as_str(), wire);
        assert_eq!(serde_json::to_value(state).unwrap(), wire);
    }
}

#[test]
fn the_registry_holds_one_session_at_a_time() {
    let r = AvGateRegistry::default();
    assert_eq!(r.holder(), None);
    let _rx = r.claim(holder("a")).unwrap();
    assert_eq!(r.holder(), Some(holder("a")));
    assert_eq!(r.claim(holder("b")).unwrap_err(), holder("a"));
    r.release("b");
    assert_eq!(r.holder(), Some(holder("a")), "only the holder can release");
    r.release("a");
    assert_eq!(r.holder(), None);
    let _rx = r.claim(holder("b")).unwrap();
    assert_eq!(r.holder(), Some(holder("b")));
}

#[test]
fn the_registry_opens_only_once_reconciled() {
    let r = AvGateRegistry::default();
    assert!(!r.is_reconciled());
    r.mark_reconciled();
    assert!(r.is_reconciled());
}

#[test]
fn a_stop_reaches_only_the_holding_session() {
    let r = AvGateRegistry::default();
    assert!(!r.request_stop("a"), "nobody holds the slot");
    let rx = r.claim(holder("a")).unwrap();
    assert!(!*rx.borrow());
    assert!(!r.request_stop("b"));
    assert!(!*rx.borrow());
    assert!(r.request_stop("a"));
    assert!(*rx.borrow());
}

#[test]
fn quota_allows_up_to_the_budget_exactly() {
    assert!(quota_allows(3_600, 400, 4_000));
    assert!(!quota_allows(3_601, 400, 4_000));
    assert!(quota_allows(0, 400, 400));
    assert!(!quota_allows(0, 401, 400));
}

#[test]
fn validate_request_trims_and_bounds_its_inputs() {
    assert_eq!(
        validate_request("  camera-box ", Some(" My gate "), "0123456789"),
        Ok(("camera-box".to_string(), "My gate".to_string()))
    );
    assert_eq!(
        validate_request("restreamer-ci", None, "abcdef0123456789"),
        Ok((
            "restreamer-ci".to_string(),
            "A/V gate restreamer-ci abcdef01".to_string()
        ))
    );
    assert!(validate_request("  ", None, "x").is_err());
    assert!(validate_request(&"r".repeat(65), None, "x").is_err());
    assert!(validate_request(&"r".repeat(64), None, "x").is_ok());
    assert!(validate_request("r", Some("   "), "x").is_err());
    assert!(validate_request("r", Some(&"t".repeat(101)), "x").is_err());
    assert!(validate_request("r", Some(&"t".repeat(100)), "x").is_ok());
}

#[test]
fn timings_come_from_the_config() {
    let cfg = AvGateConfig {
        idle_timeout_secs: 61,
        processing_timeout_secs: 62,
        ..AvGateConfig::default()
    };
    assert_eq!(
        AvGateTimings::from_config(&cfg),
        AvGateTimings {
            poll: Duration::from_secs(15),
            drain_extra: Duration::from_secs(15),
            idle_timeout: Duration::from_secs(61),
            servers_gone_timeout: Duration::from_secs(180),
            processing_poll: Duration::from_secs(30),
            processing_timeout: Duration::from_secs(62),
            cleanup_retry: Duration::from_secs(300),
        }
    );
}

#[test]
fn the_view_carries_every_row_field() {
    let row = AvGateSessionRow {
        id: "s".to_string(),
        requester: "r".to_string(),
        title: "t".to_string(),
        state: "done".to_string(),
        broadcast_id: Some("b".to_string()),
        stream_id: Some("st".to_string()),
        event_id: Some(1),
        went_live: true,
        cleanup_pending: true,
        vod_id: Some("v".to_string()),
        reason: Some("why".to_string()),
        quota_units: 9,
        created_at: "c".to_string(),
        ready_at: Some("r1".to_string()),
        stop_requested_at: Some("s1".to_string()),
        processing_at: Some("p1".to_string()),
        finished_at: Some("f1".to_string()),
    };
    let v = serde_json::to_value(SessionView::from(row)).unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "session_id": "s", "state": "done", "requester": "r", "title": "t",
            "broadcast_id": "b", "vod_id": "v", "reason": "why", "quota_units": 9,
            "cleanup_pending": true,
            "timestamps": {"created": "c", "ready": "r1", "stop_requested": "s1",
                           "processing": "p1", "finished": "f1"}
        })
    );
}
