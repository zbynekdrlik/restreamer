//! Unit tests for the RTMP pre-bind probe + message formatting (#106).

use super::*;

#[test]
fn free_port_probes_bindable_and_clears_error() {
    // Reserve a port, then release it so the probe sees it free.
    let port = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let state = InpointState::new();
    state.set_bind_error("stale".into());
    let (ws_tx, _rx) = broadcast::channel::<WsEvent>(4);

    assert!(probe_and_record_bind("127.0.0.1", port, &state, &ws_tx));
    assert!(
        state.bind_error().is_none(),
        "a successful probe must clear any stale bind error"
    );
}

#[test]
fn occupied_port_records_error_and_emits_event() {
    let hog = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = hog.local_addr().unwrap().port();
    let state = InpointState::new();
    let (ws_tx, mut rx) = broadcast::channel::<WsEvent>(4);

    assert!(!probe_and_record_bind("127.0.0.1", port, &state, &ws_tx));
    let err = state.bind_error().expect("bind error must be recorded");
    assert!(err.contains(&port.to_string()));
    match rx.try_recv() {
        Ok(WsEvent::RtmpBindFailed { port: p, .. }) => assert_eq!(p, port),
        other => panic!("expected RtmpBindFailed, got {other:?}"),
    }
}

#[test]
fn addr_in_use_message_names_holder_when_known() {
    let e = std::io::Error::from(std::io::ErrorKind::AddrInUse);
    let with = format_bind_error(1234, &e, Some("PID 42: inpoint_service.exe"));
    assert!(with.contains("1234"));
    assert!(with.contains("inpoint_service.exe"));

    let without = format_bind_error(1234, &e, None);
    assert!(without.contains("1234"));
    assert!(without.contains("already in use"));

    let other = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
    let msg = format_bind_error(1234, &other, None);
    assert!(msg.contains("could not bind port 1234"));
}
