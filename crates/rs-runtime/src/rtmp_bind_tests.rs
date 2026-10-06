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

// ----- #367: the port-holder lookup, pinned for cargo-mutants -----

/// On Linux the probe names the process that holds the port: here, this
/// test process itself (`ss` shows a user's own sockets without privileges).
#[cfg(not(target_os = "windows"))]
#[test]
fn a_conflict_names_the_holding_process() {
    let hog = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = hog.local_addr().unwrap().port();
    let me = format!("PID {}", std::process::id());

    let holder = identify_port_holder(port).expect("ss must name this test's listener");
    assert!(
        holder.starts_with(&me),
        "holder {holder:?} must be {me}: <name>"
    );

    let state = InpointState::new();
    let (ws_tx, _rx) = broadcast::channel::<WsEvent>(4);
    assert!(!probe_and_record_bind("127.0.0.1", port, &state, &ws_tx));
    let err = state.bind_error().expect("bind error recorded");
    assert!(
        err.contains(&me),
        "an AddrInUse error names the holder: {err:?}"
    );
}

const SS_OUTPUT: &str = "\
LISTEN 0 128 127.0.0.1:11234 0.0.0.0:* users:((\"other\",pid=7,fd=3))
LISTEN 0 128 127.0.0.1:1234 0.0.0.0:* users:((\"obs64\",pid=4242,fd=9))
LISTEN 0 128 0.0.0.0:22 0.0.0.0:*
LISTEN 0 128 0.0.0.0:2222 0.0.0.0:* users:((\"\",pid=99,fd=4))
LISTEN 0 128 0.0.0.0:3333 0.0.0.0:* users:((\"sshd\",fd=4))
";

#[test]
fn ss_output_parses_to_the_listener_on_exactly_that_port() {
    assert_eq!(
        holder_from_ss(SS_OUTPUT, 1234),
        SsHolder::Found("PID 4242: obs64".into()),
        "the :11234 row before it must not match :1234"
    );
    assert_eq!(holder_from_ss(SS_OUTPUT, 22), SsHolder::Unidentified);
    assert_eq!(
        holder_from_ss(SS_OUTPUT, 2222),
        SsHolder::Found("PID 99: unknown".into()),
        "an empty process name reads as unknown"
    );
    assert_eq!(
        holder_from_ss(SS_OUTPUT, 3333),
        SsHolder::Found("sshd".into()),
        "no pid: the name alone"
    );
    assert_eq!(holder_from_ss(SS_OUTPUT, 4444), SsHolder::NoMatch);
}

const NETSTAT_OUTPUT: &str = "
Active Connections

  Proto  Local Address          Foreign Address        State           PID
  TCP    10.77.9.204:1234       10.77.9.10:51000       ESTABLISHED     8080
  TCP    0.0.0.0:135            0.0.0.0:0              LISTENING       1040
  TCP    0.0.0.0:11234          0.0.0.0:0              LISTENING       77
  TCP    [::]:1234              [::]:0                 LISTENING       5512
  UDP    0.0.0.0:1234           *:*                                    999
";

#[test]
fn netstat_output_parses_to_the_listening_pid_on_exactly_that_port() {
    assert_eq!(
        listening_pid_from_netstat(NETSTAT_OUTPUT, 1234).as_deref(),
        Some("5512"),
        "the dual-stack [::] listener; not the ESTABLISHED row, not :11234"
    );
    assert_eq!(
        listening_pid_from_netstat(NETSTAT_OUTPUT, 135).as_deref(),
        Some("1040")
    );
    assert_eq!(listening_pid_from_netstat(NETSTAT_OUTPUT, 51000), None);
    assert_eq!(listening_pid_from_netstat(NETSTAT_OUTPUT, 4444), None);
}

#[test]
fn tasklist_csv_parses_to_the_image_name() {
    assert_eq!(
        image_name_from_tasklist("\"obs64.exe\",\"5512\",\"Console\",\"1\",\"412,000 K\"\r\n")
            .as_deref(),
        Some("obs64.exe")
    );
    assert_eq!(
        image_name_from_tasklist("\"\",\"5512\""),
        None,
        "empty name"
    );
    assert_eq!(
        image_name_from_tasklist("INFO: No tasks are running which match the specified criteria."),
        None
    );
    assert_eq!(
        format_holder("5512", Some("obs64.exe")),
        "PID 5512: obs64.exe"
    );
    assert_eq!(format_holder("5512", None), "PID 5512");
}
