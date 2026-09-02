//! Regression test for the #148 loopback port-reservation TOCTOU.
//!
//! The RTMP test harnesses discover an ephemeral loopback port for their xiu
//! server. The old strategy bound `127.0.0.1:0`, read the port, then DROPPED
//! the listener and returned the bare port — leaving a pick-then-release window
//! in which a concurrent `bind(0)` (the TLS bridge listener in
//! `spawn_recording_xiu_server_tls`) could be handed the very same port. That
//! cross-wired the TLS bridge onto the plain server's port and surfaced as
//! `rustls` `InvalidContentType` on the TLS accept (a non-TLS connection landed
//! on the TLS listener). cargo-tarpaulin's instrumentation widened the window
//! enough to make it fire on CI.
//!
//! The fix hands the harness a HELD listener (the socket that reserves the port
//! is the socket that accepts), so the reserved port is owned continuously with
//! no steal window. This test asserts that invariant directly and
//! deterministically (no timing dependence): the reserved port must be OWNED
//! the instant reservation returns.

mod common;

#[tokio::test]
async fn reserved_loopback_listener_holds_its_port_no_toctou() {
    let (listener, port) = common::reserved_loopback_listener().await;

    // The reserved port must be OWNED the instant reservation returns: a second
    // bind on it MUST fail (EADDRINUSE). With the old drop-then-rebind
    // discovery the port was free on return, letting a concurrent bind(0) (the
    // TLS bridge listener) steal it (#148 InvalidContentType cross-wire). The
    // held listener closes that window.
    let steal = std::net::TcpListener::bind(("127.0.0.1", port));
    assert!(
        steal.is_err(),
        "reserved port {port} was free on return — TOCTOU steal window present (#148)"
    );

    // Sanity: once the reservation is released, the port is free again.
    drop(listener);
    assert!(
        std::net::TcpListener::bind(("127.0.0.1", port)).is_ok(),
        "port {port} should be free after the reserved listener is dropped"
    );
}
