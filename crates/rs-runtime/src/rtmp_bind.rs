//! RTMP listener pre-bind probe + port-holder diagnostics (#106).
//!
//! Historically a bind conflict on the RTMP port (1234) failed SILENTLY: xiu's
//! `rtmp_server.run()` returned the bind `Err`, but `RtmpServer::run` swallowed
//! it into `Ok(())` inside a `tokio::select!`, so the orchestrator's inpoint
//! loop read it as a clean stop and exited — the dashboard showed "everything
//! fine" while RTMP was dead. This module makes the failure LOUD: the inpoint
//! loop probes the bind BEFORE starting xiu, records a human-readable error on
//! the shared `InpointState` (surfaced on `/api/v1/status` and as a red
//! dashboard banner), emits a `WsEvent::RtmpBindFailed`, and retries on a
//! backoff instead of giving up.

use std::net::TcpListener;

use rs_core::audit::{Action, AuditRow, Severity, Source};
use rs_core::models::{InpointState, WsEvent};
use tokio::sync::broadcast;
use tracing::{error, info};

/// The first RTMP bind-retry wait, and its cap (#106).
pub(crate) const INITIAL_BIND_BACKOFF_SECS: u64 = 2;
pub(crate) const MAX_BIND_BACKOFF_SECS: u64 = 30;

/// The bind-retry wait after the one just served (#106): doubles up to
/// [`MAX_BIND_BACKOFF_SECS`]. A restart request re-probes at once, so the
/// next conflict starts fresh at [`INITIAL_BIND_BACKOFF_SECS`].
pub(crate) fn next_bind_backoff(current_secs: u64, was_restart: bool) -> u64 {
    if was_restart {
        INITIAL_BIND_BACKOFF_SECS
    } else {
        (current_secs * 2).min(MAX_BIND_BACKOFF_SECS)
    }
}

/// Probe whether `bind:port` can be bound for the RTMP listener, recording the
/// outcome on the shared `InpointState` (#106).
///
/// Returns `true` when the port is free (safe to start xiu) and `false` when it
/// cannot be bound.
///
/// EDGE-TRIGGERED: the runtime calls this at the top of every inpoint-loop
/// iteration (once per bind-backoff tick), so it must not flood on a persistent
/// conflict. It acts only on a STATE TRANSITION:
/// - first failure of a streak (`bind_error()` was `None`) → run diagnostics,
///   record the error on `inpoint_state` (surfaced on `/api/v1/status`), emit
///   `WsEvent::RtmpBindFailed` for live dashboards, `error!`-log once, and write
///   a durable `RtmpBindFailed` audit row;
/// - recovery (`bind_error()` was `Some`, port now free) → clear the error (the
///   banner auto-clears), `info!`-log, and write an `RtmpBindRecovered` audit
///   row;
/// - steady state (unchanged) → nothing but the bind probe + return value.
///
/// The probe binds then immediately DROPS the listener, releasing the port so
/// the xiu server can bind it. If a process wins the tiny TOCTOU window between
/// drop and xiu's own bind, xiu's bind error is now propagated (not swallowed —
/// #106, `rtmp_server.rs`), so `run_inpoint_loop` restarts and the NEXT probe
/// surfaces the conflict — no permanent silent death.
///
/// This runs synchronously (blocking `TcpListener::bind` + blocking diagnostics
/// child processes); the runtime wraps the call in `spawn_blocking` so it never
/// stalls an async worker.
pub fn probe_and_record_bind(
    bind: &str,
    port: u16,
    inpoint_state: &InpointState,
    ws_tx: &broadcast::Sender<WsEvent>,
) -> bool {
    match TcpListener::bind(format!("{bind}:{port}")) {
        Ok(listener) => {
            // Release the port for the real xiu server.
            drop(listener);
            // Recovery edge only: clear + audit when we were previously failing.
            if inpoint_state.bind_error().is_some() {
                inpoint_state.clear_bind_error();
                info!("RTMP port {port} is free again — listener will bind");
                if let Some(tx) = inpoint_state.audit_tx() {
                    rs_core::audit::record(
                        tx,
                        AuditRow {
                            severity: Severity::Info,
                            source: Source::Inpoint,
                            event_id: None,
                            instance_id: None,
                            endpoint: None,
                            action: Action::RtmpBindRecovered,
                            detail: serde_json::json!({ "port": port }),
                            ts_override: None,
                        },
                    );
                }
            }
            true
        }
        Err(e) => {
            // First-failure edge only: diagnose + record + broadcast + audit
            // once. On a persistent conflict subsequent ticks are silent.
            if inpoint_state.bind_error().is_none() {
                let holder = if e.kind() == std::io::ErrorKind::AddrInUse {
                    identify_port_holder(port)
                } else {
                    None
                };
                let msg = format_bind_error(port, &e, holder.as_deref());
                error!("RTMP listener bind probe failed on {bind}:{port}: {msg}");
                inpoint_state.set_bind_error(msg.clone());
                let _ = ws_tx.send(WsEvent::RtmpBindFailed {
                    port,
                    error: msg.clone(),
                });
                if let Some(tx) = inpoint_state.audit_tx() {
                    rs_core::audit::record(
                        tx,
                        AuditRow {
                            severity: Severity::Warn,
                            source: Source::Inpoint,
                            event_id: None,
                            instance_id: None,
                            endpoint: None,
                            action: Action::RtmpBindFailed,
                            detail: serde_json::json!({
                                "port": port,
                                "holder": holder,
                                "error": msg,
                            }),
                            ts_override: None,
                        },
                    );
                }
            }
            false
        }
    }
}

/// Human-readable, operator-facing bind-error message (#106). Names the port and
/// — when the OS conflict was identified — the holding process, so the operator
/// knows exactly what to kill.
fn format_bind_error(port: u16, e: &std::io::Error, holder: Option<&str>) -> String {
    if e.kind() == std::io::ErrorKind::AddrInUse {
        match holder {
            Some(h) => format!(
                "Port {port} is already in use by another process ({h}). \
                 RTMP streaming will not work until the conflict is resolved."
            ),
            None => format!(
                "Port {port} is already in use by another process. \
                 RTMP streaming will not work until the conflict is resolved."
            ),
        }
    } else {
        format!("RTMP listener could not bind port {port}: {e}")
    }
}

/// Best-effort identification of the process holding `port` (#106).
///
/// Windows: `netstat -ano` -> the LISTENING PID -> `tasklist` for the image
/// name. Linux (CI / dev): `ss -ltnp`. Returns `None` on any error, missing
/// tool, or no match — the bind error is still surfaced with the port name.
pub fn identify_port_holder(port: u16) -> Option<String> {
    #[cfg(target_os = "windows")]
    {
        holder_windows::identify_port_holder_windows(port)
    }
    #[cfg(not(target_os = "windows"))]
    {
        identify_port_holder_unix(port)
    }
}

// The Windows command glue lives in its own file, excluded from mutation in
// `.cargo/mutants.toml`: cargo-mutants does not evaluate `cfg`, so its
// mutants would be generated on Linux, never compiled, and reported MISSED.
// Every decision it makes is one of the tested parsers below (#367).
#[cfg(target_os = "windows")]
#[path = "rtmp_bind_windows.rs"]
mod holder_windows;

/// `"PID <pid>: <image>"`, or `"PID <pid>"` when the image name is unknown.
fn format_holder(pid: &str, name: Option<&str>) -> String {
    match name {
        Some(n) => format!("PID {pid}: {n}"),
        None => format!("PID {pid}"),
    }
}

/// The PID of the LISTENING row whose local address ends with `:<port>` in
/// `netstat -ano` output (Windows). The command has no `-p TCP`: that lists
/// IPv4 rows only, so a process listening on the dual-stack `[::]:<port>`
/// (which DOES block `0.0.0.0:<port>` on Windows) would be missed. Plain
/// `-ano` lists every protocol; the LISTENING + `:<port>` filter selects the
/// right TCP row, and UDP has no LISTENING state so it never matches (#106
/// review). Compiled on Linux for its tests only.
#[cfg(any(target_os = "windows", test))]
fn listening_pid_from_netstat(text: &str, port: u16) -> Option<String> {
    let needle = format!(":{port}");
    text.lines().find_map(|line| {
        let l = line.trim();
        if !l.contains("LISTENING") {
            return None;
        }
        // netstat -ano TCP row: Proto Local Foreign State PID
        let cols: Vec<&str> = l.split_whitespace().collect();
        let local = cols.get(1)?;
        if !local.ends_with(&needle) {
            return None;
        }
        cols.last().map(|s| s.to_string())
    })
}

/// The image name from `tasklist /FO CSV /NH` output: its first quoted field
/// (`"image.exe","1234",...`). `None` for an empty name or no match.
#[cfg(any(target_os = "windows", test))]
fn image_name_from_tasklist(csv: &str) -> Option<String> {
    csv.split('"')
        .nth(1)
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

/// What `ss -ltnpH` output says about the listener on a port.
#[cfg(any(not(target_os = "windows"), test))]
#[derive(Debug, PartialEq)]
enum SsHolder {
    /// The listener's process: `"PID <pid>: <name>"`, or the name alone.
    Found(String),
    /// A row matched the port but carries no `users:` field (`ss` needs
    /// privileges to see another user's process).
    Unidentified,
    /// No listener on the port in the output.
    NoMatch,
}

/// Parse `ss -ltnpH` output, e.g.
/// `LISTEN 0 128 127.0.0.1:1234 0.0.0.0:* users:(("proc",pid=42,fd=3))`.
#[cfg(any(not(target_os = "windows"), test))]
fn holder_from_ss(text: &str, port: u16) -> SsHolder {
    let needle = format!(":{port}");
    for line in text.lines() {
        let cols: Vec<&str> = line.split_whitespace().collect();
        let local = cols.get(3).copied().unwrap_or("");
        if !local.ends_with(&needle) {
            continue;
        }
        // Parse users:(("name",pid=NNN,...))
        let Some(users) = line.split("users:").nth(1) else {
            return SsHolder::Unidentified;
        };
        let name = users
            .split('"')
            .nth(1)
            .filter(|s| !s.is_empty())
            .unwrap_or("unknown");
        let pid = users
            .split("pid=")
            .nth(1)
            .and_then(|s| s.split(|c: char| !c.is_ascii_digit()).next())
            .filter(|s| !s.is_empty());
        return SsHolder::Found(match pid {
            Some(p) => format_holder(p, Some(name)),
            None => name.to_string(),
        });
    }
    SsHolder::NoMatch
}

#[cfg(not(target_os = "windows"))]
fn identify_port_holder_unix(port: u16) -> Option<String> {
    use std::process::Command;

    let out = Command::new("ss").args(["-ltnpH"]).output().ok()?;
    match holder_from_ss(&String::from_utf8_lossy(&out.stdout), port) {
        SsHolder::Found(holder) => Some(holder),
        SsHolder::Unidentified => {
            // Matched the port but no `users:` field — log before bailing so
            // this case is not silent.
            tracing::info!("port :{port} held but holder not identifiable (ss needs privileges)");
            None
        }
        SsHolder::NoMatch => {
            tracing::info!("no port-holder identified for :{port} (ss unavailable or no match)");
            None
        }
    }
}

#[cfg(test)]
#[path = "rtmp_bind_tests.rs"]
mod tests;
