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

use rs_core::models::{InpointState, WsEvent};
use tokio::sync::broadcast;
use tracing::error;

/// Probe whether `bind:port` can be bound for the RTMP listener, recording the
/// outcome on the shared `InpointState` (#106).
///
/// Returns `true` when the port is free (safe to start xiu) and `false` when it
/// cannot be bound. On failure it records a human-readable error on
/// `inpoint_state` (which `/api/v1/status` surfaces) and emits
/// `WsEvent::RtmpBindFailed` so live dashboards update immediately; on success
/// it clears any previously-recorded error so the banner auto-clears.
///
/// The probe binds then immediately DROPS the listener, releasing the port so
/// the xiu server can bind it. The tiny TOCTOU window between drop and xiu's
/// bind is acceptable for a diagnostic surface: a process that wins the race is
/// re-surfaced by the next loop iteration's probe.
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
            inpoint_state.clear_bind_error();
            true
        }
        Err(e) => {
            let holder = if e.kind() == std::io::ErrorKind::AddrInUse {
                identify_port_holder(port)
            } else {
                None
            };
            let msg = format_bind_error(port, &e, holder.as_deref());
            error!("RTMP listener bind probe failed on {bind}:{port}: {msg}");
            inpoint_state.set_bind_error(msg.clone());
            let _ = ws_tx.send(WsEvent::RtmpBindFailed { port, error: msg });
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
        identify_port_holder_windows(port)
    }
    #[cfg(not(target_os = "windows"))]
    {
        identify_port_holder_unix(port)
    }
}

#[cfg(target_os = "windows")]
fn identify_port_holder_windows(port: u16) -> Option<String> {
    use std::process::Command;

    let out = Command::new("netstat")
        .args(["-ano", "-p", "TCP"])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let needle = format!(":{port}");
    // Find a LISTENING row whose local address ends with :<port>.
    let pid = text.lines().find_map(|line| {
        let l = line.trim();
        if !l.contains("LISTENING") {
            return None;
        }
        let cols: Vec<&str> = l.split_whitespace().collect();
        // netstat -ano TCP row: Proto Local Foreign State PID
        let local = cols.get(1)?;
        if !local.ends_with(&needle) {
            return None;
        }
        cols.last().map(|s| s.to_string())
    })?;

    // Resolve the PID to an image name via tasklist.
    let name = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|csv| {
            // "image.exe","1234",...  -> first quoted field
            csv.split('"').nth(1).map(|s| s.to_string())
        })
        .filter(|s| !s.is_empty());

    Some(match name {
        Some(n) => format!("PID {pid}: {n}"),
        None => format!("PID {pid}"),
    })
}

#[cfg(not(target_os = "windows"))]
fn identify_port_holder_unix(port: u16) -> Option<String> {
    use std::process::Command;

    let out = Command::new("ss").args(["-ltnpH"]).output().ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let needle = format!(":{port}");
    for line in text.lines() {
        // e.g. LISTEN 0 128 127.0.0.1:1234 0.0.0.0:* users:(("proc",pid=42,fd=3))
        let cols: Vec<&str> = line.split_whitespace().collect();
        let local = cols.get(3).copied().unwrap_or("");
        if !local.ends_with(&needle) {
            continue;
        }
        // Parse users:(("name",pid=NNN,...))
        if let Some(users) = line.split("users:").nth(1) {
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
            return Some(match pid {
                Some(p) => format!("PID {p}: {name}"),
                None => name.to_string(),
            });
        }
        return None;
    }
    // No `ss` match (or `-p` needs privileges) — leave the holder unnamed.
    tracing::info!("no port-holder identified for :{port} (ss unavailable or no match)");
    None
}

#[cfg(test)]
#[path = "rtmp_bind_tests.rs"]
mod tests;
