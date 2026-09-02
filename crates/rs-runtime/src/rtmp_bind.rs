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

// NOTE(#106): STUB for the RED test commit — always claims the port is bindable
// and records nothing, so the failing test proves the bug. The real probe +
// diagnostics land in the GREEN commit.

use rs_core::models::{InpointState, WsEvent};
use tokio::sync::broadcast;

/// Probe whether `bind:port` can be bound for the RTMP listener, recording the
/// outcome on the shared `InpointState` (#106).
///
/// Returns `true` when the port is free (safe to start xiu) and `false` when it
/// is already in use. On failure it records a human-readable error on
/// `inpoint_state` and emits `WsEvent::RtmpBindFailed`; on success it clears any
/// previously-recorded error.
pub fn probe_and_record_bind(
    _bind: &str,
    _port: u16,
    _inpoint_state: &InpointState,
    _ws_tx: &broadcast::Sender<WsEvent>,
) -> bool {
    // STUB — not yet implemented (RED).
    true
}
